use crate::block::*;
use crate::btree::BtreeNode;
use crate::dir::Directory;
use crate::inode::{INode, ACL_SYMBOLLINK};
use crate::Filesystem;

use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, Write};

#[macro_export]
macro_rules! dir_name {
    ($path: expr) => {
        std::path::Path::new($path)
            .parent()
            .unwrap()
            .to_string_lossy()
            .to_string()
    };
}

#[macro_export]
macro_rules! base_name {
    ($path: expr) => {
        std::path::Path::new($path)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string()
    };
}

pub struct File {
    inode: INode,
    inode_count: u64,
    btree_root: BtreeNode,
}

impl File {
    /** Create a file */
    pub fn create<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = create(fs, device).unwrap();

        let mut dir = Directory::open(fs, device, &dir_name!(path))?;
        dir.add_file(fs, device, &base_name!(path), inode_count)?;

        Self::open_by_inode(fs, device, inode_count)
    }
    /** Create a symbol link */
    pub fn create_symlink<D>(
        fs: &mut Filesystem,
        device: &mut D,
        path: &str,
        point_to: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = create_symlink(fs, device, point_to).unwrap();

        let mut dir = Directory::open(fs, device, &dir_name!(path))?;
        dir.add_file(fs, device, &base_name!(path), inode_count)?;

        Self::open_by_inode(fs, device, inode_count)
    }
    pub fn from_inode<D>(
        fs: &mut Filesystem,
        device: &mut D,
        inode_count: u64,
        inode: INode,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        Ok(Self {
            inode,
            inode_count,
            btree_root: BtreeNode::new(
                inode.btree_root,
                &fs.get_data_block(device, inode.btree_root)?,
            ),
        })
    }
    /** Open file by absolute path */
    pub fn open<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = *Directory::open(fs, device, &dir_name!(path))?
            .list_dir(fs, device)?
            .get(&base_name!(path))
            .unwrap();

        let inode = fs.get_inode(device, inode_count)?;

        if inode.is_symlink() {
            let path = Self::from_inode(fs, device, inode_count, inode)?.read_link(fs, device)?;
            Self::open(fs, device, &path)
        } else {
            Self::open_by_inode(fs, device, inode_count)
        }
    }
    pub fn open_by_inode<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode = fs.get_inode(device, inode_count)?;

        Ok(Self {
            inode,
            inode_count,
            btree_root: BtreeNode::new(
                inode.btree_root,
                &fs.get_data_block(device, inode.btree_root)?,
            ),
        })
    }
    /** Write data */
    pub fn write<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        mut offset: u64,
        mut data: &[u8],
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        while !data.is_empty() {
            let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
            let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block

            if let Some(block) = self.btree_root.offset_lookup(
                fs,
                device,
                block_count,
                self.inode.btree_depth as usize,
            ) {
                let written_size = std::cmp::min(data.len(), BLOCK_SIZE - block_offset as usize);
                let mut data_block = fs.get_data_block(device, block)?;

                data_block[block_offset as usize..block_offset as usize + written_size]
                    .copy_from_slice(&data[..written_size]);

                if fs.is_multireference(block) {
                    let new_block = fs.block_copy_out(device, block)?;
                    self.btree_root.offset_modify(
                        fs,
                        device,
                        block_count,
                        new_block,
                        self.inode.btree_depth as usize,
                    )?;
                    self.inode.btree_root = self.btree_root.block_count;
                    fs.set_data_block(device, new_block, data_block)?;
                } else {
                    fs.set_data_block(device, block, data_block)?;
                }

                if offset + written_size as u64 > self.get_size() {
                    self.inode.size += offset + written_size as u64 - self.get_size();
                }

                data = &data[written_size..];
                offset += written_size as u64;
            } else {
                let written_size = std::cmp::min(data.len(), BLOCK_SIZE);
                let data_block_count = fs.new_block().unwrap();
                self.inode.btree_depth = self.btree_root.offset_insert(
                    fs,
                    device,
                    block_count,
                    data_block_count,
                    self.inode.btree_depth as usize,
                )? as u8;

                let mut block_data = [0; BLOCK_SIZE];
                block_data[..written_size].copy_from_slice(&data[..written_size]);

                if offset + written_size as u64 > self.get_size() {
                    self.inode.size += offset + written_size as u64 - self.get_size();
                }

                fs.set_data_block(device, data_block_count, block_data)?;

                data = &data[written_size..];
                offset += written_size as u64;
            }
        }
        fs.set_inode(device, self.inode_count, self.inode)?;
        Ok(())
    }
    /** Read from file */
    pub fn read<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        mut offset: u64,
        mut data: &mut [u8],
        mut size: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let btree_root = BtreeNode::new(
            self.inode.btree_root,
            &fs.get_data_block(device, self.inode.btree_root)?,
        );

        loop {
            let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
            let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block
            if let Some(block) =
                btree_root.offset_lookup(fs, device, block_count, self.inode.btree_depth as usize)
            {
                let block = fs.get_data_block(device, block)?;
                let written_size = std::cmp::min(size as usize, BLOCK_SIZE - block_offset as usize);
                data[..written_size].copy_from_slice(
                    &block[block_offset as usize..block_offset as usize + written_size],
                );
                if written_size < size as usize {
                    offset += written_size as u64;
                    size -= written_size as u64;
                    data = &mut data[written_size as usize..];
                } else {
                    break;
                }
            } else {
                let written_size = std::cmp::min(size as usize, BLOCK_SIZE);

                data[..written_size as usize].copy_from_slice(&[0].repeat(written_size as usize));

                if written_size < size as usize {
                    offset += written_size as u64;
                    size -= written_size as u64;
                    data = &mut data[written_size as usize..];
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
    /** Adjust file size */
    pub fn truncate<D>(&mut self, fs: &mut Filesystem, device: &mut D, size: u64) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        if size > self.get_size() {
            self.inode.size = size;
            fs.set_inode(device, self.inode_count, self.inode)?;
        } else {
            let start_block = if size % BLOCK_SIZE as u64 == 0 {
                size / BLOCK_SIZE as u64 + 1
            } else {
                size / BLOCK_SIZE as u64 + 2
            };

            let end_block = if self.get_size() % BLOCK_SIZE as u64 == 0 {
                self.get_size() / BLOCK_SIZE as u64
            } else {
                self.get_size() / BLOCK_SIZE as u64 + 1
            };

            for i in start_block..end_block {
                self.inode.btree_depth =
                    self.btree_root
                        .offset_remove(fs, device, i, self.inode.btree_depth as usize)?
                        as u8;
            }
            self.inode.size = size;
            fs.set_inode(device, self.inode_count, self.inode)?;
        }
        Ok(())
    }
    pub fn get_size(&self) -> u64 {
        self.inode.size
    }
    pub fn get_inode(&self) -> u64 {
        self.inode_count
    }
    /** Read symbol link */
    pub fn read_link<D>(&self, fs: &mut Filesystem, device: &mut D) -> IOResult<String>
    where
        D: Read + Write + Seek,
    {
        if !self.inode.is_symlink() {
            return Err(Error::new(ErrorKind::PermissionDenied, "Not a symbol link"));
        }
        let mut path = vec![0; self.get_size() as usize];
        self.read(fs, device, 0, &mut path, self.get_size())?;
        Ok(String::from_utf8_lossy(&path).to_string())
    }
    /** Remove a file */
    pub fn remove<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let fd = Self::open(fs, device, path)?;
        remove_by_inode(fs, device, fd.inode_count)?;
        Ok(())
    }
}

/** Create a file and return the inode count */
pub fn create<D>(fs: &mut Filesystem, device: &mut D) -> Option<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = fs.new_inode(device).unwrap();
    let mut inode = fs.get_inode(device, inode_count).unwrap();
    let btree_root = fs.new_block().unwrap();
    inode.btree_root = btree_root;
    let btree = BtreeNode::default();
    fs.set_data_block(device, btree_root, btree.dump()).unwrap();
    fs.set_inode(device, inode_count, inode).unwrap();

    Some(inode_count)
}

/** Create a symbol link and return the inode count */
pub fn create_symlink<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = create(fs, device).unwrap();
    File::open_by_inode(fs, device, inode_count)?.write(fs, device, 0, path.as_bytes())?;
    let mut inode = fs.get_inode(device, inode_count)?;
    inode.permission |= ACL_SYMBOLLINK;
    fs.set_inode(device, inode_count, inode)?;

    Ok(inode_count)
}

/** Remove a file */
pub fn remove_by_inode<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let mut inode = fs.get_inode(device, inode_count).unwrap();

    if inode.hlinks > 0 {
        inode.hlinks -= 1;
        fs.set_inode(device, inode_count, inode)?;
    } else {
        let mut btree_root = BtreeNode::new(
            inode.btree_root,
            &fs.get_data_block(device, inode.btree_root)?,
        );

        btree_root.destroy(fs, device, inode.btree_depth as usize);
        fs.release_inode(inode_count);
    }
    Ok(())
}

/** Copy a file */
pub fn copy_by_inode<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode = fs.get_inode(device, inode_count).unwrap();
    let new_inode_count = fs.new_inode(device).unwrap();
    let mut new_inode = INode::default();

    let mut btree_root = BtreeNode::new(
        inode.btree_root,
        &fs.get_data_block(device, inode.btree_root)?,
    );
    btree_root.clone_tree(fs, device, inode.btree_depth as usize);
    new_inode.size = inode.size;
    new_inode.btree_root = inode.btree_root;
    new_inode.btree_depth = inode.btree_depth;
    fs.set_inode(device, new_inode_count, new_inode)?;
    Ok(new_inode_count)
}
