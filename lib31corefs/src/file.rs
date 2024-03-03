use crate::block::*;
use crate::btree::*;
use crate::dir::Directory;
use crate::inode::{INode, ACL_FILE, ACL_SYMBOLLINK, INODE_PER_GROUP};
use crate::subvol::Subvolume;
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
    pub fn create<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = create(fs, subvol, device)?;

        let mut dir = Directory::open(fs, subvol, device, &dir_name!(path))?;
        dir.add_file(fs, subvol, device, &base_name!(path), inode_count)?;

        Self::open_by_inode(fs, subvol, device, inode_count)
    }
    /** Create a symbol link */
    pub fn create_symlink<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
        point_to: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = create_symlink(fs, subvol, device, point_to)?;

        let mut dir = Directory::open(fs, subvol, device, &dir_name!(path))?;
        dir.add_file(fs, subvol, device, &base_name!(path), inode_count)?;

        Self::open_by_inode(fs, subvol, device, inode_count)
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
                BtreeType::Leaf,
                &fs.get_data_block(device, inode.btree_root)?,
            ),
        })
    }
    /** Open regular file by absolute path */
    pub fn open<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = match Directory::open(fs, subvol, device, &dir_name!(path))?
            .list_dir(fs, subvol, device)?
            .get(&base_name!(path))
        {
            Some(count) => *count,
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("No such file '{}'", path),
                ))
            }
        };

        let inode = subvol.get_inode(fs, device, inode_count)?;

        /* read link and open orignal file */
        if inode.is_symlink() {
            let real_path =
                Self::from_inode(fs, device, inode_count, inode)?.read_link(fs, subvol, device)?;
            Self::open(fs, subvol, device, &real_path)
        } else if inode.is_dir() {
            Err(Error::new(
                ErrorKind::Unsupported,
                format!("'{}' is a directory.", path),
            ))
        } else {
            Self::open_by_inode(fs, subvol, device, inode_count)
        }
    }
    /** Open a file by inode count */
    pub fn open_by_inode<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        inode_count: u64,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode = subvol.get_inode(fs, device, inode_count)?;

        Ok(Self {
            inode,
            inode_count,
            btree_root: BtreeNode::new(
                inode.btree_root,
                BtreeType::Leaf,
                &fs.get_data_block(device, inode.btree_root)?,
            ),
        })
    }
    /** Write data */
    pub fn write<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        mut offset: u64,
        mut data: &[u8],
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.handle_rc_inode(fs, subvol, device)?;

        while !data.is_empty() {
            let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
            let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block

            let written_size = std::cmp::min(data.len(), BLOCK_SIZE - block_offset as usize);

            /* data block has been allocated */
            if let Ok(entry) = self.btree_root.lookup(fs, device, block_count) {
                let block = entry.value;
                let mut data_block = fs.get_data_block(device, block)?;

                data_block[block_offset as usize..block_offset as usize + written_size]
                    .copy_from_slice(&data[..written_size]);

                if entry.rc > 0 {
                    let new_block = crate::block::block_copy_out(fs, device, block)?;
                    self.btree_root.modify(fs, device, block_count, new_block)?;
                    self.inode.btree_root = self.btree_root.block_count;
                    fs.set_data_block(device, new_block, data_block)?;
                } else {
                    fs.set_data_block(device, block, data_block)?;
                }
            } else {
                let data_block_count = fs.new_block()?;
                self.btree_root
                    .insert(fs, device, block_count, data_block_count)?;
                self.inode.btree_root = self.btree_root.block_count;

                let mut block_data = [0; BLOCK_SIZE];
                block_data[block_offset as usize..block_offset as usize + written_size]
                    .copy_from_slice(&data[..written_size]);

                fs.set_data_block(device, data_block_count, block_data)?;
            }

            if offset + written_size as u64 > self.inode.size {
                self.inode.size += offset + written_size as u64 - self.inode.size;
            }

            data = &data[written_size..];
            offset += written_size as u64;
        }

        self.inode.update_mtime();
        subvol.set_inode(fs, device, self.inode_count, self.inode)?;
        Ok(())
    }
    /** Read from file */
    pub fn read<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        mut offset: u64,
        mut buffer: &mut [u8],
        mut size: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let btree_root = BtreeNode::new(
            self.inode.btree_root,
            BtreeType::Leaf,
            &fs.get_data_block(device, self.inode.btree_root)?,
        );

        loop {
            let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
            let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block

            let read_size;
            if let Ok(entry) = btree_root.lookup(fs, device, block_count) {
                let block = entry.value;
                let block = fs.get_data_block(device, block)?;
                read_size = std::cmp::min(size as usize, BLOCK_SIZE - block_offset as usize);
                buffer[..read_size].copy_from_slice(
                    &block[block_offset as usize..block_offset as usize + read_size],
                );
            }
            /* section with unallocated data block in sparse file, fill zero bytes */
            else {
                read_size = std::cmp::min(size as usize, BLOCK_SIZE);

                buffer[..read_size].copy_from_slice(&[0].repeat(read_size));
            }

            if read_size < size as usize {
                offset += read_size as u64;
                size -= read_size as u64;
                buffer = &mut buffer[read_size..];
            } else {
                break;
            }
        }

        self.inode.update_atime();
        subvol.set_inode(fs, device, self.inode_count, self.inode)?;
        Ok(())
    }
    /** Adjust file size */
    pub fn truncate<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        size: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.handle_rc_inode(fs, subvol, device)?;

        /* reduce file size */
        if size < self.inode.size {
            let start_block = if size % BLOCK_SIZE as u64 == 0 {
                size / BLOCK_SIZE as u64 + 1
            } else {
                size / BLOCK_SIZE as u64 + 2
            };

            let end_block = if self.inode.size % BLOCK_SIZE as u64 == 0 {
                self.inode.size / BLOCK_SIZE as u64
            } else {
                self.inode.size / BLOCK_SIZE as u64 + 1
            };

            for i in start_block..end_block {
                self.btree_root.remove(fs, device, i)?;
            }
        }
        self.inode.size = size;
        self.inode.update_mtime();
        subvol.set_inode(fs, device, self.inode_count, self.inode)?;
        Ok(())
    }
    pub fn get_inode_count(&self) -> u64 {
        self.inode_count
    }
    pub fn get_inode(&self) -> INode {
        self.inode
    }
    /** Read symbol link */
    pub fn read_link<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<String>
    where
        D: Read + Write + Seek,
    {
        if !self.inode.is_symlink() {
            return Err(Error::new(ErrorKind::PermissionDenied, "Not a symbol link"));
        }
        let mut path = vec![0; self.inode.size as usize];
        self.read(fs, subvol, device, 0, &mut path, self.inode.size)?;
        Ok(String::from_utf8_lossy(&path).to_string())
    }
    /** Copy a regular file or a symbol link */
    pub fn copy<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        src: &str,
        dst: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let fd = Self::open(fs, subvol, device, src)?;
        let inode = copy_by_inode(fs, subvol, device, fd.inode_count)?;

        Directory::open(fs, subvol, device, &dir_name!(src))?.add_file(
            fs,
            subvol,
            device,
            &base_name!(dst),
            inode,
        )?;

        Ok(())
    }
    /** Remove a regular file or a symbol link */
    pub fn remove<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut fd = Self::open(fs, subvol, device, path)?;

        fd.handle_rc_inode(fs, subvol, device)?;

        if fd.inode.is_dir() {
            Directory::remove(fs, subvol, device, path)?;
        } else {
            remove_by_inode(fs, subvol, device, fd.inode_count)?;

            Directory::open(fs, subvol, device, &dir_name!(path))?.remove_file(
                fs,
                subvol,
                device,
                &base_name!(path),
            )?;
        }

        Ok(())
    }
    /** Before writing a multi-referenced file, first do these steps:
     * * Clone data blocks of each inode in the group
     * * Clone the inode group
     */
    fn handle_rc_inode<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let inode_group_count = self.inode_count / INODE_PER_GROUP as u64;
        /* check if the inode is multiple referenced */
        let btree_query_result = subvol
            .igroup_mgt_btree
            .lookup(fs, device, inode_group_count)?;
        let inode_group_block = btree_query_result.value;
        if btree_query_result.rc > 0 {
            let mut inode_group = INodeGroup::load(fs.get_data_block(device, inode_group_block)?);
            /* clone data blocks of each inode in the group */
            for (i, inode) in inode_group.inodes.iter().enumerate() {
                if !inode.is_empty_inode() {
                    clone_by_inode(
                        fs,
                        subvol,
                        device,
                        self.inode_count - (self.inode_count % INODE_PER_GROUP as u64) + i as u64,
                    )?;
                }
            }
            /* clone inode group */
            let new_inode_group_block = fs.new_block()?;
            inode_group.sync(device, new_inode_group_block)?;
            subvol
                .igroup_mgt_btree
                .modify(fs, device, inode_group_count, new_inode_group_block)?;
            subvol.entry.inode_tree_root = subvol.igroup_mgt_btree.block_count;
            crate::subvol::SubvolumeManager::set_subvolume(
                fs,
                device,
                fs.sb.subvol_mgr,
                subvol.entry.id,
                subvol.entry,
            )?;
        }

        Ok(())
    }
}

/** Create a file and return the inode count */
pub fn create<D>(fs: &mut Filesystem, subvol: &mut Subvolume, device: &mut D) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = subvol.new_inode(fs, device)?;
    let btree_root = BtreeNode::allocate_on_block(fs, device)?;

    let inode = INode {
        btree_root,
        permission: ACL_FILE,
        ..Default::default()
    };
    subvol.set_inode(fs, device, inode_count, inode)?;

    Ok(inode_count)
}

/** Create a symbol link and return the inode count */
pub fn create_symlink<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    path: &str,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = create(fs, subvol, device)?;
    let inode = INode {
        permission: ACL_SYMBOLLINK,
        ..Default::default()
    };
    subvol.set_inode(fs, device, inode_count, inode)?;

    File::open_by_inode(fs, subvol, device, inode_count)?.write(
        fs,
        subvol,
        device,
        0,
        path.as_bytes(),
    )?;

    Ok(inode_count)
}

/** Remove a file */
pub fn remove_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let mut inode = subvol.get_inode(fs, device, inode_count)?;

    if inode.hlinks > 0 {
        inode.hlinks -= 1;
        subvol.set_inode(fs, device, inode_count, inode)?;
    } else {
        let mut btree_root = BtreeNode::new(
            inode.btree_root,
            BtreeType::Leaf,
            &fs.get_data_block(device, inode.btree_root)?,
        );

        btree_root.destroy(fs, device)?;
        subvol.release_inode(fs, device, inode_count)?;
    }
    Ok(())
}

/** Copy a file */
pub fn copy_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(fs, device, inode_count)?;
    let new_inode_count = subvol.new_inode(fs, device)?;
    let mut new_inode = INode::default();

    clone_by_inode(fs, subvol, device, inode_count)?;
    new_inode.size = inode.size;
    new_inode.btree_root = inode.btree_root;
    subvol.set_inode(fs, device, new_inode_count, new_inode)?;
    Ok(new_inode_count)
}

/** Clone a file, do not allocate inode */
pub fn clone_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(fs, device, inode_count)?;
    let mut btree_root = BtreeNode::new(
        inode.btree_root,
        BtreeType::Leaf,
        &fs.get_data_block(device, inode.btree_root)?,
    );
    btree_root.clone_tree(fs, device)?;
    Ok(())
}
