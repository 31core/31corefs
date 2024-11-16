use crate::block::*;
use crate::btree::*;
use crate::dir::Directory;
use crate::inode::{INode, ACL_REGULAR_FILE, INODE_PER_GROUP, PERMISSION_BITS};
use crate::path_util::{base_name, dir_path};
use crate::subvol::Subvolume;
use crate::symlink::read_link_from_inode;
use crate::Filesystem;

use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, Write};
use std::path::Path;

#[derive(Debug)]
pub struct File {
    inode: INode,
    inode_count: u64,
    btree_root: Option<BtreeNode>,
}

impl File {
    /** Create a file */
    pub fn create<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let inode_count = create(fs, subvol, device)?;

        let mut dir = Directory::open(fs, subvol, device, dir_path(path.as_ref()))?;
        dir.add_file(fs, subvol, device, base_name(path.as_ref()), inode_count)?;

        Self::open_by_inode(subvol, device, inode_count)
    }
    pub(crate) fn from_inode<D>(device: &mut D, inode_count: u64, inode: INode) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let btree_root = if inode.btree_root != 0 {
            Some(BtreeNode::new(
                inode.btree_root,
                BtreeType::Leaf,
                &load_block(device, inode.btree_root)?,
            ))
        } else {
            None
        };

        Ok(Self {
            inode,
            inode_count,
            btree_root,
        })
    }
    /** Open regular file by absolute path */
    pub fn open<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let inode_count = Directory::open(fs, subvol, device, dir_path(path.as_ref()))?
            .find_inode_by_name(fs, subvol, device, base_name(path.as_ref()))?;

        let inode = subvol.get_inode(device, inode_count)?;

        /* read link and open orignal file */
        if inode.is_symlink() {
            let real_path = read_link_from_inode(subvol, device, inode_count)?;
            Self::open(fs, subvol, device, &real_path)
        } else if inode.is_dir() {
            Err(Error::new(
                ErrorKind::Unsupported,
                format!("'{}' is a directory.", path.as_ref().to_str().unwrap()),
            ))
        } else {
            Self::open_by_inode(subvol, device, inode_count)
        }
    }
    /** Open a file by inode count */
    pub(crate) fn open_by_inode<D>(
        subvol: &mut Subvolume,
        device: &mut D,
        inode_count: u64,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode = subvol.get_inode(device, inode_count)?;

        let btree_root = if inode.btree_root != 0 {
            Some(BtreeNode::new(
                inode.btree_root,
                BtreeType::Leaf,
                &load_block(device, inode.btree_root)?,
            ))
        } else {
            None
        };

        Ok(Self {
            inode,
            inode_count,
            btree_root,
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

        if self.btree_root.is_none() {
            self.inode.btree_root = BtreeNode::allocate_on_block_subvol(fs, subvol, device)?;
            self.btree_root = Some(BtreeNode {
                block_count: self.inode.btree_root,
                r#type: BtreeType::Leaf,
                ..Default::default()
            });
        }

        while !data.is_empty() {
            let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
            let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block

            let written_size = std::cmp::min(data.len(), BLOCK_SIZE - block_offset as usize);
            if let Some(btree_root) = &mut self.btree_root {
                /* data block has been allocated */
                if let Ok(entry) = btree_root.lookup(device, block_count) {
                    let block = entry.value;
                    let mut data_block = load_block(device, block)?;

                    data_block[block_offset as usize..block_offset as usize + written_size]
                        .copy_from_slice(&data[..written_size]);

                    if entry.rc > 0 {
                        let new_block = crate::block::block_copy_out(fs, subvol, device, block)?;
                        btree_root.modify(fs, subvol, device, block_count, new_block)?;
                        self.inode.btree_root = btree_root.block_count;
                        save_block(device, new_block, data_block)?;
                    } else {
                        save_block(device, block, data_block)?;
                    }
                } else {
                    let data_block_count = subvol.new_block(fs, device)?;
                    btree_root.insert(fs, subvol, device, block_count, data_block_count)?;
                    self.inode.btree_root = btree_root.block_count;

                    let mut block_data = [0; BLOCK_SIZE];
                    block_data[block_offset as usize..block_offset as usize + written_size]
                        .copy_from_slice(&data[..written_size]);

                    save_block(device, data_block_count, block_data)?;
                }

                if offset + written_size as u64 > self.inode.size {
                    self.inode.size = offset + written_size as u64;
                }

                data = &data[written_size..];
                offset += written_size as u64;
            }
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
        if self.btree_root.is_none() {
            buffer[..size as usize].fill(0);
        } else if let Some(btree_root) = &mut self.btree_root {
            loop {
                let block_count = offset / BLOCK_SIZE as u64; // the block count to be write
                let block_offset = offset % BLOCK_SIZE as u64; // the relative offset to the block

                let read_size;
                if let Ok(entry) = btree_root.lookup(device, block_count) {
                    let block = entry.value;
                    let block = load_block(device, block)?;
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

        if let Some(btree) = &mut self.btree_root {
            /* reduce file size */
            if size > 0 && size < self.inode.size {
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
                    if btree.lookup(device, i).is_ok() {
                        btree.remove(fs, subvol, device, i)?;
                    }
                }
            } else if size == 0 {
                btree.destroy(fs, subvol, device)?;
                self.inode.btree_root = 0;
                self.btree_root = None;
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
    /** Copy a regular file or a symbol link */
    pub fn copy<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        src: P,
        dst: P,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let fd = Self::open(fs, subvol, device, &src)?;
        let inode = copy_by_inode(fs, subvol, device, fd.inode_count)?;

        Directory::open(fs, subvol, device, dir_path(src.as_ref()))?.add_file(
            fs,
            subvol,
            device,
            base_name(dst.as_ref()),
            inode,
        )?;

        Ok(())
    }
    /** Remove a regular file or a symbol link */
    pub fn remove<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let mut fd = Self::open(fs, subvol, device, &path)?;

        fd.handle_rc_inode(fs, subvol, device)?;

        if fd.inode.is_dir() {
            Directory::remove(fs, subvol, device, path)?;
        } else {
            remove_by_inode(fs, subvol, device, fd.inode_count)?;

            Directory::open(fs, subvol, device, dir_path(path.as_ref()))?.remove_file(
                fs,
                subvol,
                device,
                base_name(path.as_ref()),
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
        let btree_query_result = subvol.igroup_mgt_btree.lookup(device, inode_group_count)?;
        let inode_group_block = btree_query_result.value;
        if btree_query_result.rc > 0 {
            let mut inode_group = INodeGroup::load(load_block(device, inode_group_block)?);
            /* clone data blocks of each inode in the group */
            for (i, inode) in inode_group.inodes.iter().enumerate() {
                if !inode.is_empty_inode() {
                    clone_by_inode(
                        subvol,
                        device,
                        self.inode_count - (self.inode_count % INODE_PER_GROUP as u64) + i as u64,
                    )?;
                }
            }
            /* clone inode group */
            let new_inode_group_block = subvol.new_block(fs, device)?;
            inode_group.sync(device, new_inode_group_block)?;
            subvol.igroup_mgt_btree.modify(
                fs,
                &mut subvol.clone(),
                device,
                inode_group_count,
                new_inode_group_block,
            )?;
            subvol.entry.inode_tree_root = subvol.igroup_mgt_btree.block_count;
            crate::subvol::SubvolumeManager::set_subvolume(
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
pub(crate) fn create<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = subvol.new_inode(fs, device)?;

    let inode = INode {
        acl: ACL_REGULAR_FILE << PERMISSION_BITS,
        ..Default::default()
    };
    subvol.set_inode(fs, device, inode_count, inode)?;

    Ok(inode_count)
}

/** Remove a file */
pub(crate) fn remove_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let mut inode = subvol.get_inode(device, inode_count)?;

    if inode.hlinks > 0 {
        inode.hlinks -= 1;
        subvol.set_inode(fs, device, inode_count, inode)?;
    } else if inode.btree_root != 0 {
        let mut btree_root = BtreeNode::new(
            inode.btree_root,
            BtreeType::Leaf,
            &load_block(device, inode.btree_root)?,
        );

        btree_root.destroy(fs, subvol, device)?;
        subvol.release_inode(fs, device, inode_count)?;
    }
    Ok(())
}

/** Copy a file */
pub(crate) fn copy_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(device, inode_count)?;
    let new_inode_count = subvol.new_inode(fs, device)?;
    let mut new_inode = INode::default();

    clone_by_inode(subvol, device, inode_count)?;
    new_inode.size = inode.size;
    new_inode.btree_root = inode.btree_root;
    subvol.set_inode(fs, device, new_inode_count, new_inode)?;
    Ok(new_inode_count)
}

/** Clone a file, do not allocate inode */
pub(crate) fn clone_by_inode<D>(
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(device, inode_count)?;
    let mut btree_root = BtreeNode::new(
        inode.btree_root,
        BtreeType::Leaf,
        &load_block(device, inode.btree_root)?,
    );
    btree_root.clone_tree(device)?;
    Ok(())
}
