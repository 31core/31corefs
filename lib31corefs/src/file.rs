use crate::block::*;
use crate::btree::*;
use crate::inode::INode;
use crate::Filesystem;

use std::io::{Read, Result as IOResult, Seek, Write};

pub struct File {
    fd: INode,
    inode: u64,
    btree_root: crate::btree::BtreeNode,
}

impl File {
    pub fn open_by_inode<D>(fs: &mut Filesystem, device: &mut D, inode: u64) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let fd = fs.get_inode(device, inode)?;

        Ok(Self {
            fd,
            inode,
            btree_root: BtreeNode::new(fd.btree_root, &fs.get_data_block(device, fd.btree_root)?),
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
            let block_count = offset / BLOCK_SIZE as u64;
            let block_offset = offset % BLOCK_SIZE as u64;
            if let Some(block) =
                self.btree_root
                    .offset_lookup(fs, device, block_count, self.fd.btree_depth as usize)
            {
                let written_size = std::cmp::min(data.len(), BLOCK_SIZE - block_offset as usize);
                let mut data_block = fs.get_data_block(device, block)?;

                data_block[block_offset as usize..block_offset as usize + written_size]
                    .copy_from_slice(&data[..written_size]);

                if fs.is_multireference(block) {
                    let new_block = fs.block_copy_out(device, block)?;
                    self.btree_root.offset_remove(
                        fs,
                        device,
                        block_count,
                        self.fd.btree_depth as usize,
                    )?;
                    self.btree_root.offset_insert(
                        fs,
                        device,
                        block_count,
                        new_block,
                        self.fd.btree_depth as usize,
                    )?;
                    fs.set_data_block(device, new_block, data_block)?;
                } else {
                    fs.set_data_block(device, block, data_block)?;
                }

                self.fd.size += written_size as u64;

                data = &data[written_size..];
                offset += written_size as u64;
            } else {
                let written_size = std::cmp::min(data.len(), BLOCK_SIZE);
                let data_block_count = fs.new_block().unwrap();
                self.fd.btree_depth = self.btree_root.offset_insert(
                    fs,
                    device,
                    block_count,
                    data_block_count,
                    self.fd.btree_depth as usize,
                )? as u8;

                let mut block_data = [0; BLOCK_SIZE];
                block_data[..written_size].copy_from_slice(&data[..written_size]);
                self.fd.size += written_size as u64;

                fs.set_data_block(device, data_block_count, block_data)?;

                data = &data[written_size..];
                offset += written_size as u64;
            }
        }
        fs.set_inode(device, self.inode, self.fd)?;
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
            self.fd.btree_root,
            &fs.get_data_block(device, self.fd.btree_root)?,
        );

        loop {
            let block_count = offset / BLOCK_SIZE as u64;
            let block_offset = offset % BLOCK_SIZE as u64;
            if let Some(block) =
                btree_root.offset_lookup(fs, device, block_count, self.fd.btree_depth as usize)
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
    pub fn get_size(&self) -> u64 {
        self.fd.size
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

/** Remove a file */
pub fn remove<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<()>
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
pub fn copy<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode = fs.get_inode(device, inode_count).unwrap();
    let new_inode = fs.new_inode(device).unwrap();
    let mut new = fs.get_inode(device, new_inode).unwrap();

    let mut btree_root = BtreeNode::new(
        inode.btree_root,
        &fs.get_data_block(device, inode.btree_root)?,
    );
    btree_root.clone_tree(fs, device, inode.btree_depth as usize);
    new.btree_root = inode.btree_root;
    new.btree_depth = inode.btree_depth;
    fs.set_inode(device, new_inode, new)?;
    Ok(new_inode)
}
