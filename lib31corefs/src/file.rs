use crate::block::*;
use crate::btree::*;
use crate::inode::INode;
use crate::Filesystem;

use std::io::{Read, Result as IOResult, Seek, Write};

pub struct File {
    fd: INode,
    inode: u64,
}

impl File {
    pub fn open_by_inode<D>(fs: &mut Filesystem, device: &mut D, inode: u64) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        Ok(Self {
            fd: fs.get_inode(device, inode)?,
            inode,
        })
    }
    pub fn insert<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        mut data: &[u8],
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut btree_root = BtreeNode::new(
            self.fd.btree_root,
            &fs.get_data_block(device, self.fd.btree_root)?,
        );

        if let Some((block, block_offset, available_size)) =
            btree_root.offset_lookup(fs, device, offset)
        {
            let written_size = std::cmp::min(available_size as usize, data.len());
            let mut data_block = fs.get_data_block(device, block)?;
            data_block[block_offset as usize..block_offset as usize + written_size]
                .copy_from_slice(&data[..written_size]);
            self.fd.size += written_size as u64;
            fs.set_inode(device, self.inode, self.fd)?;
            fs.set_data_block(device, block, data_block)?;

            data = &data[written_size..];
            if !data.is_empty() {
                self.insert(fs, device, offset + written_size as u64, data)?;
            }
        } else {
            let block = fs.new_block().unwrap();
            btree_root.offset_insert(fs, device, offset, block)?;
            btree_root.offset_adjust(fs, device, offset, (data.len() % BLOCK_SIZE) as u64, true);

            let mut block_data = [0; BLOCK_SIZE];
            block_data[..data.len() % BLOCK_SIZE].copy_from_slice(&data[..data.len() % BLOCK_SIZE]);
            self.fd.size += (data.len() % BLOCK_SIZE) as u64;
            fs.set_inode(device, self.inode, self.fd)?;
            fs.set_data_block(device, block, block_data)?;
            fs.set_data_block(device, self.fd.btree_root, btree_root.dump())?;

            data = &data[data.len() % BLOCK_SIZE..];
            if !data.is_empty() {
                self.insert(fs, device, offset, data)?;
            }
        }
        Ok(())
    }

    pub fn read<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        data: &mut [u8],
        size: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let btree_root = BtreeNode::new(
            self.fd.btree_root,
            &fs.get_data_block(device, self.fd.btree_root)?,
        );

        if let Some((block, offset, available_size)) = btree_root.offset_lookup(fs, device, offset)
        {
            let block = fs.get_data_block(device, block)?;
            let written_size = std::cmp::min(size, available_size);
            data[..written_size as usize]
                .copy_from_slice(&block[offset as usize..offset as usize + written_size as usize]);
            if written_size < size {
                self.read(
                    fs,
                    device,
                    offset + written_size,
                    &mut data[written_size as usize..],
                    size - written_size,
                )?;
            }
        } else if let Some((_, block_offset, _)) =
            btree_root.offset_lookup(fs, device, offset + BLOCK_SIZE as u64)
        {
            let written_size = offset + BLOCK_SIZE as u64 - block_offset;
            data[..written_size as usize].copy_from_slice(&[0].repeat(written_size as usize));

            if written_size < size {
                self.read(
                    fs,
                    device,
                    offset + written_size,
                    &mut data[written_size as usize..],
                    size - written_size,
                )?;
            } else {
                let written_size = (size as usize % BLOCK_SIZE) as u64;
                data[..written_size as usize].copy_from_slice(&[0].repeat(written_size as usize));

                if written_size < size {
                    self.read(
                        fs,
                        device,
                        offset + written_size,
                        &mut data[written_size as usize..],
                        size - written_size,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn delete<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        size: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut inode = fs.get_inode(device, self.inode)?;
        let mut btree_root = BtreeNode::new(
            inode.btree_root,
            &fs.get_data_block(device, inode.btree_root)?,
        );

        if let Some((block, block_offset, available_size)) =
            btree_root.offset_lookup(fs, device, offset)
        {
            let written_size = std::cmp::min(available_size, size);
            let mut data_block = fs.get_data_block(device, block)?;
            data_block[block_offset as usize..(block_offset + written_size) as usize]
                .copy_from_slice(&[0].repeat(written_size as usize));
            fs.set_data_block(device, block, data_block)?;
        }
        btree_root.offset_adjust(fs, device, offset, size, false);
        inode.size -= size;
        fs.set_inode(device, self.inode, inode)?;

        Ok(())
    }
}

pub fn create<D>(fs: &mut Filesystem, device: &mut D) -> Option<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = fs.new_inode(device).unwrap();
    let mut inode = fs.get_inode(device, inode_count).unwrap();
    let btree_root = fs.new_block().unwrap();
    inode.btree_root = btree_root;
    let btree = BtreeNode::new_node(BTREE_LEAF);
    fs.set_data_block(device, btree_root, btree.dump()).unwrap();
    fs.set_inode(device, inode_count, inode).unwrap();

    Some(inode_count)
}
