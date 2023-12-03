pub mod block;
pub mod btree;
pub mod dir;
pub mod file;
pub mod inode;

use std::io::{Read, Result as IOResult, Seek, Write};

use block::*;

pub const FS_MAGIC_HEADER: [u8; 4] = [0x31, 0xc0, 0x8e, 0xf5];
pub const FS_VERSION: u8 = 1;

#[derive(Debug, Default)]
pub struct Filesystem {
    pub sb: block::SuperBlock,
    pub groups: Vec<block::BlockGroup>,
}

impl Filesystem {
    pub fn create<D>(device: &mut D, block_size: usize) -> Self
    where
        D: Read + Write + Seek,
    {
        let mut fs = Self::default();
        let groups_count = block_size / block::GPOUP_SIZE;
        fs.sb.groups = groups_count as u64;
        fs.sb.uuid = *uuid::Uuid::new_v4().as_bytes();
        fs.groups = vec![block::BlockGroup::default(); groups_count];

        for (i, group) in fs.groups.iter_mut().enumerate() {
            group.group_count = i as u64;
        }

        fs.sb.root_inode = file::create(&mut fs, device).unwrap();

        fs
    }
    pub fn load<D>(device: &mut D) -> Self
    where
        D: Read + Write + Seek,
    {
        let sb = block::SuperBlock::load(block::load_block(device, 0).unwrap());

        let mut groups = vec![block::BlockGroup::default(); sb.groups as usize];

        for (i, group) in groups.iter_mut().enumerate() {
            group.group_count = i as u64;
            group.load(device).unwrap();
        }

        Self { sb, groups }
    }
    /** Allocate an inode */
    pub fn new_inode<D>(&mut self, device: &mut D) -> Option<u64>
    where
        D: Read + Write + Seek,
    {
        for group in &mut self.groups {
            if let Ok(inode) = group.new_inode(device) {
                return Some(inode);
            }
        }
        None
    }
    pub fn get_inode<D>(&self, device: &mut D, inode: u64) -> IOResult<inode::INode>
    where
        D: Read + Write + Seek,
    {
        let group = inode / INODE_PER_GROUP as u64;
        let relative_inode = inode % INODE_PER_GROUP as u64;
        self.groups[group as usize].get_inode(device, relative_inode)
    }
    pub fn set_inode<D>(&self, device: &mut D, count: u64, inode: inode::INode) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let group = count / INODE_PER_GROUP as u64;
        let relative_inode = count % INODE_PER_GROUP as u64;
        self.groups[group as usize].set_inode(device, relative_inode, inode)
    }
    /** Release an inode */
    pub fn release_inode(&mut self, inode: u64) {
        let group = inode / INODE_PER_GROUP as u64;
        let relative_inode = inode % INODE_PER_GROUP as u64;
        self.groups[group as usize].release_inode(relative_inode);
    }
    /** Allocate a data block */
    pub fn new_block(&mut self) -> Option<u64> {
        for (i, group) in self.groups.iter_mut().enumerate() {
            if let Some(count) = group.new_block() {
                return Some(data_block_relative_to_absolute!(i as u64, count));
            }
        }
        None
    }
    /** Copy out a mutiple referenced data block */
    pub fn block_copy_out<D>(&mut self, device: &mut D, count: u64) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let block = self.get_data_block(device, count)?;
        let new_block = self.new_block().unwrap();
        self.set_data_block(device, new_block, block)?;

        self.release_block(count);
        Ok(new_block)
    }
    /** Clone a data block */
    pub fn clone_block(&mut self, count: u64) {
        let group = (count as usize - 1) / GPOUP_SIZE;
        self.groups[group].clone_block((count - 1) % GPOUP_SIZE as u64);
    }
    /** Release a data block */
    pub fn release_block(&mut self, count: u64) {
        let group = (count as usize - 1) / GPOUP_SIZE;
        self.groups[group].release_block((count - 1) % GPOUP_SIZE as u64);
    }
    /** Load data block */
    pub fn set_data_block<D>(
        &mut self,
        device: &mut D,
        count: u64,
        block: [u8; BLOCK_SIZE],
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        device.seek(std::io::SeekFrom::Start(count * BLOCK_SIZE as u64))?;
        device.write_all(&block)?;
        Ok(())
    }
    pub fn is_multireference(&self, count: u64) -> bool {
        let group = (count as usize - 1) / GPOUP_SIZE;
        let relative_count = (count as usize - 1) % GPOUP_SIZE;
        self.groups[group].block_map[relative_count / (BLOCK_SIZE / 2)].counts
            [relative_count % (BLOCK_SIZE / 2)]
            > 1
    }
    /** Dump data block */
    pub fn get_data_block<D>(&self, device: &mut D, count: u64) -> IOResult<[u8; BLOCK_SIZE]>
    where
        D: Read + Write + Seek,
    {
        device.seek(std::io::SeekFrom::Start(count * BLOCK_SIZE as u64))?;
        let mut block = [0; BLOCK_SIZE];
        device.read_exact(&mut block)?;
        Ok(block)
    }
    /** Synchronize meta data to disk */
    pub fn sync<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.sb.sync(0, device)?;
        for group in &mut self.groups {
            group.sync(device)?;
        }

        Ok(())
    }
}
