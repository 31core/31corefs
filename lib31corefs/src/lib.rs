pub mod block;
pub mod btree;
pub mod dir;
pub mod file;
pub mod inode;
pub mod subvol;

use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, SeekFrom, Write};

use block::*;
use subvol::{Subvolume, SubvolumeManager};

pub const FS_MAGIC_HEADER: [u8; 4] = [0x31, 0xc0, 0x8e, 0xf5];
pub const FS_VERSION: u8 = 1;

#[derive(Debug, Default)]
pub struct Filesystem {
    pub sb: SuperBlock,
    pub subvol_mgr: SubvolumeManager,
    pub groups: Vec<BlockGroup>,
}

impl Filesystem {
    pub fn create<D>(device: &mut D, block_size: usize) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let mut fs = Self::default();
        let groups_count = block_size / GPOUP_SIZE;
        fs.sb.groups = groups_count as u64;
        fs.sb.uuid = *uuid::Uuid::new_v4().as_bytes();
        fs.sb.total_blocks = block_size as u64;
        fs.groups = vec![BlockGroup::default(); groups_count];

        for (i, group) in fs.groups.iter_mut().enumerate() {
            group.group_count = i as u64;
        }

        fs.sb.subvol_mgr = subvol::SubvolumeManager::allocate_on_block(&mut fs, device)?;

        fs.sb.default_subvol = fs.new_subvolume(device)?;

        Ok(fs)
    }
    pub fn load<D>(device: &mut D) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let sb = SuperBlock::load(block::load_block(device, 0)?);

        let mut groups = vec![BlockGroup::default(); sb.groups as usize];

        for (i, group) in groups.iter_mut().enumerate() {
            group.group_count = i as u64;
            group.load(device)?;
        }

        let subvol_mgr = SubvolumeManager::load(load_block(device, sb.subvol_mgr)?);

        Ok(Self {
            sb,
            groups,
            subvol_mgr,
        })
    }
    /** Allocate a data block */
    pub fn new_block(&mut self) -> IOResult<u64> {
        for (i, group) in self.groups.iter_mut().enumerate() {
            if let Ok(count) = group.new_block() {
                return Ok(data_block_relative_to_absolute!(i as u64, count));
            }
        }
        Err(Error::new(ErrorKind::Other, "No enough block"))
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
        device.seek(SeekFrom::Start(count * BLOCK_SIZE as u64))?;
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
        device.seek(SeekFrom::Start(count * BLOCK_SIZE as u64))?;
        let mut block = [0; BLOCK_SIZE];
        device.read_exact(&mut block)?;
        Ok(block)
    }
    /** Synchronize meta data to disk */
    pub fn sync_meta_data<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.sb.sync(device, 0)?;
        for group in &mut self.groups {
            group.sync(device)?;
        }

        Ok(())
    }
    pub fn new_subvolume<D>(&mut self, device: &mut D) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let subvol_mgr = self.sb.subvol_mgr;
        SubvolumeManager::new_subvolume(self, device, subvol_mgr)
    }
    pub fn get_subvolume<D>(&self, device: &mut D, id: u64) -> Subvolume
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::get_subvolume(self, device, self.sb.subvol_mgr, id).unwrap()
    }
    pub fn get_default_subvolume<D>(&self, device: &mut D) -> Subvolume
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::get_subvolume(self, device, self.sb.subvol_mgr, self.sb.default_subvol)
            .unwrap()
    }
    /** Create a snapshot */
    pub fn create_snapshot<D>(&mut self, device: &mut D, id: u64) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::create_snapshot(self, device, self.sb.subvol_mgr, id)
    }
}
