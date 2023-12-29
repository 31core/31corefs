use crate::inode::*;
use std::io::{Error, ErrorKind, Read, Result as IOResult, Seek, SeekFrom, Write};

pub const GPOUP_SIZE: usize = BLOCK_MAP_SIZE + DATA_BLOCK_PER_GROUP;

pub const BLOCK_SIZE: usize = 4096;
pub const DATA_BLOCK_PER_GROUP: usize = BLOCK_MAP_SIZE * (BLOCK_SIZE / 2);
pub const BLOCK_MAP_SIZE: usize = 32;

#[macro_export]
macro_rules! data_block_relative_to_absolute {
    ($group_count: expr, $count: expr) => {
        1 + $group_count * GPOUP_SIZE as u64 + BLOCK_MAP_SIZE as u64 + $count
    };
}

#[macro_export]
macro_rules! inode_table_relative_to_absolute {
    ($group_count: expr, $count: expr) => {
        1 + $group_count * GPOUP_SIZE as u64 + (INODE_BITMAP_SIZE + BLOCK_MAP_SIZE) as u64 + $count
    };
}

pub fn load_block<D>(device: &mut D, block_count: u64) -> IOResult<[u8; BLOCK_SIZE]>
where
    D: Read + Write + Seek,
{
    let mut block = [0; BLOCK_SIZE];
    device.seek(SeekFrom::Start(block_count * BLOCK_SIZE as u64))?;
    device.read_exact(&mut block)?;

    Ok(block)
}

pub trait Block: Default {
    /** Load from bytes */
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self;
    /** Dump to bytes */
    fn dump(&self) -> [u8; BLOCK_SIZE];
    /** Synchronize to device */
    fn sync<D>(&mut self, device: &mut D, block_count: u64) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        device.seek(SeekFrom::Start(block_count * BLOCK_SIZE as u64))?;
        device.write_all(&self.dump())?;
        Ok(())
    }
    /** Allocate and initialize an empty block on device */
    fn allocate_on_block<D>(fs: &mut crate::Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let block_count = fs.new_block()?;
        Self::default().sync(device, block_count)?;
        Ok(block_count)
    }
}

#[derive(Debug, Clone)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |4  |Magic header|
 * |4    |5  |Version    |
 * |5    |13 |Count of groups|
 * |13   |29 |UUID       |
 * |29   |285|Label      |
 * |285  |293|Total blocks|
 * |293  |301|Used blocks|
 * |301  |309|Real used blocks|
 * |309  |317|Subvolume block|
 * |317  |325|Default subvolume|
*/
pub struct SuperBlock {
    pub groups: u64,
    pub uuid: [u8; 16],
    pub label: [u8; 256],
    pub total_blocks: u64,
    pub used_blocks: u64,
    pub real_used_blocks: u64,
    pub default_subvol: u64,
    pub subvol_mgr: u64,
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            groups: 0,
            uuid: [0; 16],
            label: [0; 256],
            total_blocks: 0,
            used_blocks: 0,
            real_used_blocks: 0,
            subvol_mgr: 0,
            default_subvol: 0,
        }
    }
}

impl Block for SuperBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            groups: u64::from_be_bytes(bytes[5..13].try_into().unwrap()),
            uuid: bytes[13..29].try_into().unwrap(),
            label: bytes[29..285].try_into().unwrap(),
            total_blocks: u64::from_be_bytes(bytes[285..293].try_into().unwrap()),
            used_blocks: u64::from_be_bytes(bytes[293..301].try_into().unwrap()),
            real_used_blocks: u64::from_be_bytes(bytes[301..309].try_into().unwrap()),
            subvol_mgr: u64::from_be_bytes(bytes[309..317].try_into().unwrap()),
            default_subvol: u64::from_be_bytes(bytes[317..325].try_into().unwrap()),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..4].copy_from_slice(&crate::FS_MAGIC_HEADER);
        bytes[4] = crate::FS_VERSION;
        bytes[5..13].copy_from_slice(&self.groups.to_be_bytes());
        bytes[13..29].copy_from_slice(&self.uuid);
        bytes[29..285].copy_from_slice(&self.label);
        bytes[285..293].copy_from_slice(&self.total_blocks.to_be_bytes());
        bytes[293..301].copy_from_slice(&self.used_blocks.to_be_bytes());
        bytes[301..309].copy_from_slice(&self.real_used_blocks.to_be_bytes());
        bytes[309..317].copy_from_slice(&self.subvol_mgr.to_be_bytes());
        bytes[317..325].copy_from_slice(&self.default_subvol.to_be_bytes());

        bytes
    }
}

impl SuperBlock {
    pub fn set_label(&mut self, label: &str) {
        self.label = [0; 256];
        self.label[..label.len()].copy_from_slice(label.as_bytes());
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockGroup {
    pub group_count: u64,
    pub block_map: [BlockMapBlock; BLOCK_MAP_SIZE],
}

impl BlockGroup {
    pub fn load<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        for i in 0..BLOCK_MAP_SIZE as u64 {
            self.block_map[i as usize] = BlockMapBlock::load(load_block(
                device,
                GPOUP_SIZE as u64 * self.group_count + 1 + i,
            )?);
        }

        Ok(())
    }
    /** Allocate a data block */
    pub fn new_block(&mut self) -> IOResult<u64> {
        for block in 0..BLOCK_MAP_SIZE {
            for count in 0..BLOCK_SIZE / 2 {
                if self.block_map[block].counts[count] == 0 {
                    self.block_map[block].counts[count] = 1;
                    return Ok((block * (BLOCK_SIZE / 2) + count) as u64);
                }
            }
        }
        Err(Error::new(ErrorKind::Other, "No enough block"))
    }
    /** Clone a data block */
    pub fn clone_block(&mut self, count: u64) {
        let block = (count as usize - (GPOUP_SIZE - DATA_BLOCK_PER_GROUP)) / (BLOCK_SIZE / 2);
        let count = (count as usize - (GPOUP_SIZE - DATA_BLOCK_PER_GROUP)) % (BLOCK_SIZE / 2);
        self.block_map[block].counts[count] += 1;
    }
    /** Release a data block */
    pub fn release_block(&mut self, count: u64) {
        let block = (count as usize - (GPOUP_SIZE - DATA_BLOCK_PER_GROUP)) / (BLOCK_SIZE / 2);
        let count = (count as usize - (GPOUP_SIZE - DATA_BLOCK_PER_GROUP)) % (BLOCK_SIZE / 2);
        self.block_map[block].counts[count] -= 1;
    }
    pub fn sync<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        for (i, block) in self.block_map.iter_mut().enumerate() {
            block.sync(device, self.group_count * GPOUP_SIZE as u64 + 1 + i as u64)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BitmapBlock {
    pub bytes: [u8; BLOCK_SIZE],
}

impl Default for BitmapBlock {
    fn default() -> Self {
        Self {
            bytes: [0; BLOCK_SIZE],
        }
    }
}

impl Block for BitmapBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self { bytes }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        self.bytes
    }
}

impl BitmapBlock {
    /** Get if used */
    pub fn get_used(&self, count: u64) -> bool {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.bytes[byte] >> (7 - bit) << 7 != 0
    }
    /** Mark as used */
    pub fn set_used(&mut self, count: u64) {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.bytes[byte] |= 1 << (7 - bit);
    }
    /** Mark as unused */
    pub fn set_unused(&mut self, count: u64) {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.bytes[byte] &= !(1 << (7 - bit));
    }
    pub fn find_unused(&self) -> Option<u64> {
        for (i, byte) in self.bytes.iter().enumerate() {
            if *byte != 255 {
                for j in 0..8 {
                    let position = (i * 8 + j) as u64;
                    if !self.get_used(position) {
                        return Some(position);
                    }
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
pub struct BlockMapBlock {
    pub counts: [u16; BLOCK_SIZE / 2],
}

impl Default for BlockMapBlock {
    fn default() -> Self {
        Self {
            counts: [0; BLOCK_SIZE / 2],
        }
    }
}

impl Block for BlockMapBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut block = Self::default();

        for i in 0..BLOCK_SIZE / 2 {
            block.counts[i] = u16::from_be_bytes(bytes[2 * i..2 * i + 2].try_into().unwrap());
        }

        block
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        for i in 0..BLOCK_SIZE / 2 {
            bytes[2 * i..2 * i + 2].copy_from_slice(&self.counts[i].to_be_bytes());
        }

        bytes
    }
}

impl BlockMapBlock {
    pub fn get_count(&mut self, offset: usize) -> u16 {
        self.counts[offset]
    }
    pub fn set_count(&mut self, offset: usize, count: u16) {
        self.counts[offset] = count;
    }
}

#[derive(Debug)]
pub struct INodeBlock {
    pub inodes: [INode; BLOCK_SIZE / INODE_SIZE],
}

impl Default for INodeBlock {
    fn default() -> Self {
        Self {
            inodes: [INode::default(); BLOCK_SIZE / INODE_SIZE],
        }
    }
}

impl Block for INodeBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut block = Self::default();

        for i in 0..BLOCK_SIZE / INODE_SIZE {
            block.inodes[i] = INode::load(
                bytes[INODE_SIZE * i..INODE_SIZE * (i + 1)]
                    .try_into()
                    .unwrap(),
            );
        }

        block
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        for i in 0..BLOCK_SIZE / INODE_SIZE {
            bytes[INODE_SIZE * i..INODE_SIZE * (i + 1)].copy_from_slice(&self.inodes[i].dump());
        }

        bytes
    }
}
