use crate::inode::*;
use std::io::{Read, Result as IOResult, Seek, SeekFrom, Write};

pub const GPOUP_SIZE: usize =
    INODE_BITMAP_SIZE + BLOCK_MAP_SIZE + INODE_TABLE_SIZE + DATA_BLOCK_PER_GROUP;

pub const BLOCK_SIZE: usize = 4096;
pub const INODE_PER_GROUP: usize = BLOCK_MAP_SIZE * BLOCK_SIZE / 2;
pub const DATA_BLOCK_PER_GROUP: usize = 8 * BLOCK_SIZE;
pub const INODE_BITMAP_SIZE: usize = 1;
pub const INODE_TABLE_SIZE: usize = 8 * BLOCK_SIZE / (BLOCK_SIZE / INODE_SIZE);
pub const BLOCK_MAP_SIZE: usize = 32;

#[macro_export]
macro_rules! relative_to_absolute {
    ($group_count: expr, $count: expr) => {
        1 + $group_count * GPOUP_SIZE as u64
            + (INODE_BITMAP_SIZE + BLOCK_MAP_SIZE + INODE_TABLE_SIZE) as u64
            + $count
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

pub trait Block {
    /** Load from bytes */
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self;
    /** Dump to bytes */
    fn dump(&self) -> [u8; BLOCK_SIZE];
    fn sync<D>(&mut self, block_count: u64, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        device.seek(SeekFrom::Start(block_count * BLOCK_SIZE as u64))?;
        device.write_all(&self.dump())?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
/**
 * Super block
 *
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |4  |Magic header|
 * |4    |5  |Version    |
 * |5    |13 |Count of groups|
 * |13   |29 |UUID       |
 * |29   |285|Label      |
*/
pub struct SuperBlock {
    pub groups: u64,
    pub root_inode: u64,
    pub uuid: [u8; 16],
    pub label: [u8; 256],
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            groups: 0,
            root_inode: 0,
            uuid: [0; 16],
            label: [0; 256],
        }
    }
}

impl Block for SuperBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            groups: u64::from_be_bytes(bytes[5..13].try_into().unwrap()),
            root_inode: u64::from_be_bytes(bytes[13..21].try_into().unwrap()),
            uuid: bytes[21..37].try_into().unwrap(),
            label: bytes[37..293].try_into().unwrap(),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..4].copy_from_slice(&crate::FS_MAGIC_HEADER);
        bytes[4] = crate::FS_VERSION;
        bytes[5..13].copy_from_slice(&self.groups.to_be_bytes());
        bytes[13..21].copy_from_slice(&self.root_inode.to_be_bytes());
        bytes[21..37].copy_from_slice(&self.uuid);
        bytes[37..293].copy_from_slice(&self.label);

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
    pub inode_bitmap: BitmapBlock,
    pub block_map: [BlockMapBlock; BLOCK_MAP_SIZE],
}

impl BlockGroup {
    pub fn load<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.inode_bitmap = BitmapBlock::load(load_block(
            device,
            GPOUP_SIZE as u64 * self.group_count + 1,
        )?);

        for i in 0..BLOCK_MAP_SIZE as u64 {
            self.block_map[i as usize] = BlockMapBlock::load(load_block(
                device,
                GPOUP_SIZE as u64 * self.group_count + 2 + i,
            )?);
        }

        Ok(())
    }
    /** Allocate an inode */
    pub fn new_inode<D>(&mut self, device: &mut D) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let inode = self.inode_bitmap.find_unused();
        if let Some(inode) = inode {
            /* clean up the inode */
            self.inode_bitmap.set_used(inode);
            let block_count =
                relative_to_absolute!(self.group_count, inode / (BLOCK_SIZE / INODE_SIZE) as u64)
                    - INODE_TABLE_SIZE as u64;
            let block = load_block(device, block_count)?;
            let mut inode_table_block = INodeBlock::load(block);

            inode_table_block.inodes[(inode % (BLOCK_SIZE / INODE_SIZE) as u64) as usize] =
                INode::default();
            inode_table_block.sync(block_count, device)?;
            Ok(inode)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::Other, ""))
        }
    }
    /** Get an inode */
    pub fn get_inode<D>(&self, device: &mut D, inode: u64) -> IOResult<INode>
    where
        D: Read + Write + Seek,
    {
        let block_count =
            relative_to_absolute!(self.group_count, inode / (BLOCK_SIZE / INODE_SIZE) as u64)
                - INODE_TABLE_SIZE as u64;
        let block = load_block(device, block_count)?;
        let inodes = INodeBlock::load(block);

        Ok(inodes.inodes[(inode % (BLOCK_SIZE / INODE_SIZE) as u64) as usize])
    }
    /** Write an inode */
    pub fn set_inode<D>(&self, device: &mut D, inode_count: u64, inode: INode) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let block_count = relative_to_absolute!(
            self.group_count,
            inode_count / (BLOCK_SIZE / INODE_SIZE) as u64
        ) - INODE_TABLE_SIZE as u64;
        let block = load_block(device, block_count)?;
        let mut inode_table_block = INodeBlock::load(block);

        inode_table_block.inodes[(inode_count % (BLOCK_SIZE / INODE_SIZE) as u64) as usize] = inode;
        inode_table_block.sync(block_count, device)?;

        Ok(())
    }
    /** Release an inode */
    pub fn release_inode(&mut self, inode: u64) {
        self.inode_bitmap.set_unused(inode);
    }
    /** Allocate a data block */
    pub fn new_block(&mut self) -> Option<u64> {
        for count in 0..DATA_BLOCK_PER_GROUP as u64 {
            if self.block_map[count as usize / (BLOCK_MAP_SIZE / 2)].counts
                [count as usize % (BLOCK_MAP_SIZE / 2)]
                == 0
            {
                self.block_map[count as usize / (BLOCK_MAP_SIZE / 2)].counts
                    [count as usize % (BLOCK_MAP_SIZE / 2)] += 1;
                return Some(count);
            }
        }
        None
    }
    /** Release a data block */
    pub fn release_block(&mut self, count: u64) {
        self.block_map[count as usize / (BLOCK_MAP_SIZE / 2)].counts
            [count as usize % (BLOCK_MAP_SIZE / 2)] -= 1;
    }
    pub fn sync<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.inode_bitmap
            .sync(self.group_count * GPOUP_SIZE as u64 + 1, device)?;

        for (i, block) in self.block_map.iter_mut().enumerate() {
            block.sync(self.group_count * GPOUP_SIZE as u64 + 2 + i as u64, device)?;
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
