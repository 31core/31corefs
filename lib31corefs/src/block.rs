use crate::inode::{INode, INODE_PER_GROUP, INODE_SIZE};
use crate::subvol::Subvolume;
use crate::Filesystem;

use std::fmt::Debug;
use std::io::Result as IOResult;
use std::io::{Read, Seek, SeekFrom, Write};

pub const BLOCK_SIZE: usize = 4096;

const BLOCK_MAP_SIZE: usize = 1;
const LABEL_MAX_LEN: usize = 256;

/** Copy out a mutiple referenced data block */
pub fn block_copy_out<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    count: u64,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let block = load_block(device, count)?;
    let new_block = subvol.new_block(fs, device)?;
    save_block(device, new_block, block)?;
    Ok(new_block)
}

pub(crate) fn load_block<D>(device: &mut D, block_count: u64) -> IOResult<[u8; BLOCK_SIZE]>
where
    D: Read + Write + Seek,
{
    let mut block = [0; BLOCK_SIZE];
    device.seek(SeekFrom::Start(block_count * BLOCK_SIZE as u64))?;
    device.read_exact(&mut block)?;

    Ok(block)
}

/** Store data block */
pub(crate) fn save_block<D>(
    device: &mut D,
    block_count: u64,
    block: [u8; BLOCK_SIZE],
) -> IOResult<[u8; BLOCK_SIZE]>
where
    D: Read + Write + Seek,
{
    device.seek(SeekFrom::Start(block_count * BLOCK_SIZE as u64))?;
    device.write_all(&block)?;

    Ok(block)
}

pub trait Block: Default + Debug {
    /** Load from bytes */
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self;
    /** Dump to bytes */
    fn dump(&self) -> [u8; BLOCK_SIZE];
    /** Load from device */
    fn load_block<D>(device: &mut D, block_count: u64) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        Ok(Self::load(load_block(device, block_count)?))
    }
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
    fn allocate_on_block<D>(fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let block_count = fs.new_block()?;
        Self::default().sync(device, block_count)?;
        Ok(block_count)
    }
    /** Allocate and initialize an empty block on device, also managed by subvolume bitmap */
    fn allocate_on_block_subvol<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let block_count = subvol.new_block(fs, device)?;
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
 * |325  |333|Filesystem created time|
*/
pub struct SuperBlock {
    pub groups: u64,
    pub uuid: [u8; 16],
    pub label: [u8; LABEL_MAX_LEN],
    pub total_blocks: u64,
    pub used_blocks: u64,
    pub real_used_blocks: u64,
    pub default_subvol: u64,
    pub subvol_mgr: u64,
    pub creation_time: u64,
}

impl Default for SuperBlock {
    fn default() -> Self {
        Self {
            groups: 0,
            uuid: [0; 16],
            label: [0; LABEL_MAX_LEN],
            total_blocks: 0,
            used_blocks: 0,
            real_used_blocks: 0,
            subvol_mgr: 0,
            default_subvol: 0,
            creation_time: 0,
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
            creation_time: u64::from_be_bytes(bytes[325..333].try_into().unwrap()),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[..4].copy_from_slice(&crate::FS_MAGIC_HEADER);
        bytes[4] = crate::FS_VERSION;
        bytes[5..13].copy_from_slice(&self.groups.to_be_bytes());
        bytes[13..29].copy_from_slice(&self.uuid);
        bytes[29..285].copy_from_slice(&self.label);
        bytes[285..293].copy_from_slice(&self.total_blocks.to_be_bytes());
        bytes[293..301].copy_from_slice(&self.used_blocks.to_be_bytes());
        bytes[301..309].copy_from_slice(&self.real_used_blocks.to_be_bytes());
        bytes[309..317].copy_from_slice(&self.subvol_mgr.to_be_bytes());
        bytes[317..325].copy_from_slice(&self.default_subvol.to_be_bytes());
        bytes[325..333].copy_from_slice(&self.creation_time.to_be_bytes());

        bytes
    }
}

impl SuperBlock {
    /** Set filesystem label */
    pub fn set_label(&mut self, label: &str) {
        self.label = [0; LABEL_MAX_LEN];
        self.label[..label.len()].copy_from_slice(label.as_bytes());
    }
    /** Get filesystem label */
    pub fn get_label(&self) -> String {
        let mut null_idx = self.label.len();

        for (i, byte) in self.label.iter().enumerate() {
            if *byte == 0 {
                null_idx = i;
                break;
            }
        }

        String::from_utf8_lossy(&self.label[..null_idx]).to_string()
    }
    pub(crate) fn is_valid(bytes: &[u8]) -> bool {
        /* check magic header */
        for (i, byte) in crate::FS_MAGIC_HEADER.iter().enumerate() {
            if *byte != bytes[i] {
                return false;
            }
        }

        /* check fs version */
        bytes[4] == crate::FS_VERSION
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockGroupMeta {
    pub id: u64,
    pub free_blocks: u64,
    pub next_group: u64,
}

impl Block for BlockGroupMeta {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            id: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            free_blocks: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            next_group: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];
        block[..8].copy_from_slice(&self.id.to_be_bytes());
        block[8..16].copy_from_slice(&self.free_blocks.to_be_bytes());
        block[16..24].copy_from_slice(&self.next_group.to_be_bytes());

        block
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockGroup {
    pub meta_data: BlockGroupMeta,
    pub start_block: u64,
    pub block_map: BitmapBlock,
}

impl BlockGroup {
    pub fn create(group_start: u64, totol_blocks: u64) -> Self {
        let mut group = BlockGroup {
            start_block: group_start,
            ..Default::default()
        };

        if totol_blocks <= group.blocks() {
            group.meta_data.next_group = 0;
            const META_BLOCK: u64 = 1;
            group.meta_data.free_blocks = totol_blocks - META_BLOCK - BLOCK_MAP_SIZE as u64;
        } else {
            group.meta_data.next_group = group_start + group.blocks();
            group.meta_data.free_blocks = 8 * BLOCK_SIZE as u64;
        }

        group
    }
    pub fn load<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.meta_data = BlockGroupMeta::load_block(device, self.start_block)?;
        self.block_map = BitmapBlock::load_block(device, self.start_block + 1)?;

        Ok(())
    }
    /** Allocate a data block */
    pub fn new_block(&mut self) -> Option<u64> {
        if self.meta_data.free_blocks > 0 {
            if let Some(off) = self.block_map.find_unused() {
                self.block_map.set_used(off);
                self.meta_data.free_blocks -= 1;
                return Some(off);
            }
        }
        None
    }
    /** Clone a data block */
    pub fn clone_block(&mut self, count: u64) {
        self.block_map.get_used(count);
    }
    /** Release a data block */
    pub fn release_block(&mut self, count: u64) {
        self.block_map.set_unused(count);
        self.meta_data.free_blocks += 1;
    }
    pub fn sync<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.meta_data.sync(device, self.start_block)?;
        self.block_map.sync(device, self.start_block + 1)?;

        Ok(())
    }
    #[inline]
    pub(crate) fn blocks(&self) -> u64 {
        const META_BLOCK: u64 = 1;
        META_BLOCK + BLOCK_MAP_SIZE as u64 + 8 * BLOCK_SIZE as u64
    }
    #[inline]
    pub(crate) fn to_relative_block(&self, absolute_block: u64) -> u64 {
        const META_BLOCK: u64 = 1;
        absolute_block - self.start_block - META_BLOCK - BLOCK_MAP_SIZE as u64
    }
    #[inline]
    pub(crate) fn to_absolute_block(&self, relative_block: u64) -> u64 {
        const META_BLOCK: u64 = 1;
        self.start_block + META_BLOCK + BLOCK_MAP_SIZE as u64 + relative_block
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
        self.bytes[byte] & (1 << (7 - bit)) != 0
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
    /**
     * Find an unmarked bit and return its position.
     */
    pub fn find_unused(&self) -> Option<u64> {
        for (i, byte) in self.bytes.iter().enumerate() {
            if *byte != 0xff {
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

#[derive(Debug)]
pub struct BitmapIndexBlock {
    pub next: u64,
    pub bitmaps: [u64; BLOCK_SIZE / 8 - 1],
}

impl Block for BitmapIndexBlock {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut block = Self {
            next: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            ..Default::default()
        };

        let bitmaps = &bytes[8..];
        for (i, block) in block.bitmaps.iter_mut().enumerate() {
            *block = u64::from_be_bytes(bitmaps[8 * i..8 * (i + 1)].try_into().unwrap());
        }

        block
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[..8].copy_from_slice(&self.next.to_be_bytes());
        let bitmaps = &mut bytes[8..];
        for (i, block) in self.bitmaps.iter().enumerate() {
            bitmaps[8 * i..8 * (i + 1)].copy_from_slice(&block.to_be_bytes());
        }

        bytes
    }
}

impl Default for BitmapIndexBlock {
    fn default() -> Self {
        Self {
            bitmaps: [0; BLOCK_SIZE / 8 - 1],
            next: 0,
        }
    }
}

#[derive(Debug)]
pub struct INodeGroup {
    pub inodes: [INode; INODE_PER_GROUP],
}

impl Default for INodeGroup {
    fn default() -> Self {
        Self {
            inodes: [INode::empty(); INODE_PER_GROUP],
        }
    }
}

impl Block for INodeGroup {
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        for (i, inode) in self.inodes.iter().enumerate() {
            bytes[i * INODE_SIZE..(i + 1) * INODE_SIZE].copy_from_slice(&inode.dump());
        }

        bytes
    }
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut block = Self::default();

        for i in 0..INODE_PER_GROUP {
            block.inodes[i] = INode::load(
                bytes[i * INODE_SIZE..(i + 1) * INODE_SIZE]
                    .try_into()
                    .unwrap(),
            );
        }

        block
    }
}

impl INodeGroup {
    pub fn is_empty(&self) -> bool {
        for i in self.inodes {
            if !i.is_empty_inode() {
                return false;
            }
        }
        true
    }
    pub fn is_full(&self) -> bool {
        for i in self.inodes {
            if i.is_empty_inode() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct LinkedContentTable {
    pub next: u64,
    pub content: [u8; BLOCK_SIZE - 8],
}

impl Default for LinkedContentTable {
    fn default() -> Self {
        Self {
            next: 0,
            content: [0; BLOCK_SIZE - 8],
        }
    }
}

impl Block for LinkedContentTable {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            next: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            content: bytes[8..].try_into().unwrap(),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];

        block[..8].copy_from_slice(&self.next.to_be_bytes());
        block[8..].copy_from_slice(&self.content);

        block
    }
}
