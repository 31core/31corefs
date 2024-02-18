use std::time::{SystemTime, UNIX_EPOCH};

use crate::block::BLOCK_SIZE;

pub const INODE_SIZE: usize = 64;
pub const INODE_PER_GROUP: usize = BLOCK_SIZE / INODE_SIZE;

pub const ACL_DIRECTORY: u16 = 1 << 15;
pub const ACL_SYMBOLLINK: u16 = 1 << 14;
pub const ACL_FILE: u16 = 1 << 13;

#[derive(Default, Debug, Clone, Copy)]
/**
 * # Data structure
 *
 * Each Inode takes 64 bytes, the on-disk layout is:
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |2  |Permission |
 * |2    |3  |UID        |
 * |4    |6  |GID        |
 * |6    |14 |atime      |
 * |14   |22 |ctime      |
 * |22   |30 |mtime      |
 * |30   |32 |Hard links |
 * |32   |40 |Size       |
 * |40   |48 |B-Tree root|
 * |48   |64 |Reserved   |
 */
pub struct INode {
    pub permission: u16,
    pub uid: u16,
    pub gid: u16,
    pub atime: u64,
    pub ctime: u64,
    pub mtime: u64,
    pub hlinks: u16,
    pub size: u64,
    pub btree_root: u64,
}

impl INode {
    /** Load from bytes */
    pub fn load(bytes: [u8; INODE_SIZE]) -> Self {
        Self {
            permission: u16::from_be_bytes(bytes[..2].try_into().unwrap()),
            uid: u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
            gid: u16::from_be_bytes(bytes[4..6].try_into().unwrap()),
            atime: u64::from_be_bytes(bytes[6..14].try_into().unwrap()),
            ctime: u64::from_be_bytes(bytes[14..22].try_into().unwrap()),
            mtime: u64::from_be_bytes(bytes[22..30].try_into().unwrap()),
            hlinks: u16::from_be_bytes(bytes[30..32].try_into().unwrap()),
            size: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
            btree_root: u64::from_be_bytes(bytes[40..48].try_into().unwrap()),
        }
    }
    /** Dump to bytes */
    pub fn dump(&self) -> [u8; INODE_SIZE] {
        let mut inode_bytes = [0; INODE_SIZE];

        inode_bytes[..2].copy_from_slice(&self.permission.to_be_bytes());
        inode_bytes[2..4].copy_from_slice(&self.uid.to_be_bytes());
        inode_bytes[4..6].copy_from_slice(&self.gid.to_be_bytes());
        inode_bytes[6..14].copy_from_slice(&self.atime.to_be_bytes());
        inode_bytes[14..22].copy_from_slice(&self.ctime.to_be_bytes());
        inode_bytes[22..30].copy_from_slice(&self.mtime.to_be_bytes());
        inode_bytes[30..32].copy_from_slice(&self.hlinks.to_be_bytes());
        inode_bytes[32..40].copy_from_slice(&self.size.to_be_bytes());
        inode_bytes[40..48].copy_from_slice(&self.btree_root.to_be_bytes());

        inode_bytes
    }
    pub fn is_dir(&self) -> bool {
        (self.permission & ACL_DIRECTORY) != 0
    }
    pub fn is_symlink(&self) -> bool {
        (self.permission & ACL_SYMBOLLINK) != 0
    }
    pub fn is_file(&self) -> bool {
        (self.permission & ACL_FILE) != 0
    }
    pub fn is_empty_inode(&self) -> bool {
        !(self.is_dir() | self.is_symlink() | self.is_file())
    }
    pub fn update_atime(&mut self) {
        self.atime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    pub fn update_ctime(&mut self) {
        self.ctime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    pub fn update_mtime(&mut self) {
        self.update_ctime();
        self.mtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}
