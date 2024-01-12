use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, Write};

use crate::block::*;
use crate::btree::*;
use crate::inode::{INode, INODE_PER_GROUP};
use crate::Filesystem;

const SUBVOLUMES: usize = BLOCK_SIZE / SUBVOLUME_ENTRY_SIZE - 1;
const SUBVOLUME_ENTRY_SIZE: usize = 64;

#[derive(Default, Debug, Clone, Copy)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |ID         |
 * |8    |16 |Inode B-Tree|
 * |16   |24 |Inode allocator|
 * |24   |32 |Root Inode |
 * |32   |33 |Inode B-Tree depth|
 */
pub struct SubvolumeEntry {
    pub id: u64,
    pub inode_tree_root: u64,
    pub inode_tree_depth: u8,
    pub inode_alloc_block: u64,
    pub root_inode: u64,
}

impl SubvolumeEntry {
    pub fn load(bytes: &[u8]) -> Self {
        Self {
            id: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            inode_tree_root: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            inode_alloc_block: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            root_inode: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            inode_tree_depth: bytes[24],
        }
    }
    pub fn dump(&self) -> [u8; SUBVOLUME_ENTRY_SIZE] {
        let mut bytes = [0; SUBVOLUME_ENTRY_SIZE];

        bytes[0..8].copy_from_slice(&self.id.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.inode_tree_root.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.inode_alloc_block.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.root_inode.to_be_bytes());
        bytes[32] = self.inode_tree_depth;

        bytes
    }
}

#[derive(Debug, Default, Clone)]
pub struct SubvolumeManager {
    pub entries: Vec<SubvolumeEntry>,

    pub next: u64,
}

impl Block for SubvolumeManager {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut mgr = Self {
            next: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            ..Default::default()
        };

        let entries = u64::from_be_bytes(bytes[8..16].try_into().unwrap()) as usize;

        for i in 1..entries + 1 {
            let entry = SubvolumeEntry::load(&bytes[SUBVOLUME_ENTRY_SIZE * i..]);
            mgr.entries.push(entry);
        }
        mgr
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..8].copy_from_slice(&self.next.to_be_bytes());
        bytes[8..16].copy_from_slice(&(self.entries.len() as u64).to_be_bytes());

        for i in 1..self.entries.len() + 1 {
            bytes[SUBVOLUME_ENTRY_SIZE * i..SUBVOLUME_ENTRY_SIZE * (i + 1)]
                .copy_from_slice(&self.entries[i - 1].dump());
        }

        bytes
    }
}

impl SubvolumeManager {
    /** Generate ID if a new subvolume */
    fn generate_new_id<D>(fs: &Filesystem, device: &mut D, mut mgr_block_count: u64) -> u64
    where
        D: Write + Read + Seek,
    {
        loop {
            let mgr = Self::load(fs.get_data_block(device, mgr_block_count).unwrap());

            if mgr.next == 0 {
                return match mgr.entries.last() {
                    Some(subvol) => subvol.id + 1,
                    None => 0,
                };
            } else {
                mgr_block_count = mgr.next;
            }
        }
    }
    fn get_subvol_internal<D>(
        &self,
        fs: &Filesystem,
        device: &mut D,
        id: u64,
    ) -> IOResult<Subvolume>
    where
        D: Write + Read + Seek,
    {
        for entry in &self.entries {
            if entry.id == id {
                return Ok(Subvolume {
                    entry: *entry,
                    btree: if entry.inode_tree_depth == 0 {
                        BtreeNode::new(
                            entry.inode_tree_root,
                            BtreeType::Leaf,
                            &fs.get_data_block(device, entry.inode_tree_root)?,
                        )
                    } else {
                        BtreeNode::new(
                            entry.inode_tree_root,
                            BtreeType::Internal,
                            &fs.get_data_block(device, entry.inode_tree_root)?,
                        )
                    },
                });
            }
        }
        Err(Error::new(
            ErrorKind::NotFound,
            format!("No such subvolume '{id}'"),
        ))
    }
    /** Get a subvolume */
    pub fn get_subvolume<D>(
        fs: &Filesystem,
        device: &mut D,
        mut mgr_block_count: u64,
        id: u64,
    ) -> IOResult<Subvolume>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mgr = Self::load(fs.get_data_block(device, mgr_block_count)?);

            match mgr.get_subvol_internal(fs, device, id) {
                Ok(subvol) => return Ok(subvol),
                Err(err) => {
                    if mgr.next != 0 {
                        mgr_block_count = mgr.next;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }
    /** Set subvolume sntry */
    pub fn set_subvolume<D>(
        fs: &Filesystem,
        device: &mut D,
        mut mgr_block_count: u64,
        id: u64,
        entry: SubvolumeEntry,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut mgr = Self::load(fs.get_data_block(device, mgr_block_count)?);

            for (i, this_entry) in mgr.entries.iter().enumerate() {
                if this_entry.id == id {
                    mgr.entries[i] = entry;
                    mgr.sync(device, mgr_block_count)?;
                    return Ok(());
                }
            }

            if mgr.next != 0 {
                mgr_block_count = mgr.next;
            } else {
                return Ok(());
            }
        }
    }
    /** Create a new subvolume */
    pub fn new_subvolume<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut mgr_block_count: u64,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut mgr = Self::load(fs.get_data_block(device, mgr_block_count)?);
            if mgr.next == 0 {
                if mgr.entries.len() < SUBVOLUMES {
                    let entry = SubvolumeEntry {
                        id: Self::generate_new_id(fs, device, mgr_block_count),
                        inode_tree_root: BtreeNode::allocate_on_block(fs, device)?,
                        inode_alloc_block: AvailableInodeManager::allocate_on_block(fs, device)?,
                        ..Default::default()
                    };
                    let subvol_id = entry.id;
                    mgr.entries.push(entry);
                    mgr.sync(device, mgr_block_count)?;

                    let mut subvol = Self::get_subvolume(fs, device, mgr_block_count, subvol_id)?;
                    crate::dir::create(fs, &mut subvol, device)?;
                    return Ok(subvol_id);
                } else {
                    let new_mgr_id = fs.new_block()?;
                    mgr.next = new_mgr_id;
                    mgr.sync(device, mgr_block_count)?;
                    mgr_block_count = new_mgr_id;
                }
            }
        }
    }
    /** Remove a subvolume */
    pub fn remove_subvolume<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut mgr_block_count: u64,
        id: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut mgr = Self::load(fs.get_data_block(device, mgr_block_count)?);

            for (i, subvol) in mgr.entries.iter().enumerate() {
                if subvol.id == id {
                    /* destroy the inode tree */
                    let mut root = crate::btree::BtreeNode::load(
                        fs.get_data_block(device, subvol.inode_tree_root)?,
                    );
                    root.destroy(fs, device, subvol.inode_tree_depth as usize)?;

                    AvailableInodeManager::destroy_blocks(fs, device, subvol.inode_alloc_block)?;

                    mgr.entries.remove(i);
                    mgr.sync(device, mgr_block_count)?;
                    return Ok(());
                }
            }

            if mgr.next == 0 {
                return Ok(());
            } else {
                mgr_block_count = mgr.next;
            }
        }
    }
    /** Create a snapshot */
    pub fn create_snapshot<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mgr_block_count: u64,
        id: u64,
    ) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let subvol_id = Self::new_subvolume(fs, device, mgr_block_count)?;
        let mut subvol = Self::get_subvolume(fs, device, mgr_block_count, id)?;

        subvol.entry.id = subvol_id;
        Self::set_subvolume(fs, device, mgr_block_count, subvol_id, subvol.entry)?;

        subvol
            .btree
            .clone_tree(fs, device, subvol.entry.inode_tree_depth as usize)?; // clone inode tree
        AvailableInodeManager::clone_blocks(fs, device, subvol.entry.inode_alloc_block)?;
        Ok(subvol_id)
    }
}

const AVAILABLE_INODE_MANAGER_LEN: usize = BLOCK_SIZE / 8 - 3;

#[derive(Debug, Default)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |Pointer of the next block|
 * |8    |16 |Reference count|
 * |16   |24 |Count of Inodes|
 * |8*(N+3)|8*(N+4)|Inode counts|
 */
pub struct AvailableInodeManager {
    pub next: u64,
    pub rc: u64,
    pub inodes: Vec<u64>,
}

impl Block for AvailableInodeManager {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut mgr = Self {
            next: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            rc: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),

            ..Default::default()
        };

        let inodes = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        for i in 0..inodes as usize {
            mgr.inodes.push(u64::from_be_bytes(
                bytes[8 * (i + 3)..8 * (i + 4)].try_into().unwrap(),
            ));
        }

        mgr
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..8].copy_from_slice(&self.next.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.rc.to_be_bytes());
        bytes[16..24].copy_from_slice(&(self.inodes.len() as u64).to_be_bytes());
        for (i, inode) in self.inodes.iter().enumerate() {
            bytes[8 * (i + 3)..8 * (i + 4)].copy_from_slice(&inode.to_be_bytes());
        }

        bytes
    }
}

impl AvailableInodeManager {
    /** Returns an available inode */
    pub fn get_available_inode(&self) -> Option<u64> {
        self.inodes.last().copied()
    }
    /** Recursively allocate an inode */
    pub fn allocate_inode<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        loop {
            let allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);
            if let Some(inode_count) = allocator.get_available_inode() {
                return Ok(inode_count);
            } else if allocator.next != 0 {
                allocator_count = allocator.next;
            } else {
                return Err(Error::new(ErrorKind::Other, "No available inode"));
            }
        }
    }
    /** Recursively insert an inode */
    pub fn insert_inode<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            /* this block is full */
            if allocator.inodes.len() == AVAILABLE_INODE_MANAGER_LEN {
                let new_allocator_count = Self::allocate_on_block(fs, device)?;
                allocator.next = new_allocator_count;
                allocator.sync(device, allocator_count)?;
                allocator_count = new_allocator_count;
            } else {
                if allocator.rc > 0 {
                    allocator.rc -= 1;
                    allocator.sync(device, allocator_count)?;
                    allocator_count = fs.new_block()?;
                    allocator.rc = 0;
                }
                allocator.inodes.push(inode);
                allocator.sync(device, allocator_count)?;
                return Ok(());
            }
        }
    }
    fn modify_next_pointer<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        pointer: u64,
        allocator_chain: &[u64],
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut allocator =
            AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);
        allocator.next = pointer;

        if allocator.rc > 0 {
            allocator.rc -= 1;
            allocator.sync(device, allocator_count)?;
            allocator_count = fs.block_copy_out(device, allocator_count)?;
            allocator.rc = 0;

            if !allocator_chain.is_empty() {
                Self::modify_next_pointer(
                    fs,
                    device,
                    *allocator_chain.last().unwrap(),
                    allocator_count,
                    &allocator_chain[..allocator_chain.len() - 1],
                )?;
            }
        }
        allocator.sync(device, allocator_count)?;

        Ok(())
    }
    /** Recursively remove an inode */
    pub fn remove_inode<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut allocator_chain = Vec::new();
        loop {
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);
            for (i, this_inode) in allocator.inodes.iter().enumerate() {
                if *this_inode == inode {
                    allocator.inodes.remove(i);

                    /* when this allocator is empty, then release this block */
                    if allocator.inodes.is_empty() {
                        /* modify the pointer of last allocator */
                        if !allocator_chain.is_empty() {
                            Self::modify_next_pointer(
                                fs,
                                device,
                                *allocator_chain.last().unwrap(),
                                allocator_count,
                                &allocator_chain[..allocator_chain.len() - 1],
                            )?;
                        }
                        fs.release_block(allocator_count);
                    } else {
                        if allocator.rc > 0 {
                            allocator.rc -= 1;
                            allocator.sync(device, allocator_count)?;
                            allocator_count = fs.block_copy_out(device, allocator_count)?;
                            allocator.rc = 0;

                            if !allocator_chain.is_empty() {
                                Self::modify_next_pointer(
                                    fs,
                                    device,
                                    *allocator_chain.last().unwrap(),
                                    allocator_count,
                                    &allocator_chain,
                                )?;
                            }
                        }
                        allocator.sync(device, allocator_count)?;
                    }
                    return Ok(());
                }
            }
            allocator_chain.push(allocator_count);
            allocator_count = allocator.next;
        }
    }
    /** Recursively clone blocks */
    pub fn clone_blocks<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);
            fs.clone_block(allocator_count);

            if allocator.next == 0 {
                return Ok(());
            } else {
                allocator_count = allocator.next;
            }
        }
    }
    /** Recursively destroy blocks */
    pub fn destroy_blocks<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            if allocator.rc > 0 {
                allocator.rc -= 1;
                allocator.sync(device, allocator_count)?;
            } else {
                fs.release_block(allocator_count);
            }

            if allocator.next == 0 {
                return Ok(());
            } else {
                allocator_count = allocator.next;
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct Subvolume {
    pub entry: SubvolumeEntry,
    pub btree: BtreeNode,
}

impl Subvolume {
    pub fn create<D>(fs: &mut Filesystem, device: &mut D) -> IOResult<Self>
    where
        D: Write + Read + Seek,
    {
        let mut subvol = Self::default();

        subvol.entry.inode_tree_root = BtreeNode::allocate_on_block(fs, device)?;
        subvol.entry.inode_alloc_block = AvailableInodeManager::allocate_on_block(fs, device)?;
        subvol.btree.block_count = subvol.entry.inode_tree_root;

        Ok(subvol)
    }
    pub fn new_inode<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        if let Ok(inode_count) =
            AvailableInodeManager::allocate_inode(fs, device, self.entry.inode_alloc_block)
        {
            AvailableInodeManager::remove_inode(
                fs,
                device,
                self.entry.inode_alloc_block,
                inode_count,
            )?;
            Ok(inode_count)
        } else {
            let inode_group_block = INodeGroup::allocate_on_block(fs, device)?;
            let inode_group_count =
                self.btree
                    .find_unused(fs, device, self.entry.inode_tree_depth as usize)?;
            self.entry.inode_tree_depth = self.btree.insert(
                fs,
                device,
                inode_group_count,
                inode_group_block,
                self.entry.inode_tree_depth as usize,
            )? as u8;
            self.entry.inode_tree_root = self.btree.block_count;
            SubvolumeManager::set_subvolume(
                fs,
                device,
                fs.sb.subvol_mgr,
                self.entry.id,
                self.entry,
            )?;

            for i in 1..INODE_PER_GROUP as u64 {
                AvailableInodeManager::insert_inode(
                    fs,
                    device,
                    self.entry.inode_alloc_block,
                    inode_group_count * INODE_PER_GROUP as u64 + i,
                )?;
            }

            Ok(inode_group_count * INODE_PER_GROUP as u64)
        }
    }
    pub fn get_inode<D>(&self, fs: &mut Filesystem, device: &mut D, inode: u64) -> IOResult<INode>
    where
        D: Read + Write + Seek,
    {
        let inode_group_count = inode / INODE_PER_GROUP as u64;
        let inode_num = inode as usize % INODE_PER_GROUP;
        let inode_group_block = self
            .btree
            .lookup(
                fs,
                device,
                inode_group_count,
                self.entry.inode_tree_depth as usize,
            )?
            .value;
        let inode_block = fs.get_data_block(device, inode_group_block)?;
        let inode_group = INodeGroup::load(inode_block);
        Ok(inode_group.inodes[inode_num])
    }
    pub fn set_inode<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        count: u64,
        inode: INode,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let inode_group_count = count / INODE_PER_GROUP as u64;
        let inode_num = count as usize % INODE_PER_GROUP;

        let btree_query_result = self.btree.lookup(
            fs,
            device,
            inode_group_count,
            self.entry.inode_tree_depth as usize,
        )?;
        let inode_group_block = btree_query_result.value;

        let mut inode_group = INodeGroup::load(fs.get_data_block(device, inode_group_block)?);
        inode_group.inodes[inode_num] = inode;
        if btree_query_result.rc > 0 {
            let new_inode_group_block = fs.new_block()?;
            self.btree.modify(
                fs,
                device,
                inode_group_count,
                new_inode_group_block,
                self.entry.inode_tree_depth as usize,
            )?;
            self.entry.inode_tree_root = self.btree.block_count;
            SubvolumeManager::set_subvolume(
                fs,
                device,
                fs.sb.subvol_mgr,
                self.entry.id,
                self.entry,
            )?;

            inode_group.sync(device, new_inode_group_block)?;
            for (i, inode) in inode_group.inodes.iter().enumerate() {
                if !inode.is_empty_inode() {
                    crate::file::clone_by_inode(
                        fs,
                        self,
                        device,
                        inode_group_count * INODE_PER_GROUP as u64 + i as u64,
                    )?;
                }
            }
        } else {
            inode_group.sync(device, inode_group_block)?;
        }
        Ok(())
    }
    /** Release an inode */
    pub fn release_inode<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.set_inode(fs, device, inode, INode::default())?;
        AvailableInodeManager::insert_inode(fs, device, self.entry.inode_alloc_block, inode)?;
        Ok(())
    }
}
