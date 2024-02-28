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
 */
pub struct SubvolumeEntry {
    pub id: u64,
    pub inode_tree_root: u64,
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
        }
    }
    pub fn dump(&self) -> [u8; SUBVOLUME_ENTRY_SIZE] {
        let mut bytes = [0; SUBVOLUME_ENTRY_SIZE];

        bytes[0..8].copy_from_slice(&self.id.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.inode_tree_root.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.inode_alloc_block.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.root_inode.to_be_bytes());

        bytes
    }
}

#[derive(Debug, Default, Clone)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |Next pointer|
 * |8    |16 |Count of entries|
 * |16   |4096|Entries   |
*/
pub struct SubvolumeManager {
    pub next: u64,
    pub entries: Vec<SubvolumeEntry>,
}

impl Block for SubvolumeManager {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut mgr = Self {
            next: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            ..Default::default()
        };

        let entries = u64::from_be_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let entries_content = &bytes[SUBVOLUME_ENTRY_SIZE..];

        for i in 0..entries {
            let entry = SubvolumeEntry::load(&entries_content[SUBVOLUME_ENTRY_SIZE * i..]);
            mgr.entries.push(entry);
        }
        mgr
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..8].copy_from_slice(&self.next.to_be_bytes());
        bytes[8..16].copy_from_slice(&(self.entries.len() as u64).to_be_bytes());

        let entries_content = &mut bytes[SUBVOLUME_ENTRY_SIZE..];
        for (i, entry) in self.entries.iter().enumerate() {
            entries_content[SUBVOLUME_ENTRY_SIZE * i..SUBVOLUME_ENTRY_SIZE * (i + 1)]
                .copy_from_slice(&entry.dump());
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
                    igroup_mgt_btree: BtreeNode::new(
                        entry.inode_tree_root,
                        BtreeType::Leaf,
                        &fs.get_data_block(device, entry.inode_tree_root)?,
                    ),
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
                    root.destroy(fs, device)?;

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

        subvol.igroup_mgt_btree.clone_tree(fs, device)?; // clone inode tree
        AvailableInodeManager::clone_blocks(fs, device, subvol.entry.inode_alloc_block)?;
        Ok(subvol_id)
    }
}

#[derive(Debug)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |Pointer of the next block|
 * |8    |16 |Reference count|
 * |8*(N+2)|8*(N+2)|Inode group bitmap|
 */
pub struct AvailableInodeManager {
    pub next: u64,
    pub rc: u64,
    pub bitmap_data: [u8; BLOCK_SIZE - 16],
}

impl Default for AvailableInodeManager {
    fn default() -> Self {
        Self {
            next: 0,
            rc: 0,
            bitmap_data: [0; BLOCK_SIZE - 16],
        }
    }
}

impl Block for AvailableInodeManager {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            next: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            rc: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            bitmap_data: bytes[16..].try_into().unwrap(),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[0..8].copy_from_slice(&self.next.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.rc.to_be_bytes());
        bytes[16..].copy_from_slice(&self.bitmap_data);

        bytes
    }
}

impl AvailableInodeManager {
    /** Get if a inode group is vailable */
    pub fn get_available<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        count: u64,
    ) -> IOResult<bool>
    where
        D: Write + Read + Seek,
    {
        let mut byte = count as usize / 8;
        let bit = count as usize % 8;
        loop {
            let allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            if byte < allocator.bitmap_data.len() {
                return Ok(allocator.bitmap_data[byte] >> (7 - bit) << 7 != 0);
            } else {
                byte -= allocator.bitmap_data.len();
                allocator_count = allocator.next;
            }
        }
    }
    /** Mark as available */
    pub fn set_available<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        count: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut byte = count as usize / 8;
        let bit = count as usize % 8;

        let mut allocator_chain = Vec::new();
        loop {
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            if byte < allocator.bitmap_data.len() {
                allocator.bitmap_data[byte] |= 1 << (7 - bit);

                if allocator.rc > 0 {
                    allocator.rc -= 1;
                    allocator.sync(device, allocator_count)?;
                    allocator_count = fs.new_block()?;
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
                return Ok(());
            } else {
                byte -= allocator.bitmap_data.len();

                allocator_chain.push(allocator_count);
                allocator_count = allocator.next;
            }
        }
    }
    /** Mark as unavailable */
    pub fn set_unavailable<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_count: u64,
        count: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut byte = count as usize / 8;
        let bit = count as usize % 8;

        let mut allocator_chain = Vec::new();
        loop {
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            if byte < allocator.bitmap_data.len() {
                allocator.bitmap_data[byte] &= !(1 << (7 - bit));

                if allocator.rc > 0 {
                    allocator.rc -= 1;
                    allocator.sync(device, allocator_count)?;
                    allocator_count = fs.new_block()?;
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
                return Ok(());
            } else {
                byte -= allocator.bitmap_data.len();

                allocator_chain.push(allocator_count);
                allocator_count = allocator.next;
            }
        }
    }
    pub fn find_available<D>(
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

            for (i, byte) in allocator.bitmap_data.iter().enumerate() {
                if *byte != 0 {
                    for j in 0..8 {
                        let position = (i * 8 + j) as u64;
                        if AvailableInodeManager::get_available(
                            fs,
                            device,
                            allocator_count,
                            position,
                        )? {
                            return Ok(position);
                        }
                    }
                }
            }

            if allocator.next != 0 {
                allocator_count = allocator.next;
            } else {
                return Err(Error::new(ErrorKind::Other, ""));
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
            allocator_count = block_copy_out(fs, device, allocator_count)?;
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
            let mut allocator =
                AvailableInodeManager::load(fs.get_data_block(device, allocator_count)?);

            allocator.rc += 1;
            allocator.sync(device, allocator_count)?;

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
    pub igroup_mgt_btree: BtreeNode,
}

impl Subvolume {
    pub fn create<D>(fs: &mut Filesystem, device: &mut D) -> IOResult<Self>
    where
        D: Write + Read + Seek,
    {
        let mut subvol = Self::default();

        subvol.entry.inode_tree_root = BtreeNode::allocate_on_block(fs, device)?;
        subvol.entry.inode_alloc_block = AvailableInodeManager::allocate_on_block(fs, device)?;
        subvol.igroup_mgt_btree.block_count = subvol.entry.inode_tree_root;

        Ok(subvol)
    }
    pub fn new_inode<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        if let Ok(inode_group) =
            AvailableInodeManager::find_available(fs, device, self.entry.inode_alloc_block)
        {
            let inode_block_count = self.igroup_mgt_btree.lookup(fs, device, inode_group)?.value;
            let group = INodeGroup::load(fs.get_data_block(device, inode_block_count)?);

            let mut inode_count = 0;
            for (i, inode) in group.inodes.iter().enumerate() {
                if inode.is_empty_inode() {
                    inode_count = INODE_PER_GROUP as u64 * inode_group + i as u64;
                    break;
                }
            }

            if group.is_full() {
                AvailableInodeManager::set_unavailable(
                    fs,
                    device,
                    self.entry.inode_alloc_block,
                    inode_block_count,
                )?;
            }

            Ok(inode_count)
        } else {
            let inode_group_block = INodeGroup::allocate_on_block(fs, device)?;
            let inode_group_count = self.igroup_mgt_btree.find_unused(fs, device)?;
            self.igroup_mgt_btree
                .insert(fs, device, inode_group_count, inode_group_block)?;
            self.entry.inode_tree_root = self.igroup_mgt_btree.block_count;

            SubvolumeManager::set_subvolume(
                fs,
                device,
                fs.sb.subvol_mgr,
                self.entry.id,
                self.entry,
            )?;

            AvailableInodeManager::set_available(
                fs,
                device,
                self.entry.inode_alloc_block,
                inode_group_count,
            )?;

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
            .igroup_mgt_btree
            .lookup(fs, device, inode_group_count)?
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

        let btree_query_result = self
            .igroup_mgt_btree
            .lookup(fs, device, inode_group_count)
            .unwrap();
        let inode_group_block = btree_query_result.value;

        let mut inode_group = INodeGroup::load(fs.get_data_block(device, inode_group_block)?);
        inode_group.inodes[inode_num] = inode;
        if btree_query_result.rc > 0 {
            let new_inode_group_block = fs.new_block()?;
            self.igroup_mgt_btree
                .modify(fs, device, inode_group_count, new_inode_group_block)?;
            self.entry.inode_tree_root = self.igroup_mgt_btree.block_count;
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
        let inode_group_count = inode / INODE_PER_GROUP as u64;
        let btree_query_result = self
            .igroup_mgt_btree
            .lookup(fs, device, inode_group_count)?;
        let inode_group_block = btree_query_result.value;
        self.set_inode(fs, device, inode, INode::default())?;

        let inode_group = INodeGroup::load(fs.get_data_block(device, inode_group_block)?);

        /* release inode group */
        if inode_group.is_empty() {
            AvailableInodeManager::set_unavailable(
                fs,
                device,
                self.entry.inode_alloc_block,
                inode_group_block,
            )?;
            self.igroup_mgt_btree
                .remove(fs, device, inode_group_count)?;
            fs.release_block(inode_group_block);
            fs.sync_meta_data(device)?;
        }
        Ok(())
    }
}
