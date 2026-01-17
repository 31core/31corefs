use crate::{
    Filesystem,
    block::{BLOCK_SIZE, BitmapBlock, BitmapIndexBlock, Block, INodeGroup},
    btree::BtreeNode,
    inode::{INODE_PER_GROUP, INode},
    utils::get_sys_time,
};
use std::{
    io::{Error, ErrorKind, Result as IOResult},
    io::{Read, Seek, Write},
};

const SUBVOLUMES: usize = BLOCK_SIZE / SUBVOLUME_ENTRY_SIZE - 1;
const SUBVOLUME_ENTRY_SIZE: usize = 128;

pub const SUBVOLUME_STATE_ALLOCATED: u8 = 1;
pub const SUBVOLUME_STATE_REMOVED: u8 = 2;

fn new_bitmap<D>(fs: &mut Filesystem, device: &mut D, bitmap_count: usize) -> IOResult<u64>
where
    D: Write + Read + Seek,
{
    let mut index_block_idx = BitmapIndexBlock::allocate_on_block(fs, device)?;
    let first_block = index_block_idx;

    let mut index_block = BitmapIndexBlock::default();
    for i in 0..bitmap_count {
        if i > 0 && i % index_block.bitmaps.len() == 0 {
            let next_block = BitmapIndexBlock::allocate_on_block(fs, device)?;
            index_block.next = next_block;
            index_block.sync(device, index_block_idx)?;
            index_block = BitmapIndexBlock::default();
            index_block_idx = next_block;
        }

        index_block.bitmaps[i % index_block.bitmaps.len()] =
            BitmapBlock::allocate_on_block(fs, device)?;
    }
    index_block.sync(device, index_block_idx)?;

    Ok(first_block)
}

fn merge_to_shared_bitmap<D>(device: &mut D, bitmap: u64, total_bitmap: u64) -> IOResult<()>
where
    D: Write + Read + Seek,
{
    let mut index_block = BitmapIndexBlock::load_block(device, bitmap)?;
    let total_index_block = BitmapIndexBlock::load_block(device, total_bitmap)?;
    loop {
        for (bitmap_index, bitmap) in index_block.bitmaps.iter().enumerate() {
            let bitmap = BitmapBlock::load_block(device, *bitmap)?;
            let mut total_bitmap =
                BitmapBlock::load_block(device, total_index_block.bitmaps[bitmap_index]).unwrap();
            for byte in 0..BLOCK_SIZE {
                total_bitmap.bytes[byte] |= bitmap.bytes[byte];
            }
            total_bitmap.sync(device, total_index_block.bitmaps[bitmap_index])?;
        }
        if index_block.next != 0 {
            index_block = BitmapIndexBlock::load_block(device, index_block.next)?;
        } else {
            break;
        }
    }

    Ok(())
}

fn clean_bitmap<D>(device: &mut D, bitmap: u64) -> IOResult<()>
where
    D: Write + Read + Seek,
{
    let mut index_block = BitmapIndexBlock::load_block(device, bitmap)?;
    loop {
        for (bitmap_index, bitmap) in index_block.bitmaps.iter().enumerate() {
            let mut bitmap = BitmapBlock::load_block(device, *bitmap)?;
            for byte in 0..BLOCK_SIZE {
                bitmap.bytes[byte] = 0;
            }
            bitmap.sync(device, index_block.bitmaps[bitmap_index])?;
        }
        if index_block.next != 0 {
            index_block = BitmapIndexBlock::load_block(device, index_block.next)?;
        } else {
            break;
        }
    }

    Ok(())
}

const SUBVOL_TYPE_NORMAL: u8 = 1;
const SUBVOL_TYPE_SNAP: u8 = 2;

const SUBVOL_FLAG_RO: u8 = 1;

#[derive(Default, Debug, Clone, Copy)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |ID         |
 * |8    |16 |Inode B-Tree|
 * |16   |24 |Root Inode |
 * |24   |32 |Bitmap block|
 * |32   |40 |Shared bitmap block|
 * |40   |48 |IGroup bitmap block|
 * |48   |56 |Used blocks|
 * |56   |64 |Real used blocks|
 * |64   |72 |Create date|
 * |72   |80 |Snapshot count|
 * |80   |88 |Parent subvolume (for snapshot only)|
 * |88   |89 |Statement |
 * |89   |90 |Flags     |
 */
pub struct SubvolumeEntry {
    pub id: u64,
    pub inode_tree_root: u64,
    pub root_inode: u64,
    pub bitmap: u64,
    pub shared_bitmap: u64,
    pub igroup_bitmap: u64,
    pub used_blocks: u64,
    pub real_used_blocks: u64,
    pub creation_date: u64,
    pub snaps: u64,
    pub parent_subvol: u64,
    pub state: u8,
    pub subvol_type: u8,
    pub flags: u8,
}

impl SubvolumeEntry {
    pub fn load(bytes: &[u8]) -> Self {
        Self {
            id: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            inode_tree_root: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            root_inode: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
            bitmap: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
            shared_bitmap: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
            igroup_bitmap: u64::from_be_bytes(bytes[40..48].try_into().unwrap()),
            used_blocks: u64::from_be_bytes(bytes[48..56].try_into().unwrap()),
            real_used_blocks: u64::from_be_bytes(bytes[56..64].try_into().unwrap()),
            creation_date: u64::from_be_bytes(bytes[64..72].try_into().unwrap()),
            snaps: u64::from_be_bytes(bytes[72..80].try_into().unwrap()),
            parent_subvol: u64::from_be_bytes(bytes[80..88].try_into().unwrap()),
            state: bytes[88],
            subvol_type: bytes[89],
            flags: bytes[90],
        }
    }
    pub fn dump(&self) -> [u8; SUBVOLUME_ENTRY_SIZE] {
        let mut bytes = [0; SUBVOLUME_ENTRY_SIZE];

        bytes[..8].copy_from_slice(&self.id.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.inode_tree_root.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.root_inode.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.bitmap.to_be_bytes());
        bytes[32..40].copy_from_slice(&self.shared_bitmap.to_be_bytes());
        bytes[40..48].copy_from_slice(&self.igroup_bitmap.to_be_bytes());
        bytes[48..56].copy_from_slice(&self.used_blocks.to_be_bytes());
        bytes[56..64].copy_from_slice(&self.real_used_blocks.to_be_bytes());
        bytes[64..72].copy_from_slice(&self.creation_date.to_be_bytes());
        bytes[72..80].copy_from_slice(&self.snaps.to_be_bytes());
        bytes[80..88].copy_from_slice(&self.parent_subvol.to_be_bytes());
        bytes[88] = self.state;
        bytes[89] = self.subvol_type;

        bytes
    }
    pub fn is_readonly(&self) -> bool {
        self.flags & SUBVOL_FLAG_RO != 0
    }
}

#[derive(Debug, Default, Clone)]
/**
 * # Data structure
 *
 * |Start|End |Description|
 * |-----|----|-----------|
 * |0    |8   |Next pointer|
 * |8    |16  |Count of entries|
 * |128  |4096|Entries   |
*/
pub struct SubvolumeManager {
    pub next: u64,
    pub entries: Vec<SubvolumeEntry>,
}

impl Block for SubvolumeManager {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut mgr = Self {
            next: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            ..Default::default()
        };

        let entries_num = u64::from_be_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let entries_content = &bytes[SUBVOLUME_ENTRY_SIZE..];

        for i in 0..entries_num {
            let entry = SubvolumeEntry::load(&entries_content[SUBVOLUME_ENTRY_SIZE * i..]);
            mgr.entries.push(entry);
        }
        mgr
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[..8].copy_from_slice(&self.next.to_be_bytes());
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
    /** Generate ID for a new subvolume */
    fn generate_new_id<D>(device: &mut D, mut mgr_block_index: u64) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let mut max_id = 0; // the maxium entry ID + 1 in previous block
        loop {
            let mgr = Self::load_block(device, mgr_block_index)?;

            if mgr.next == 0 {
                return match mgr.entries.last() {
                    Some(subvol) => Ok(subvol.id + 1),
                    None => Ok(max_id),
                };
            } else {
                if let Some(entry) = mgr.entries.last() {
                    max_id = entry.id + 1;
                }
                mgr_block_index = mgr.next;
            }
        }
    }
    fn get_subvol_internal<D>(&self, device: &mut D, id: u64) -> IOResult<Subvolume>
    where
        D: Write + Read + Seek,
    {
        for entry in &self.entries {
            if entry.id == id {
                let mut igroup_mgt_btree = BtreeNode::load_block(device, entry.inode_tree_root)?;
                igroup_mgt_btree.block_index = entry.inode_tree_root;
                return Ok(Subvolume {
                    entry: *entry,
                    igroup_mgt_btree,
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
        device: &mut D,
        mut mgr_block_index: u64,
        id: u64,
    ) -> IOResult<Subvolume>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mgr = Self::load_block(device, mgr_block_index)?;

            match mgr.get_subvol_internal(device, id) {
                Ok(subvol) => return Ok(subvol),
                Err(err) => {
                    if mgr.next != 0 {
                        mgr_block_index = mgr.next;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }
    /** Set subvolume sntry */
    pub fn set_subvolume<D>(
        device: &mut D,
        mut mgr_block_index: u64,
        id: u64,
        entry: SubvolumeEntry,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut mgr = Self::load_block(device, mgr_block_index)?;

            for (i, this_entry) in mgr.entries.iter().enumerate() {
                if this_entry.id == id {
                    mgr.entries[i] = entry;
                    mgr.sync(device, mgr_block_index)?;
                    return Ok(());
                }
            }

            if mgr.next != 0 {
                mgr_block_index = mgr.next;
            } else {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("No such subvolume '{id}'"),
                ));
            }
        }
    }
    /** Create a new subvolume */
    pub fn new_subvolume<D>(fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let mut mgr_block_index = fs.sb.subvol_mgr;
        loop {
            let mut mgr = Self::load_block(device, mgr_block_index)?;
            if mgr.next == 0 {
                if mgr.entries.len() < SUBVOLUMES {
                    let entry = SubvolumeEntry {
                        id: Self::generate_new_id(device, fs.sb.subvol_mgr)?,
                        inode_tree_root: BtreeNode::allocate_on_block(fs, device)?,
                        igroup_bitmap: IGroupBitmap::allocate_on_block(fs, device)?,
                        bitmap: new_bitmap(fs, device, fs.groups.len())?,
                        creation_date: get_sys_time(),
                        state: SUBVOLUME_STATE_ALLOCATED,
                        subvol_type: SUBVOL_TYPE_NORMAL,
                        ..Default::default()
                    };
                    let subvol_id = entry.id;
                    mgr.entries.push(entry);
                    mgr.sync(device, mgr_block_index)?;

                    let mut subvol = Self::get_subvolume(device, fs.sb.subvol_mgr, subvol_id)?;
                    crate::dir::create(fs, &mut subvol, device)?;
                    return Ok(subvol_id);
                } else {
                    let new_mgr_id = SubvolumeManager::allocate_on_block(fs, device)?;
                    mgr.next = new_mgr_id;
                    mgr.sync(device, mgr_block_index)?;
                    mgr_block_index = new_mgr_id;
                }
            } else {
                mgr_block_index = mgr.next;
            }
        }
    }
    /** Remove a subvolume */
    pub fn remove_subvolume<D>(fs: &mut Filesystem, device: &mut D, id: u64) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut mgr_block_index = fs.sb.subvol_mgr;
        loop {
            let mut mgr = Self::load_block(device, mgr_block_index)?;

            for (i, subvol) in mgr.entries.iter_mut().enumerate() {
                if subvol.id == id {
                    IGroupBitmap::destroy_blocks(fs, device, subvol.igroup_bitmap)?;

                    if subvol.snaps == 0 && subvol.state == SUBVOLUME_STATE_REMOVED {
                        subvol.bitmap = subvol.shared_bitmap;
                    }

                    let mut index_block = BitmapIndexBlock::load_block(device, subvol.bitmap)?;

                    let mut bitmap_index = 0;
                    /* unmark blocks from global bitmap */
                    for group in 0..fs.groups.len() {
                        let bitmap = BitmapBlock::load_block(
                            device,
                            index_block.bitmaps[bitmap_index % index_block.bitmaps.len()],
                        )?;
                        for byte in 0..BLOCK_SIZE {
                            fs.groups[group].bitmap.bytes[byte] &= !bitmap.bytes[byte];
                        }
                        bitmap_index += 1;
                        if bitmap_index % index_block.bitmaps.len() == 0 {
                            index_block = BitmapIndexBlock::load_block(device, index_block.next)?;
                        }
                    }

                    if subvol.state != SUBVOLUME_STATE_REMOVED {
                        fs.sb.used_blocks -= subvol.used_blocks;
                    }

                    if subvol.subvol_type == SUBVOL_TYPE_SNAP {
                        let mut parent =
                            Self::get_subvolume(device, fs.sb.subvol_mgr, subvol.parent_subvol)?;
                        parent.entry.snaps -= 1;
                        Self::set_subvolume(
                            device,
                            fs.sb.subvol_mgr,
                            subvol.parent_subvol,
                            parent.entry,
                        )?;
                        if parent.entry.snaps == 0 && parent.entry.state == SUBVOLUME_STATE_REMOVED
                        {
                            SubvolumeManager::remove_subvolume(fs, device, parent.entry.id)?;
                        }
                    }
                    if subvol.snaps > 0 {
                        subvol.state = SUBVOLUME_STATE_REMOVED;
                    } else {
                        if subvol.state != SUBVOLUME_STATE_REMOVED {
                            fs.sb.real_used_blocks -= subvol.real_used_blocks;
                        }
                        mgr.entries.remove(i);
                    }

                    mgr.sync(device, mgr_block_index)?;
                    return Ok(());
                }
            }

            if mgr.next == 0 {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    format!("No such subvolume '{id}'"),
                ));
            } else {
                mgr_block_index = mgr.next;
            }
        }
    }
    /** Create a snapshot */
    pub fn create_snapshot<D>(fs: &mut Filesystem, device: &mut D, id: u64) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let snap_id = Self::new_subvolume(fs, device)?;
        let mut snap = Self::get_subvolume(device, fs.sb.subvol_mgr, id)?;
        let mut origin_subvol = Self::get_subvolume(device, fs.sb.subvol_mgr, id)?;

        snap.entry.id = snap_id;
        snap.entry.shared_bitmap = new_bitmap(fs, device, fs.groups.len())?;
        snap.entry.creation_date = get_sys_time();
        snap.entry.parent_subvol = id;
        snap.entry.subvol_type = SUBVOL_TYPE_SNAP;
        snap.entry.used_blocks = origin_subvol.entry.used_blocks;
        Self::set_subvolume(device, fs.sb.subvol_mgr, snap_id, snap.entry)?;

        origin_subvol.entry.snaps += 1;
        /* allocate shared bitmap if empty */
        if origin_subvol.entry.shared_bitmap == 0 {
            origin_subvol.entry.shared_bitmap = new_bitmap(fs, device, fs.groups.len())?;
        }
        merge_to_shared_bitmap(
            device,
            origin_subvol.entry.bitmap,
            origin_subvol.entry.shared_bitmap,
        )?;
        clean_bitmap(device, origin_subvol.entry.bitmap)?;
        Self::set_subvolume(device, fs.sb.subvol_mgr, id, origin_subvol.entry)?;

        origin_subvol.igroup_mgt_btree.clone_tree(device)?; // clone inode tree
        IGroupBitmap::clone_blocks(device, origin_subvol.entry.igroup_bitmap)?;

        fs.sb.used_blocks += origin_subvol.entry.used_blocks;
        Ok(snap_id)
    }
    /** List submolumes */
    pub fn list_subvols<D>(
        device: &mut D,
        mut mgr_block_index: u64,
    ) -> IOResult<Vec<SubvolumeEntry>>
    where
        D: Read + Write + Seek,
    {
        let mut ids = Vec::new();
        loop {
            let mgr = Self::load_block(device, mgr_block_index)?;

            for this_entry in &mgr.entries {
                if this_entry.state != SUBVOLUME_STATE_REMOVED {
                    ids.push(*this_entry);
                }
            }

            if mgr.next != 0 {
                mgr_block_index = mgr.next;
            } else {
                break;
            }
        }

        Ok(ids)
    }
}

#[derive(Debug)]
/**
 * # Data structure
 *
 * |Start  |End|Description|
 * |-------|---|-----------|
 * |0      |8  |Pointer of the next block|
 * |8      |16 |Reference count|
 * |8*(N+2)|8*(N+2)|Inode group bitmap|
 */
pub struct IGroupBitmap {
    pub next: u64,
    pub rc: u64,
    pub bitmap_data: [u8; BLOCK_SIZE - 16],
}

impl Default for IGroupBitmap {
    fn default() -> Self {
        Self {
            next: 0,
            rc: 0,
            bitmap_data: [0; BLOCK_SIZE - 16],
        }
    }
}

impl Block for IGroupBitmap {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        Self {
            next: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            rc: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            bitmap_data: bytes[16..].try_into().unwrap(),
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0; BLOCK_SIZE];

        bytes[..8].copy_from_slice(&self.next.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.rc.to_be_bytes());
        bytes[16..].copy_from_slice(&self.bitmap_data);

        bytes
    }
}

impl IGroupBitmap {
    /** Get if a inode group is vailable */
    pub fn get_available<D>(
        device: &mut D,
        mut allocator_block_index: u64,
        index: u64,
    ) -> IOResult<bool>
    where
        D: Write + Read + Seek,
    {
        let mut byte = index as usize / 8;
        let bit = index as usize % 8;
        loop {
            let allocator = IGroupBitmap::load_block(device, allocator_block_index)?;

            if byte < allocator.bitmap_data.len() {
                return Ok(allocator.bitmap_data[byte] & (1 << (7 - bit)) != 0);
            } else if allocator.next != 0 {
                byte -= allocator.bitmap_data.len();
                allocator_block_index = allocator.next;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }
    }
    /** Mark as available */
    pub fn set_available<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        index: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut allocator_blk_idx = subvol.entry.igroup_bitmap;
        let mut byte = index as usize / 8;
        let bit = index as usize % 8;

        let mut last_allocator_blk_idx = None;
        loop {
            let mut allocator = IGroupBitmap::load_block(device, allocator_blk_idx)?;

            if allocator.rc > 0 {
                allocator.rc -= 1;
                allocator.sync(device, allocator_blk_idx)?;
                allocator_blk_idx = subvol.new_block(fs, device)?;
                allocator.rc = 0;

                if let Some(last_allocator_blk_idx) = last_allocator_blk_idx {
                    let mut last_allocator =
                        IGroupBitmap::load_block(device, last_allocator_blk_idx)?;
                    last_allocator.next = allocator_blk_idx;
                    last_allocator.sync(device, last_allocator_blk_idx)?;
                }
            }

            if byte < allocator.bitmap_data.len() {
                allocator.bitmap_data[byte] |= 1 << (7 - bit);
                allocator.sync(device, allocator_blk_idx)?;
                return Ok(());
            } else if allocator.next != 0 {
                byte -= allocator.bitmap_data.len();

                last_allocator_blk_idx = Some(allocator_blk_idx);
                allocator_blk_idx = allocator.next;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }
    }
    /** Mark as unavailable */
    pub fn set_unavailable<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        index: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        let mut allocator_blk_idx = subvol.entry.igroup_bitmap;
        let mut byte = index as usize / 8;
        let bit = index as usize % 8;

        let mut last_allocator_blk_idx = None;
        loop {
            let mut allocator = IGroupBitmap::load_block(device, allocator_blk_idx)?;

            if allocator.rc > 0 {
                allocator.rc -= 1;
                allocator.sync(device, allocator_blk_idx)?;
                allocator_blk_idx = subvol.new_block(fs, device)?;
                allocator.rc = 0;

                if let Some(last_allocator_blk_idx) = last_allocator_blk_idx {
                    let mut last_allocator =
                        IGroupBitmap::load_block(device, last_allocator_blk_idx)?;
                    last_allocator.next = allocator_blk_idx;
                    last_allocator.sync(device, last_allocator_blk_idx)?;
                }
            }

            if byte < allocator.bitmap_data.len() {
                allocator.bitmap_data[byte] &= !(1 << (7 - bit));
                allocator.sync(device, allocator_blk_idx)?;
                return Ok(());
            } else if allocator.next != 0 {
                byte -= allocator.bitmap_data.len();

                last_allocator_blk_idx = Some(allocator_blk_idx);
                allocator_blk_idx = allocator.next;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }
    }
    pub fn find_available<D>(device: &mut D, mut allocator_block_index: u64) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        loop {
            let allocator = IGroupBitmap::load_block(device, allocator_block_index)?;

            for (i, byte) in allocator.bitmap_data.iter().enumerate() {
                if *byte != 0 {
                    for j in 0..8 {
                        let position = (i * 8 + j) as u64;
                        if IGroupBitmap::get_available(device, allocator_block_index, position)? {
                            return Ok(position);
                        }
                    }
                }
            }

            if allocator.next != 0 {
                allocator_block_index = allocator.next;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }
    }
    /** Recursively clone blocks */
    pub fn clone_blocks<D>(device: &mut D, mut allocator_block_index: u64) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut allocator = IGroupBitmap::load_block(device, allocator_block_index)?;

            allocator.rc += 1;
            allocator.sync(device, allocator_block_index)?;

            if allocator.next == 0 {
                return Ok(());
            } else {
                allocator_block_index = allocator.next;
            }
        }
    }
    /** Recursively destroy blocks */
    pub fn destroy_blocks<D>(
        fs: &mut Filesystem,
        device: &mut D,
        mut allocator_block_index: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        loop {
            let mut allocator = IGroupBitmap::load_block(device, allocator_block_index)?;

            if allocator.rc > 0 {
                allocator.rc -= 1;
                allocator.sync(device, allocator_block_index)?;
            } else {
                fs.release_block(allocator_block_index);
            }

            if allocator.next == 0 {
                return Ok(());
            } else {
                allocator_block_index = allocator.next;
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct Subvolume {
    pub entry: SubvolumeEntry,
    pub igroup_mgt_btree: BtreeNode,
}

impl Subvolume {
    pub fn new_inode<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        if let Ok(inode_group) = IGroupBitmap::find_available(device, self.entry.igroup_bitmap) {
            let inode_block_index = self.igroup_mgt_btree.lookup(device, inode_group)?.value;
            let group = INodeGroup::load_block(device, inode_block_index)?;

            let mut inode_number = 0;
            for (i, inode) in group.inodes.iter().enumerate() {
                if inode.is_empty_inode() {
                    inode_number = INODE_PER_GROUP as u64 * inode_group + i as u64;
                    break;
                }
            }

            Ok(inode_number)
        } else {
            let inode_group_block = INodeGroup::allocate_on_block(fs, device)?;
            let inode_group_index = self.igroup_mgt_btree.find_unused(device)?;
            self.igroup_mgt_btree.insert(
                fs,
                &mut self.clone(),
                device,
                inode_group_index,
                inode_group_block,
            )?;
            self.entry.inode_tree_root = self.igroup_mgt_btree.block_index;

            SubvolumeManager::set_subvolume(device, fs.sb.subvol_mgr, self.entry.id, self.entry)?;

            IGroupBitmap::set_available(fs, self, device, inode_group_index)?;

            Ok(inode_group_index * INODE_PER_GROUP as u64)
        }
    }
    pub fn get_inode<D>(&self, device: &mut D, inode_number: u64) -> IOResult<INode>
    where
        D: Read + Write + Seek,
    {
        let inode_group_index = inode_number / INODE_PER_GROUP as u64;
        let inode_num = inode_number as usize % INODE_PER_GROUP;
        let inode_group_block = self
            .igroup_mgt_btree
            .lookup(device, inode_group_index)?
            .value;

        let inode_group = INodeGroup::load_block(device, inode_group_block)?;
        Ok(inode_group.inodes[inode_num])
    }
    pub fn set_inode<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        inode_number: u64,
        inode: INode,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let igroup_index = inode_number / INODE_PER_GROUP as u64;
        let igroup_offset = inode_number as usize % INODE_PER_GROUP;

        let btree_query_result = self.igroup_mgt_btree.lookup(device, igroup_index)?;
        let inode_group_block = btree_query_result.value;

        let mut inode_group = INodeGroup::load_block(device, inode_group_block)?;
        inode_group.inodes[igroup_offset] = inode;

        if inode_group.is_full() {
            IGroupBitmap::set_unavailable(fs, self, device, igroup_index)?;
        }

        if btree_query_result.rc > 0 {
            let new_inode_group_block = self.new_block(fs, device)?;
            self.igroup_mgt_btree.modify(
                fs,
                &mut self.clone(),
                device,
                igroup_index,
                new_inode_group_block,
            )?;
            self.entry.inode_tree_root = self.igroup_mgt_btree.block_index;
            SubvolumeManager::set_subvolume(device, fs.sb.subvol_mgr, self.entry.id, self.entry)?;

            inode_group.sync(device, new_inode_group_block)?;
            for (i, inode) in inode_group.inodes.iter().enumerate() {
                if !inode.is_empty_inode() {
                    crate::file::clone_by_inode(
                        self,
                        device,
                        igroup_index * INODE_PER_GROUP as u64 + i as u64,
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
        inode_number: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let inode_group_index = inode_number / INODE_PER_GROUP as u64;
        let btree_query_result = self.igroup_mgt_btree.lookup(device, inode_group_index)?;
        let inode_group_block = btree_query_result.value;
        self.set_inode(fs, device, inode_number, INode::empty())?;

        let inode_group = INodeGroup::load_block(device, inode_group_block)?;

        /* release inode group */
        if inode_group.is_empty() {
            IGroupBitmap::set_unavailable(fs, self, device, inode_group_block)?;
            self.igroup_mgt_btree
                .remove(fs, &mut self.clone(), device, inode_group_index)?;
            fs.release_block(inode_group_block);
            fs.sync_meta_data(device)?;
        }
        Ok(())
    }
    /** Allocate a data block */
    pub fn new_block<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let block_index = fs.new_block()?;
        self.entry.used_blocks += 1;
        self.entry.real_used_blocks += 1;

        let mut index = BitmapIndexBlock::load_block(device, self.entry.bitmap)?;

        let mut i = block_index;
        loop {
            if i < (index.bitmaps.len() * BLOCK_SIZE * 8) as u64 {
                let mut bitmap =
                    BitmapBlock::load_block(device, index.bitmaps[i as usize / (8 * BLOCK_SIZE)])?;
                bitmap.set_used(i % (8 * BLOCK_SIZE as u64));
                bitmap.sync(device, index.bitmaps[i as usize / (8 * BLOCK_SIZE)])?;
                break;
            } else if index.next != 0 {
                i -= (index.bitmaps.len() * BLOCK_SIZE * 8) as u64;
                index = BitmapIndexBlock::load_block(device, index.next)?;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }

        Ok(block_index)
    }
    /** Release a data block from shared_bitmap */
    fn release_shared_block<D>(&mut self, device: &mut D, mut block_index: u64) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut index = BitmapIndexBlock::load_block(device, self.entry.shared_bitmap)?;
        loop {
            if block_index < (index.bitmaps.len() * BLOCK_SIZE * 8) as u64 {
                let mut bitmap = BitmapBlock::load_block(
                    device,
                    index.bitmaps[block_index as usize / (8 * BLOCK_SIZE)],
                )?;
                if bitmap.get_used(block_index % (8 * BLOCK_SIZE as u64)) {
                    bitmap.set_unused(block_index % (8 * BLOCK_SIZE as u64));
                    bitmap.sync(
                        device,
                        index.bitmaps[block_index as usize / (8 * BLOCK_SIZE)],
                    )?;
                    self.entry.real_used_blocks -= 1;
                } else {
                    SubvolumeManager::get_subvolume(device, 0, self.entry.parent_subvol)?
                        .release_shared_block(device, block_index)?;
                }

                break;
            } else if index.next != 0 {
                block_index -= (index.bitmaps.len() * BLOCK_SIZE * 8) as u64;
                index = BitmapIndexBlock::load_block(device, index.next)?;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }

        Ok(())
    }
    /** Release a data block */
    pub fn release_block<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        mut block_index: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut index = BitmapIndexBlock::load_block(device, self.entry.bitmap)?;
        loop {
            if block_index < (index.bitmaps.len() * BLOCK_SIZE * 8) as u64 {
                let mut bitmap = BitmapBlock::load_block(
                    device,
                    index.bitmaps[block_index as usize / (8 * BLOCK_SIZE)],
                )?;
                if bitmap.get_used(block_index % (8 * BLOCK_SIZE as u64)) {
                    bitmap.set_unused(block_index % (8 * BLOCK_SIZE as u64));
                    bitmap.sync(
                        device,
                        index.bitmaps[block_index as usize / (8 * BLOCK_SIZE)],
                    )?;

                    self.entry.real_used_blocks -= 1;
                } else {
                    self.release_shared_block(device, block_index)?;
                }
                self.entry.used_blocks -= 1;

                break;
            } else if index.next != 0 {
                block_index -= (index.bitmaps.len() * BLOCK_SIZE * 8) as u64;
                index = BitmapIndexBlock::load_block(device, index.next)?;
            } else {
                return Err(Error::other("Unexpected end of linked list."));
            }
        }

        fs.release_block(block_index);
        Ok(())
    }
    /** Synchronize subvolume entry to disk */
    pub fn sync_meta_data<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::set_subvolume(device, fs.sb.subvol_mgr, self.entry.id, self.entry)?;

        Ok(())
    }
}
