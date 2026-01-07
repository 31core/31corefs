use crate::{
    Filesystem,
    block::{BLOCK_SIZE, Block},
    subvol::Subvolume,
};
use std::{
    io::{Error, ErrorKind, Result as IOResult},
    io::{Read, Seek, Write},
};

const MAX_INTERNAL_COUNT: usize = (BLOCK_SIZE - ENTRY_START) / ENTRY_INTERNAL_SIZE;
const MAX_LEAF_COUNT: usize = (BLOCK_SIZE - ENTRY_START) / ENTRY_LEAF_SIZE;
const ENTRY_LEAF_SIZE: usize = 2 * 8 + 4;
const ENTRY_INTERNAL_SIZE: usize = 2 * 8;
const ENTRY_START: usize = 16;

const BTREE_NODE_TYPE_INTERNAL: u8 = 0xf0;
const BTREE_NODE_TYPE_LEAF: u8 = 0x0f;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum BtreeType {
    Internal,
    #[default]
    Leaf,
}

#[derive(Debug, Default, Clone, Copy)]
/**
 * # Data structure
 *
 * For internal node:
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |Key        |
 * |8    |16 |Value      |
 *
 * For leaf node:
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |8  |Key        |
 * |8    |16 |Value      |
 * |16   |20 |Reference count|
*/
pub struct BtreeEntry {
    pub key: u64,
    pub value: u64,
    pub rc: u32,
}

impl BtreeEntry {
    pub fn new(key: u64, value: u64) -> Self {
        Self {
            key,
            value,
            ..Default::default()
        }
    }
    pub fn load_internal(bytes: &[u8]) -> Self {
        Self {
            key: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            value: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            ..Default::default()
        }
    }
    pub fn dump_internal(&self) -> [u8; ENTRY_INTERNAL_SIZE] {
        let mut bytes = [0; ENTRY_INTERNAL_SIZE];

        bytes[..8].copy_from_slice(&self.key.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.value.to_be_bytes());

        bytes
    }
    pub fn load_leaf(bytes: &[u8]) -> Self {
        Self {
            key: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            value: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            rc: u32::from_be_bytes(bytes[16..20].try_into().unwrap()),
        }
    }
    pub fn dump_leaf(&self) -> [u8; ENTRY_LEAF_SIZE] {
        let mut bytes = [0; ENTRY_LEAF_SIZE];

        bytes[..8].copy_from_slice(&self.key.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.value.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.rc.to_be_bytes());

        bytes
    }
}

#[derive(Default, Debug, Clone)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |2  |Count of entries|
 * |2    |3  |Reserved   |
 * |3    |4  |Type       |
 * |4    |8  |Reference count|
 * |8    |16 |Reserved   |
 * |16   |4096|Entries   |
*/
pub struct BtreeNode {
    pub block_index: u64,
    pub rc: u32,
    pub entries: Vec<BtreeEntry>,
    pub node_type: BtreeType,
}

impl Block for BtreeNode {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        if bytes[3] == BTREE_NODE_TYPE_INTERNAL {
            Self::load_internal(bytes)
        } else {
            Self::load_leaf(bytes)
        }
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];

        block[..2].copy_from_slice(&(self.entries.len() as u16).to_be_bytes());
        match self.node_type {
            BtreeType::Internal => block[3] = BTREE_NODE_TYPE_INTERNAL,
            BtreeType::Leaf => block[3] = BTREE_NODE_TYPE_LEAF,
        }
        block[4..8].copy_from_slice(&self.rc.to_be_bytes());
        let content = &mut block[ENTRY_START..];

        for (i, entry) in self.entries.iter().enumerate() {
            match self.node_type {
                BtreeType::Internal => content
                    [ENTRY_INTERNAL_SIZE * i..ENTRY_INTERNAL_SIZE * (i + 1)]
                    .copy_from_slice(&entry.dump_internal()),
                BtreeType::Leaf => content[ENTRY_LEAF_SIZE * i..ENTRY_LEAF_SIZE * (i + 1)]
                    .copy_from_slice(&entry.dump_leaf()),
            }
        }
        block
    }
}

impl BtreeNode {
    fn load_internal(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self {
            node_type: BtreeType::Internal,
            rc: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            ..Default::default()
        };

        let content = &bytes[ENTRY_START..];
        let entries = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;

        for i in 0..entries {
            let entry = BtreeEntry::load_internal(
                &content[ENTRY_INTERNAL_SIZE * i..ENTRY_INTERNAL_SIZE * (i + 1)],
            );
            node.entries.push(entry);
        }
        node
    }
    fn load_leaf(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self {
            node_type: BtreeType::Leaf,
            rc: u32::from_be_bytes(bytes[4..8].try_into().unwrap()),
            ..Default::default()
        };

        let content = &bytes[ENTRY_START..];
        let entries = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;

        for i in 0..entries {
            let entry =
                BtreeEntry::load_leaf(&content[ENTRY_LEAF_SIZE * i..ENTRY_LEAF_SIZE * (i + 1)]);
            node.entries.push(entry);
        }
        node
    }
    /** Add a key into the node */
    fn add(&mut self, key: u64, value: u64) {
        if self.entries.is_empty() {
            self.entries.push(BtreeEntry::new(key, value));
        } else {
            for (i, _) in self.entries.iter().enumerate() {
                if i == 0 && key < self.entries[0].key {
                    self.entries.insert(0, BtreeEntry::new(key, value));
                    break;
                } else if i + 1 < self.entries.len()
                    && key > self.entries[i].key
                    && key < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    self.entries.insert(i + 1, BtreeEntry::new(key, value));
                    break;
                }
            }
        }
    }
    /** Part the node
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node */
    fn part<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<(u64, u64)>
    where
        D: Write + Read + Seek,
    {
        let mut right_node = Self {
            node_type: self.node_type,
            block_index: subvol.new_block(fs, device)?,
            ..Default::default()
        };
        for _ in 0..self.entries.len() / 2 {
            right_node.entries.insert(0, self.entries.pop().unwrap());
        }

        right_node.sync(device, right_node.block_index)?;
        self.sync(device, self.block_index)?;

        Ok((
            right_node.entries.first().unwrap().key,
            right_node.block_index,
        ))
    }
    /** Insert an offset into B-Tree */
    pub fn insert<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        key: u64,
        block_index: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if let Some((split_key, right_block)) =
            self._insert(fs, subvol, device, key, block_index)?
        {
            let mut left = Self {
                node_type: self.node_type,
                ..Default::default()
            };
            for entry in &self.entries {
                left.entries.push(*entry);
            }

            let left_block = subvol.new_block(fs, device)?;
            left.block_index = left_block;
            left.sync(device, left_block)?;

            self.entries.clear();
            self.entries.push(BtreeEntry::new(
                left.entries.first().unwrap().key,
                left_block,
            ));
            self.entries.push(BtreeEntry::new(split_key, right_block));
            self.node_type = BtreeType::Internal;
            self.sync(device, self.block_index)?;
        }

        Ok(())
    }
    /** Insert an id
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node
     */
    fn _insert<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        key: u64,
        block_index: u64,
    ) -> IOResult<Option<(u64, u64)>>
    where
        D: Write + Read + Seek,
    {
        self.cow_clone_node(fs, subvol, device)?;

        match self.node_type {
            BtreeType::Leaf => {
                self.add(key, block_index);

                /* part into two child nodes */
                if self.entries.len() > MAX_LEAF_COUNT {
                    return Ok(Some(self.part(fs, subvol, device)?));
                }
            }
            BtreeType::Internal => {
                /* find child node to insert */
                for i in 0..self.entries.len() {
                    if i + 1 < self.entries.len()
                        && key > self.entries[i].key
                        && key < self.entries[i + 1].key
                        || i == self.entries.len() - 1
                    {
                        let mut child_node = Self::load_block(device, self.entries[i].value)?;
                        child_node.block_index = self.entries[i].value;

                        /* if parted into two sub trees */
                        if let Some((split_key, right_block)) =
                            child_node._insert(fs, subvol, device, key, block_index)?
                        {
                            self.add(split_key, right_block);

                            if self.entries.len() > MAX_INTERNAL_COUNT {
                                return Ok(Some(self.part(fs, subvol, device)?));
                            }
                        }
                        self.entries[i].value = child_node.block_index;
                    }
                }
            }
        }
        self.sync(device, self.block_index)?;
        Ok(None)
    }
    /** Modify an offset from B-Tree */
    pub fn modify<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        key: u64,
        value: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        self.cow_clone_node(fs, subvol, device)?;

        match self.node_type {
            BtreeType::Leaf => {
                /* find and modify */
                for entry in &mut self.entries {
                    if entry.key == key {
                        entry.value = value;
                        entry.rc = 0;
                        break;
                    }
                }
            }
            BtreeType::Internal => {
                for i in 0..self.entries.len() {
                    if i + 1 < self.entries.len()
                        && key >= self.entries[i].key
                        && key < self.entries[i + 1].key
                        || i == self.entries.len() - 1
                    {
                        let mut child_node = Self::load_block(device, self.entries[i].value)?;
                        child_node.block_index = self.entries[i].value;

                        child_node.cow_clone_node(fs, subvol, device)?;

                        child_node.modify(fs, subvol, device, key, value)?;
                        self.entries[i].value = child_node.block_index;
                    }
                }
            }
        }
        self.sync(device, self.block_index)?;
        Ok(())
    }
    /** Remove an offset from B-Tree */
    pub fn remove<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        key: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        self._remove(fs, subvol, device, key)?;
        if self.entries.len() == 1 && self.node_type == BtreeType::Internal {
            let mut child = Self::load_block(device, self.entries[0].value)?;
            child.block_index = self.entries[0].value;

            self.entries.clear();
            for entry in &child.entries {
                self.entries.push(*entry);
            }

            child.cow_release_node(fs, subvol, device)?;

            self.sync(device, self.block_index)?;
        }

        Ok(())
    }
    fn _remove<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        key: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        self.cow_clone_node(fs, subvol, device)?;

        match self.node_type {
            BtreeType::Internal => {
                for i in 0..self.entries.len() {
                    if i + 1 < self.entries.len()
                        && key >= self.entries[i].key
                        && key < self.entries[i + 1].key
                        || i == self.entries.len() - 1
                    {
                        let mut child_node = Self::load_block(device, self.entries[i].value)?;
                        child_node.block_index = self.entries[i].value;

                        child_node._remove(fs, subvol, device, key)?;
                        self.entries[i].value = child_node.block_index;

                        /* child nodes can be merged into previous or next node */
                        if child_node.node_type == BtreeType::Internal
                            && child_node.entries.len() < MAX_INTERNAL_COUNT / 2
                            || child_node.node_type == BtreeType::Leaf
                                && child_node.entries.len() < MAX_LEAF_COUNT / 2
                        {
                            if i > 0 {
                                let mut previous_node =
                                    Self::load_block(device, self.entries[i - 1].value)?;
                                previous_node.block_index = self.entries[i - 1].value;

                                previous_node.cow_clone_node(fs, subvol, device)?;
                                self.entries[i - 1].value = previous_node.block_index;

                                /* merge this child node into previous node */
                                if child_node.node_type == BtreeType::Internal
                                    && previous_node.entries.len() + child_node.entries.len()
                                        <= MAX_INTERNAL_COUNT
                                    || child_node.node_type == BtreeType::Leaf
                                        && previous_node.entries.len() + child_node.entries.len()
                                            <= MAX_LEAF_COUNT
                                {
                                    for child_entry in child_node.entries.iter() {
                                        previous_node.entries.push(*child_entry);
                                    }

                                    child_node.cow_release_node(fs, subvol, device)?;
                                    self.entries.remove(i);
                                } else {
                                    let id = previous_node.entries.last().unwrap().key;
                                    child_node
                                        .entries
                                        .insert(0, previous_node.entries.pop().unwrap());
                                    child_node.sync(device, child_node.block_index)?;
                                    self.entries[i].key = id;
                                }
                                previous_node.sync(device, previous_node.block_index)?;
                            } else if i < self.entries.len() - 1 {
                                let mut next_node =
                                    Self::load_block(device, self.entries[i + 1].value)?;
                                next_node.block_index = self.entries[i + 1].value;

                                next_node.cow_clone_node(fs, subvol, device)?;
                                self.entries[i + 1].value = next_node.block_index;

                                /* merge this child node into next node */
                                if child_node.node_type == BtreeType::Internal
                                    && next_node.entries.len() + child_node.entries.len()
                                        <= MAX_INTERNAL_COUNT
                                    || child_node.node_type == BtreeType::Leaf
                                        && next_node.entries.len() + child_node.entries.len()
                                            <= MAX_LEAF_COUNT
                                {
                                    for child_entry in child_node.entries.iter().rev() {
                                        next_node.entries.insert(0, *child_entry);
                                    }
                                    self.entries[i + 1].key =
                                        next_node.entries.first().unwrap().key;

                                    child_node.cow_release_node(fs, subvol, device)?;

                                    self.entries.remove(i);
                                } else {
                                    next_node.entries.remove(0);
                                    child_node.entries.push(*next_node.entries.first().unwrap());
                                    child_node.sync(device, child_node.block_index)?;
                                    self.entries[i + 1].key =
                                        next_node.entries.first().unwrap().key;
                                }
                                next_node.sync(device, next_node.block_index)?;
                            }
                        }
                        break;
                    }
                }
            }
            BtreeType::Leaf => {
                /* find and remove */
                for (i, entry) in self.entries.iter().enumerate() {
                    if entry.key == key {
                        self.entries.remove(i);
                        break;
                    }
                }
            }
        }
        self.sync(device, self.block_index)?;
        Ok(())
    }
    /** Find pointer by id
     *
     * Return:
     * 1: block count
     */
    pub fn lookup<D>(&self, device: &mut D, key: u64) -> IOResult<BtreeEntry>
    where
        D: Write + Read + Seek,
    {
        match self.node_type {
            BtreeType::Internal => {
                for i in 0..self.entries.len() {
                    if i + 1 < self.entries.len()
                        && key >= self.entries[i].key
                        && key < self.entries[i + 1].key
                        || i == self.entries.len() - 1
                    {
                        let mut child = Self::load_block(device, self.entries[i].value)?;
                        child.block_index = self.entries[i].value;

                        return child.lookup(device, key);
                    }
                }
            }
            BtreeType::Leaf => {
                for entry in &self.entries {
                    if key == entry.key {
                        return Ok(*entry);
                    }
                }
            }
        }
        Err(Error::new(
            ErrorKind::NotFound,
            format!("No such key '{}'.", key),
        ))
    }
    fn find_unused_internal<D>(&self, device: &mut D) -> IOResult<(Option<u64>, Option<u64>)>
    where
        D: Write + Read + Seek,
    {
        if self.node_type == BtreeType::Internal {
            for i in 0..self.entries.len() {
                let mut child = Self::load_block(device, self.entries[i].value)?;
                child.block_index = self.entries[i].value;
                let result = child.find_unused_internal(device)?;

                if let Some(id) = result.0 {
                    return Ok((Some(id), None));
                } else if let Some(id) = result.1
                    && (i < self.entries.len() - 1 && id + 1 < self.entries[i + 1].key
                        || i == self.entries.len() - 1)
                {
                    return Ok((Some(id + 1), None));
                }
            }
        } else if self.entries.len() > 1 {
            for i in 0..self.entries.len() - 1 {
                if self.entries[i].key + 1 < self.entries[i + 1].key {
                    return Ok((Some(self.entries[i].key + 1), None));
                }
            }
            return Ok((None, Some(self.entries.last().unwrap().key + 1)));
        } else if self.entries.len() == 1 {
            return Ok((None, Some(self.entries.last().unwrap().key + 1)));
        }
        Ok((None, None))
    }
    /** Find unused id */
    pub fn find_unused<D>(&mut self, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let result = self.find_unused_internal(device)?;

        if let Some(id) = result.0 {
            Ok(id)
        } else if let Some(id) = result.1 {
            Ok(id)
        } else {
            Ok(0)
        }
    }
    /** Clone the full B-Tree */
    pub fn clone_tree<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if let BtreeType::Leaf = self.node_type {
            for entry in &mut self.entries {
                entry.rc += 1;
            }
        }
        self.rc += 1;
        self.sync(device, self.block_index)?;
        Ok(())
    }
    /** Destroy the full B-Tree */
    pub fn destroy<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.rc > 0 {
            self.rc -= 1;
            self.sync(device, self.block_index)?;

            return Ok(());
        }
        match self.node_type {
            BtreeType::Leaf => {
                for entry in self.entries.iter_mut() {
                    if entry.rc == 0 {
                        subvol.release_block(fs, device, entry.value)?;
                    } else {
                        entry.rc -= 1;
                    }
                }
            }
            BtreeType::Internal => {
                for entry in &self.entries {
                    let mut child_node = Self::load_block(device, entry.value)?;
                    child_node.block_index = entry.value;
                    child_node.destroy(fs, subvol, device)?;
                }
            }
        }

        self.cow_release_node(fs, subvol, device)?;
        Ok(())
    }
    /** Check and clone multiple referenced node */
    fn cow_clone_node<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.rc > 0 {
            if let BtreeType::Internal = self.node_type {
                /* make child nodes inherit rc of parent node */
                for entry in &mut self.entries {
                    let mut child_node = Self::load_block(device, entry.value)?;
                    child_node.block_index = entry.value;
                    child_node.rc += self.rc;
                    child_node.sync(device, child_node.block_index)?;
                }
            }

            self.rc = 0;
            self.sync(device, self.block_index)?;
            self.block_index = subvol.new_block(fs, device)?;
            self.sync(device, self.block_index)?;

            fs.sb.real_used_blocks += 1;
        }
        Ok(())
    }
    /** Check and release multiple referenced node */
    fn cow_release_node<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.rc > 0 {
            self.rc -= 1;
            self.sync(device, self.block_index)?;

            fs.sb.used_blocks -= 1;
        } else {
            subvol.release_block(fs, device, self.block_index)?;
        }
        Ok(())
    }
}
