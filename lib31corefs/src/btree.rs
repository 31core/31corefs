use crate::block::*;
use crate::Filesystem;
use std::io::Error;
use std::io::{ErrorKind, Read, Result as IOResult, Seek, Write};

const MAX_INTERNAL_COUNT: usize = BLOCK_SIZE / ENTRY_INTERNAL_SIZE - 1;
const MAX_LEAF_COUNT: usize = BLOCK_SIZE / ENTRY_LEAF_SIZE;
const ENTRY_LEAF_SIZE: usize = 3 * 8;
const ENTRY_INTERNAL_SIZE: usize = 2 * 8;
const ENTRY_START: usize = 16;

//#[default] Leaf
#[derive(Debug, Default, Clone, Copy)]
pub enum BtreeType {
    #[default]
    Internal,
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
 * |16   |24 |Reference count|
*/
pub struct BtreeEntry {
    pub key: u64,
    pub value: u64,
    pub rc: u64,
}

impl BtreeEntry {
    pub fn new(key: u64, value: u64) -> Self {
        Self { key, value, rc: 0 }
    }
    pub fn load_internal(bytes: &[u8]) -> Self {
        Self {
            key: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            value: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            ..Default::default()
        }
    }
    pub fn dump_internal(&self) -> [u8; ENTRY_INTERNAL_SIZE] {
        let mut bytes = [0; ENTRY_INTERNAL_SIZE];

        bytes[0..8].copy_from_slice(&self.key.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.value.to_be_bytes());

        bytes
    }
    pub fn load_leaf(bytes: &[u8]) -> Self {
        Self {
            key: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            value: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            rc: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        }
    }
    pub fn dump_leaf(&self) -> [u8; ENTRY_LEAF_SIZE] {
        let mut bytes = [0; ENTRY_LEAF_SIZE];

        bytes[0..8].copy_from_slice(&self.key.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.value.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.rc.to_be_bytes());

        bytes
    }
}

#[derive(Default, Debug)]
/**
 * # Data structure
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |2  |Count of entries|
 * |2    |3  |Depth      |
 * |3    |8  |Reserved   |
 * |8    |16 |Reference count|
 * |16   |4096|Entries   |
*/
pub struct BtreeNode {
    pub block_count: u64,
    pub rc: u64,
    /// only root node has this field
    pub depth: u8,
    pub entries: Vec<BtreeEntry>,
    pub r#type: BtreeType,
}

impl Block for BtreeNode {
    fn load(_bytes: [u8; BLOCK_SIZE]) -> Self {
        Self::default()
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];

        block[0..2].copy_from_slice(&(self.entries.len() as u16).to_be_bytes());
        block[2] = self.depth;
        block[8..16].copy_from_slice(&self.rc.to_be_bytes());
        let content = &mut block[ENTRY_START..];

        for (i, entry) in self.entries.iter().enumerate() {
            match self.r#type {
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
    pub fn load_internal(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self {
            r#type: BtreeType::Internal,
            depth: bytes[2],
            rc: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            ..Default::default()
        };

        let content = &bytes[ENTRY_START..];
        let entries = u16::from_be_bytes(bytes[0..2].try_into().unwrap()) as usize;

        for i in 0..entries {
            let entry = BtreeEntry::load_internal(
                &content[ENTRY_INTERNAL_SIZE * i..ENTRY_INTERNAL_SIZE * (i + 1)],
            );
            node.entries.push(entry);
        }
        node
    }
    pub fn load_leaf(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self {
            r#type: BtreeType::Leaf,
            ..Default::default()
        };

        let content = &bytes[ENTRY_START..];
        let entries = u16::from_be_bytes(bytes[0..2].try_into().unwrap()) as usize;
        node.rc = u64::from_be_bytes(bytes[8..16].try_into().unwrap());

        for i in 0..entries {
            let entry =
                BtreeEntry::load_leaf(&content[ENTRY_LEAF_SIZE * i..ENTRY_LEAF_SIZE * (i + 1)]);
            node.entries.push(entry);
        }
        node
    }
    pub fn new(block_count: u64, node_type: BtreeType, block: &[u8; BLOCK_SIZE]) -> Self {
        let mut node = if block[2] > 0 {
            Self::load_internal(*block)
        } else {
            match node_type {
                BtreeType::Internal => Self::load_internal(*block),
                BtreeType::Leaf => Self::load_leaf(*block),
            }
        };
        node.block_count = block_count;
        node
    }
    /** Add an id into the node */
    fn add(&mut self, id: u64, ptr: u64) {
        if self.entries.is_empty() {
            self.entries.push(BtreeEntry::new(id, ptr));
        } else {
            for (i, _) in self.entries.iter().enumerate() {
                if i == 0 && id < self.entries[0].key {
                    self.entries.insert(0, BtreeEntry::new(id, ptr));
                    break;
                } else if i < self.entries.len() - 1
                    && id > self.entries[i].key
                    && id < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    self.entries.insert(i + 1, BtreeEntry::new(id, ptr));
                    break;
                }
            }
        }
    }
    /** Push an id into the current node
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node */
    fn part<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<(u64, u64)>
    where
        D: Write + Read + Seek,
    {
        let mut next = Self {
            r#type: self.r#type,
            block_count: fs.new_block()?,
            ..Default::default()
        };
        for _ in 0..self.entries.len() / 2 {
            next.entries.insert(0, self.entries.pop().unwrap());
        }

        next.sync(device, next.block_count)?;
        self.sync(device, self.block_count)?;

        Ok((next.entries.first().unwrap().key, next.block_count))
    }
    /** Insert an offset into B-Tree */
    pub fn insert<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.depth == 0 {
            self.r#type = BtreeType::Leaf;
        } else {
            self.r#type = BtreeType::Internal;
        }

        self.cow_clone_node(fs, device)?;

        if let Some((id, block)) =
            self.insert_internal(fs, device, offset, block, self.depth as usize)?
        {
            let mut left = if self.depth == 0 {
                Self {
                    r#type: BtreeType::Leaf,
                    ..Default::default()
                }
            } else {
                Self {
                    r#type: BtreeType::Internal,
                    ..Default::default()
                }
            };
            for i in 0..self.entries.len() {
                left.entries.push(self.entries[i]);
            }

            let left_block = fs.new_block()?;
            left.block_count = left_block;
            left.sync(device, left_block)?;

            self.entries.clear();
            self.entries.push(BtreeEntry::new(
                left.entries.first().unwrap().key,
                left_block,
            ));
            self.entries.push(BtreeEntry::new(id, block));
            self.r#type = BtreeType::Internal;
            self.sync(device, self.block_count)?;

            self.depth += 1;
        }

        Ok(())
    }
    /** Insert an id
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node
     */
    fn insert_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
        depth: usize,
    ) -> IOResult<Option<(u64, u64)>>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            self.add(offset, block);

            /* part into two child nodes */
            if self.entries.len() > MAX_LEAF_COUNT {
                return Ok(Some(self.part(fs, device)?));
            } else {
                self.sync(device, self.block_count)?;
            }
        } else {
            /* find child node to insert */
            for i in 0..self.entries.len() {
                if i < self.entries.len() - 1
                    && offset > self.entries[i].key
                    && offset < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    let child = fs.get_data_block(device, self.entries[i].value)?;
                    let mut child_node = if depth == 1 {
                        Self::new(self.entries[i].value, BtreeType::Leaf, &child)
                    } else {
                        Self::new(self.entries[i].value, BtreeType::Internal, &child)
                    };

                    child_node.cow_clone_node(fs, device)?;

                    /* if parted into tow sub trees */
                    if let Some((id, block)) =
                        child_node.insert_internal(fs, device, offset, block, depth - 1)?
                    {
                        self.add(id, block);
                    }

                    if self.entries.len() > MAX_INTERNAL_COUNT {
                        return Ok(Some(self.part(fs, device)?));
                    } else {
                        self.sync(device, self.block_count)?;
                    }
                }
            }
        }
        Ok(None)
    }
    /** Modify an offset from B-Tree */
    pub fn modify<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        value: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.depth == 0 {
            self.r#type = BtreeType::Leaf;
        } else {
            self.r#type = BtreeType::Internal;
        }

        self.cow_clone_node(fs, device)?;
        self.modify_internal(fs, device, key, value, self.depth as usize)?;
        Ok(())
    }
    fn modify_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        value: u64,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            /* find and modify */
            for i in 0..self.entries.len() {
                if self.entries[i].key == key {
                    self.entries[i].value = value;
                    self.entries[i].rc = 0;
                    self.sync(device, self.block_count)?;
                    break;
                }
            }
        } else {
            for i in 0..self.entries.len() {
                if i < self.entries.len() - 1
                    && key >= self.entries[i].key
                    && key < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    let child_block = fs.get_data_block(device, self.entries[i].value)?;
                    let mut child_node = if depth == 1 {
                        Self::new(self.entries[i].value, BtreeType::Leaf, &child_block)
                    } else {
                        Self::new(self.entries[i].value, BtreeType::Internal, &child_block)
                    };

                    child_node.cow_clone_node(fs, device)?;

                    child_node.modify_internal(fs, device, key, value, depth - 1)?;
                }
            }
        }
        Ok(())
    }
    /** Remove an offset from B-Tree */
    pub fn remove<D>(&mut self, fs: &mut Filesystem, device: &mut D, key: u64) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.depth == 0 {
            self.r#type = BtreeType::Leaf;
        } else {
            self.r#type = BtreeType::Internal;
        }

        self.cow_clone_node(fs, device)?;
        self.remove_internal(fs, device, key, self.depth as usize)?;
        if self.entries.len() == 1 {
            let mut child = if self.depth == 1 {
                Self::new(
                    self.entries[0].value,
                    BtreeType::Leaf,
                    &fs.get_data_block(device, self.entries[0].value)?,
                )
            } else {
                Self::new(
                    self.entries[0].value,
                    BtreeType::Internal,
                    &fs.get_data_block(device, self.entries[0].value)?,
                )
            };
            self.entries.clear();
            for i in 0..child.entries.len() {
                self.entries.push(child.entries[i]);
            }

            child.cow_release_node(fs, device)?;

            self.sync(device, self.block_count)?;
            self.depth -= 1;
        }

        Ok(())
    }
    fn remove_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.entries.len() {
                if i < self.entries.len() - 1
                    && key >= self.entries[i].key
                    && key < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    let child_block = fs.get_data_block(device, self.entries[i].value)?;
                    let mut child_node = if depth == 1 {
                        Self::new(self.entries[i].value, BtreeType::Leaf, &child_block)
                    } else {
                        Self::new(self.entries[i].value, BtreeType::Internal, &child_block)
                    };

                    child_node.cow_clone_node(fs, device)?;

                    child_node.remove_internal(fs, device, key, depth - 1)?;
                    /* when child_node is empty, self.len() must be 0 */
                    if child_node.entries.is_empty() {
                        self.entries.remove(i);
                    } else if child_node.entries.len() < MAX_INTERNAL_COUNT / 2 {
                        if i > 0 {
                            let previous_node_block =
                                fs.get_data_block(device, self.entries[i - 1].value)?;
                            let mut previous_node = if depth == 1 {
                                Self::new(
                                    self.entries[i - 1].value,
                                    BtreeType::Leaf,
                                    &previous_node_block,
                                )
                            } else {
                                Self::new(
                                    self.entries[i - 1].value,
                                    BtreeType::Internal,
                                    &previous_node_block,
                                )
                            };

                            previous_node.cow_clone_node(fs, device)?;

                            /* merge this child node into previous node */
                            if previous_node.entries.len() + child_node.entries.len()
                                <= MAX_INTERNAL_COUNT
                            {
                                for child_i in 0..child_node.entries.len() {
                                    previous_node.entries.push(child_node.entries[child_i]);
                                }

                                child_node.cow_release_node(fs, device)?;
                                self.entries.remove(i);
                            } else {
                                let id = previous_node.entries.last().unwrap().key;
                                child_node
                                    .entries
                                    .insert(0, previous_node.entries.pop().unwrap());
                                child_node.sync(device, child_node.block_count)?;
                                self.entries[i].key = id;
                            }
                            previous_node.sync(device, previous_node.block_count)?;
                        } else if i < self.entries.len() - 1 {
                            let next_node_block =
                                fs.get_data_block(device, self.entries[i + 1].value)?;
                            let mut next_node = if depth == 1 {
                                Self::new(
                                    self.entries[i + 1].value,
                                    BtreeType::Leaf,
                                    &next_node_block,
                                )
                            } else {
                                Self::new(
                                    self.entries[i + 1].value,
                                    BtreeType::Internal,
                                    &next_node_block,
                                )
                            };
                            next_node.cow_clone_node(fs, device)?;
                            /* merge this child node into next node */
                            if next_node.entries.len() + child_node.entries.len()
                                <= MAX_INTERNAL_COUNT
                            {
                                for child_i in (0..child_node.entries.len()).rev() {
                                    next_node.entries.insert(0, child_node.entries[child_i]);
                                }
                                self.entries[i + 1].key = next_node.entries.first().unwrap().key;

                                child_node.cow_release_node(fs, device)?;

                                self.entries.remove(i);
                            } else {
                                next_node.entries.remove(0);
                                child_node.entries.push(*next_node.entries.first().unwrap());
                                child_node.sync(device, child_node.block_count)?;
                                self.entries[i + 1].key = next_node.entries.first().unwrap().key;
                            }
                            next_node.sync(device, next_node.block_count)?;
                        }
                    }
                    self.sync(device, self.block_count)?;
                }
            }
        } else {
            /* find and remove */
            for i in 0..self.entries.len() {
                if self.entries[i].key == key {
                    self.entries.remove(i);
                    self.sync(device, self.block_count)?;
                    break;
                }
            }
        }
        Ok(())
    }
    /** Find pointer by id
     *
     * Return:
     * 1: block count
     */
    pub fn lookup<D>(&self, fs: &mut Filesystem, device: &mut D, key: u64) -> IOResult<BtreeEntry>
    where
        D: Write + Read + Seek,
    {
        self.lookup_internal(fs, device, key, self.depth as usize)
    }
    fn lookup_internal<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        depth: usize,
    ) -> IOResult<BtreeEntry>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.entries.len() {
                if i < self.entries.len() - 1
                    && key >= self.entries[i].key
                    && key < self.entries[i + 1].key
                    || i == self.entries.len() - 1
                {
                    let block = fs.get_data_block(device, self.entries[i].value)?;
                    let child = if depth == 1 {
                        Self::new(key, BtreeType::Leaf, &block)
                    } else {
                        Self::new(key, BtreeType::Internal, &block)
                    };

                    return child.lookup_internal(fs, device, key, depth - 1);
                }
            }
        } else {
            for entry in &self.entries {
                if key == entry.key {
                    return Ok(*entry);
                }
            }
        }
        Err(Error::new(
            ErrorKind::NotFound,
            format!("No such key '{}'.", key),
        ))
    }
    fn find_unused_internal<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        depth: usize,
    ) -> IOResult<(Option<u64>, Option<u64>)>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.entries.len() {
                let block = fs.get_data_block(device, self.entries[i].value)?;
                let child = if depth == 1 {
                    Self::new(self.entries[i].value, BtreeType::Leaf, &block)
                } else {
                    Self::new(self.entries[i].value, BtreeType::Internal, &block)
                };
                let result = child.find_unused_internal(fs, device, depth - 1)?;

                if let Some(id) = result.0 {
                    return Ok((Some(id), None));
                } else if let Some(id) = result.1 {
                    if i < self.entries.len() - 1 && id + 1 < self.entries[i + 1].key
                        || i == self.entries.len() - 1
                    {
                        return Ok((Some(id + 1), None));
                    }
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
    pub fn find_unused<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let result = self.find_unused_internal(fs, device, self.depth as usize)?;

        if let Some(id) = result.0 {
            Ok(id)
        } else if let Some(id) = result.1 {
            Ok(id)
        } else {
            Ok(0)
        }
    }
    /** Clone the full B-Tree */
    pub fn clone_tree<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.depth == 0 {
            self.r#type = BtreeType::Leaf;
        } else {
            self.r#type = BtreeType::Internal;
        }

        self.clone_tree_internal(fs, device, self.depth as usize - 1)
    }
    /** Clone the full B-Tree */
    fn clone_tree_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for entry in &mut self.entries {
                entry.rc += 1;
            }
        } else {
            for entry in &mut self.entries {
                let mut child_node = if depth == 1 {
                    Self::new(
                        entry.value,
                        BtreeType::Leaf,
                        &fs.get_data_block(device, entry.value)?,
                    )
                } else {
                    Self::new(
                        entry.value,
                        BtreeType::Internal,
                        &fs.get_data_block(device, entry.value)?,
                    )
                };
                child_node.clone_tree_internal(fs, device, depth - 1)?;
            }
        }
        self.rc += 1;
        self.sync(device, self.block_count)?;
        Ok(())
    }
    /** Destroy the full B-Tree */
    pub fn destroy<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.depth == 0 {
            self.r#type = BtreeType::Leaf;
        } else {
            self.r#type = BtreeType::Internal;
        }

        self.destroy_internal(fs, device, self.depth as usize)
    }
    fn destroy_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for entry in self.entries.iter_mut() {
                if entry.rc == 0 {
                    fs.release_block(entry.value);
                } else {
                    entry.rc -= 1;
                }
            }
        } else {
            for i in 0..self.entries.len() {
                let mut child_node = if depth == 1 {
                    Self::new(
                        self.entries[i].value,
                        BtreeType::Leaf,
                        &fs.get_data_block(device, self.entries[i].value)?,
                    )
                } else {
                    Self::new(
                        self.entries[i].value,
                        BtreeType::Internal,
                        &fs.get_data_block(device, self.entries[i].value)?,
                    )
                };
                child_node.destroy_internal(fs, device, depth - 1)?;
            }
        }

        self.cow_release_node(fs, device)?;
        Ok(())
    }
    /** Check and clone multiple referenced node */
    fn cow_clone_node<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.rc > 0 {
            self.rc -= 1;
            self.sync(device, self.block_count)?;
            self.block_count = fs.new_block()?;
            self.rc = 0;
        }
        Ok(())
    }
    /** Check and release multiple referenced node */
    fn cow_release_node<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.rc > 0 {
            self.rc -= 1;
            self.sync(device, self.block_count)?;
        } else {
            fs.release_block(self.block_count);
        }
        Ok(())
    }
}
