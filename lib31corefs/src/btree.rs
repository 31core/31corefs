use crate::block::*;
use crate::Filesystem;
use std::io::{Read, Result as IOResult, Seek, Write};

const MAX_IDS: usize = BLOCK_SIZE / (8 + 8) - 1;
const UNIT_SIZE: usize = 8 + 8;
pub const BTREE_INTERNAL: u8 = 1;
pub const BTREE_LEAF: u8 = 2;

#[derive(Default, Debug)]
pub struct BtreeNode {
    pub block_count: u64,
    pub offsets: Vec<u64>,
    pub ptrs: Vec<u64>,
    pub node_type: u8,
}

impl Block for BtreeNode {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self::new_node(bytes[0]);

        let id_count = bytes[1] as usize;

        for i in 0..id_count {
            node.push(
                u64::from_be_bytes(
                    bytes[UNIT_SIZE * (i + 1)..UNIT_SIZE * (i + 1) + 8]
                        .try_into()
                        .unwrap(),
                ),
                u64::from_be_bytes(
                    bytes[UNIT_SIZE * (i + 1) + 8..UNIT_SIZE * (i + 1) + UNIT_SIZE]
                        .try_into()
                        .unwrap(),
                ),
            );
        }
        node
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];
        block[0] = self.node_type;
        block[1] = self.len() as u8;
        for (i, _) in self.offsets.iter().enumerate() {
            block[UNIT_SIZE * (i + 1)..UNIT_SIZE * (i + 1) + 8]
                .copy_from_slice(&self.offsets[i].to_be_bytes());
            block[UNIT_SIZE * (i + 1) + 8..UNIT_SIZE * (i + 1) + UNIT_SIZE]
                .copy_from_slice(&self.ptrs[i].to_be_bytes());
        }
        block
    }
}

impl BtreeNode {
    pub fn new(block_count: u64, block: &[u8; BLOCK_SIZE]) -> Self {
        let mut node = Self::load(*block);
        node.block_count = block_count;
        node
    }
    pub fn new_node(node_type: u8) -> Self {
        Self {
            node_type,
            ..Default::default()
        }
    }
    /** Add an id into the node */
    fn add(&mut self, id: u64, ptr: u64) {
        if self.offsets.is_empty() {
            self.push(id, ptr);
        } else {
            for (i, _) in self.offsets.iter().enumerate() {
                if i == 0 {
                    self.insert(0, id, ptr);
                    break;
                } else if i < self.len() - 1 && id > self.offsets[i] && id < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    self.insert(i + 1, id, ptr);
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
    fn part<D>(&mut self, fs: &mut Filesystem, device: &mut D) -> (u64, u64)
    where
        D: Write + Read + Seek,
    {
        let mut another = Self::new_node(self.node_type);
        for _ in 0..self.len() / 2 {
            another.insert(0, self.offsets.pop().unwrap(), self.ptrs.pop().unwrap());
        }

        let another_block = fs.new_block().unwrap();
        another.block_count = another_block;
        fs.set_data_block(device, another_block, another.dump())
            .unwrap();
        fs.set_data_block(device, self.block_count, self.dump())
            .unwrap();

        (*another.offsets.first().unwrap(), another.block_count)
    }
    /** Insert an offset into B-Tree */
    pub fn offset_insert<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if let Some((id, block)) = self.offset_insert_nontop(fs, device, offset, block)? {
            let mut left = Self::new_node(self.node_type);
            for i in 0..self.len() {
                left.push(self.offsets[i], self.ptrs[i]);
            }

            let left_block = fs.new_block().unwrap();
            left.block_count = left_block;
            fs.set_data_block(device, left_block, left.dump())?;

            self.clear();
            self.node_type = BTREE_INTERNAL;
            self.push(*left.offsets.first().unwrap(), left_block);
            self.push(id, block);
            fs.set_data_block(device, self.block_count, self.dump())?;
        }
        Ok(())
    }
    /** Insert an id
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node
     */
    fn offset_insert_nontop<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
    ) -> IOResult<Option<(u64, u64)>>
    where
        D: Write + Read + Seek,
    {
        if self.is_leaf() {
            self.add(offset, block);
            fs.set_data_block(device, self.block_count, self.dump())?;

            /* part into two child nodes */
            if self.len() >= MAX_IDS {
                return Ok(Some(self.part(fs, device)));
            }
        } else {
            /* find child node to insert */
            for i in 0..self.len() {
                if i < self.len() - 1 && offset > self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    let child = fs.get_data_block(device, self.ptrs[i]).unwrap();
                    let mut child_node = Self::new(offset - self.ptrs[i], &child);
                    /* if parted into tow sub trees */
                    if let Some((id, block)) =
                        child_node.offset_insert_nontop(fs, device, offset, block)?
                    {
                        self.add(id, block);
                        fs.set_data_block(device, self.block_count, self.dump())?;
                    }

                    if self.len() >= MAX_IDS {
                        return Ok(Some(self.part(fs, device)));
                    }
                }
            }
        }
        Ok(None)
    }
    /** Remove an offset from B-Tree */
    pub fn offset_remove<D>(&mut self, fs: &mut Filesystem, device: &mut D, id: u64) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if self.is_internal() {
            for i in 0..self.len() {
                if i < self.len() - 1 && id >= self.offsets[i] && id < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    let child_block = fs.get_data_block(device, self.ptrs[i]).unwrap();
                    let mut child_node = Self::new(self.ptrs[i], &child_block);
                    child_node.offset_remove(fs, device, id)?;
                    /* when child_node is empty, self.len() must be 0 */
                    if child_node.is_empty() {
                        self.remove(i);
                    } else if child_node.len() < MAX_IDS / 2 {
                        if i > 0 {
                            let previous_node_block =
                                fs.get_data_block(device, self.ptrs[i - 1]).unwrap();
                            let mut previous_node =
                                Self::new(self.ptrs[i - 1], &previous_node_block);
                            /* merge this child node into previous node */
                            if previous_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in 0..child_node.len() {
                                    previous_node.push(
                                        child_node.offsets[child_i],
                                        child_node.ptrs[child_i],
                                    );
                                }
                                fs.release_block(child_node.block_count);
                                self.remove(i);
                            } else {
                                let id = previous_node.offsets.pop().unwrap();
                                let ptr = previous_node.ptrs.pop().unwrap();
                                child_node.insert(0, id, ptr);
                                fs.set_data_block(
                                    device,
                                    child_node.block_count,
                                    child_node.dump(),
                                )?;
                                self.offsets[i] = id;
                            }
                            fs.set_data_block(
                                device,
                                previous_node.block_count,
                                previous_node.dump(),
                            )?;
                        } else if i < self.len() - 1 {
                            let next_node_block =
                                fs.get_data_block(device, self.ptrs[i + 1]).unwrap();
                            let mut next_node = Self::new(self.ptrs[i + 1], &next_node_block);
                            /* merge this child node into next node */
                            if next_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in (0..child_node.len()).rev() {
                                    next_node.insert(
                                        0,
                                        child_node.offsets[child_i],
                                        child_node.ptrs[child_i],
                                    );
                                }
                                self.offsets[i + 1] = *next_node.offsets.first().unwrap();
                                fs.release_block(child_node.block_count);
                                self.remove(i);
                            } else {
                                let id = *next_node.offsets.first().unwrap();
                                let ptr = *next_node.ptrs.first().unwrap();
                                next_node.remove(0);
                                child_node.push(id, ptr);
                                fs.set_data_block(
                                    device,
                                    child_node.block_count,
                                    child_node.dump(),
                                )?;
                                self.offsets[i + 1] = *next_node.offsets.first().unwrap();
                            }
                            fs.set_data_block(device, next_node.block_count, next_node.dump())?;
                        }
                    }
                    fs.set_data_block(device, self.block_count, self.dump())?;
                }
            }
        } else {
            /* find and remove */
            for i in 0..self.len() {
                if self.offsets[i] == id {
                    self.remove(i);
                    fs.set_data_block(device, self.block_count, self.dump())?;
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
     * 2: offset to the block
     * 3: available data size in the block
     */
    pub fn offset_lookup<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
    ) -> Option<(u64, u64, u64)>
    where
        D: Write + Read + Seek,
    {
        if self.is_internal() {
            for i in 0..self.len() {
                if i < self.len() - 1 && offset >= self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    let block = fs.get_data_block(device, self.ptrs[i]).unwrap();
                    let child = Self::new(offset - self.ptrs[i], &block);
                    return child.offset_lookup(fs, device, offset);
                }
            }
        } else {
            for i in 0..self.offsets.len() {
                if i < self.len() - 1
                    && offset >= self.offsets[i]
                    && offset < self.offsets[i + 1]
                    && offset - self.offsets[i] < BLOCK_SIZE as u64
                {
                    if self.offsets[i + 1] - self.offsets[i] > BLOCK_SIZE as u64 {
                        return Some((
                            self.ptrs[i],
                            offset - self.offsets[i],
                            BLOCK_SIZE as u64 - (offset - self.offsets[i]),
                        ));
                    } else {
                        return Some((
                            self.ptrs[i],
                            offset - self.offsets[i],
                            self.offsets[i + 1] - offset,
                        ));
                    }
                } else if i == self.len() - 1
                    && offset >= self.offsets[i]
                    && offset - self.offsets[i] < BLOCK_SIZE as u64
                {
                    return Some((
                        self.ptrs[i],
                        offset - self.offsets[i],
                        BLOCK_SIZE as u64 - (offset - self.offsets[i]),
                    ));
                }
            }
        }
        None
    }
    pub fn offset_adjust<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        size: u64,
        add: bool,
    ) where
        D: Write + Read + Seek,
    {
        if self.is_leaf() {
            for i in 0..self.len() {
                if self.offsets[i] > offset {
                    if add {
                        self.offsets[i] += size;
                    } else {
                        self.offsets[i] -= size;
                    }
                }
            }
            fs.set_data_block(device, self.block_count, self.dump())
                .unwrap();
        } else {
            for i in 0..self.offsets.len() {
                if self.offsets[i] > offset {
                    if add {
                        self.offsets[i] += size;
                    } else {
                        self.offsets[i] -= size;
                    }
                } else {
                    let mut child_node = Self::new(
                        self.ptrs[i],
                        &fs.get_data_block(device, self.ptrs[i]).unwrap(),
                    );
                    child_node.offset_adjust(fs, device, offset - self.offsets[i], size, add);
                }
            }
        }
    }
    fn is_internal(&self) -> bool {
        self.node_type == BTREE_INTERNAL
    }
    fn is_leaf(&self) -> bool {
        self.node_type == BTREE_LEAF
    }
    fn len(&self) -> usize {
        self.offsets.len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push(&mut self, id: u64, ptr: u64) {
        self.offsets.push(id);
        self.ptrs.push(ptr);
    }
    fn insert(&mut self, index: usize, id: u64, ptr: u64) {
        self.offsets.insert(index, id);
        self.ptrs.insert(index, ptr);
    }
    fn remove(&mut self, index: usize) {
        self.offsets.remove(index);
        self.ptrs.remove(index);
    }
    fn clear(&mut self) {
        self.offsets.clear();
        self.ptrs.clear();
    }
}
