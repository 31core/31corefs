use crate::block::*;
use crate::Filesystem;
use std::io::{Read, Result as IOResult, Seek, Write};

const MAX_IDS: usize = BLOCK_SIZE / UNIT_SIZE;
const UNIT_SIZE: usize = 8 + 8;

#[derive(Default, Debug)]
pub struct BtreeNode {
    pub block_count: u64,
    pub offsets: Vec<u64>,
    pub ptrs: Vec<u64>,
}

impl Block for BtreeNode {
    fn load(bytes: [u8; BLOCK_SIZE]) -> Self {
        let mut node = Self::default();

        for i in 0..MAX_IDS {
            let id =
                u64::from_be_bytes(bytes[UNIT_SIZE * i..UNIT_SIZE * i + 8].try_into().unwrap());
            let ptr = u64::from_be_bytes(
                bytes[UNIT_SIZE * i + 8..UNIT_SIZE * i + UNIT_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if ptr == 0 {
                break;
            }
            node.push(id, ptr);
        }
        node
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];

        for (i, _) in self.offsets.iter().enumerate() {
            block[UNIT_SIZE * i..UNIT_SIZE * i + 8].copy_from_slice(&self.offsets[i].to_be_bytes());
            block[UNIT_SIZE * i + 8..UNIT_SIZE * i + UNIT_SIZE]
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
    /** Add an id into the node */
    fn add(&mut self, id: u64, ptr: u64) {
        if self.offsets.is_empty() {
            self.push(id, ptr);
        } else {
            for (i, _) in self.offsets.iter().enumerate() {
                if i == 0 && id < self.offsets[0] {
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
        let mut another = Self::default();
        for _ in 0..self.len() / 2 {
            another.insert(0, self.offsets.pop().unwrap(), self.ptrs.pop().unwrap());
        }

        let another_block = fs.new_block().unwrap();
        another.block_count = another_block;
        another.sync(device, another_block).unwrap();
        self.sync(device, self.block_count).unwrap();

        (*another.offsets.first().unwrap(), another.block_count)
    }
    /** Insert an offset into B-Tree */
    pub fn offset_insert<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
        depth: usize,
    ) -> IOResult<usize>
    where
        D: Write + Read + Seek,
    {
        if fs.is_multireference(self.block_count) {
            self.block_count = fs.block_copy_out(device, self.block_count)?;
        }
        if let Some((id, block)) = self.offset_insert_internal(fs, device, offset, block, depth)? {
            let mut left = Self::default();
            for i in 0..self.len() {
                left.push(self.offsets[i], self.ptrs[i]);
            }

            let left_block = fs.new_block().unwrap();
            left.block_count = left_block;
            left.sync(device, left_block)?;

            self.clear();
            self.push(*left.offsets.first().unwrap(), left_block);
            self.push(id, block);
            self.sync(device, self.block_count)?;

            Ok(depth + 1)
        } else {
            Ok(depth)
        }
    }
    /** Insert an id
     *
     * Return:
     * * node ID of the right node
     * * block count of the right node
     */
    fn offset_insert_internal<D>(
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
            if self.len() > MAX_IDS {
                return Ok(Some(self.part(fs, device)));
            } else {
                self.sync(device, self.block_count)?;
            }
        } else {
            /* find child node to insert */
            for i in 0..self.len() {
                if i < self.len() - 1 && offset > self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.ptrs[i]) {
                        self.ptrs[i] = fs.block_copy_out(device, self.ptrs[i])?;
                    }
                    let child = fs.get_data_block(device, self.ptrs[i]).unwrap();
                    let mut child_node = Self::new(self.ptrs[i], &child);

                    /* if parted into tow sub trees */
                    if let Some((id, block)) =
                        child_node.offset_insert_internal(fs, device, offset, block, depth - 1)?
                    {
                        self.add(id, block);
                    }

                    if self.len() > MAX_IDS {
                        return Ok(Some(self.part(fs, device)));
                    } else {
                        self.sync(device, self.block_count)?;
                    }
                }
            }
        }
        Ok(None)
    }
    /** Modify an offset from B-Tree */
    pub fn offset_modify<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
        depth: usize,
    ) -> IOResult<usize>
    where
        D: Write + Read + Seek,
    {
        if fs.is_multireference(self.block_count) {
            self.block_count = fs.block_copy_out(device, self.block_count)?;
        }
        self.offset_modify_internal(fs, device, offset, block, depth)?;
        Ok(depth)
    }
    fn offset_modify_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        block: u64,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            /* find and modify */
            for i in 0..self.len() {
                if self.offsets[i] == offset {
                    self.ptrs[i] = block;
                    self.sync(device, self.block_count)?;
                    break;
                }
            }
        } else {
            for i in 0..self.len() {
                if i < self.len() - 1 && offset >= self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.ptrs[i]) {
                        self.ptrs[i] = fs.block_copy_out(device, self.ptrs[i])?;
                        self.sync(device, self.block_count)?;
                    }
                    let child_block = fs.get_data_block(device, self.ptrs[i])?;
                    let mut child_node = Self::new(self.ptrs[i], &child_block);

                    child_node.offset_modify_internal(fs, device, offset, block, depth - 1)?;
                }
            }
        }
        Ok(())
    }
    /** Remove an offset from B-Tree */
    pub fn offset_remove<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        depth: usize,
    ) -> IOResult<usize>
    where
        D: Write + Read + Seek,
    {
        if fs.is_multireference(self.block_count) {
            self.block_count = fs.block_copy_out(device, self.block_count)?;
        }
        self.offset_remove_internal(fs, device, offset, depth)?;
        if self.len() == 1 {
            let child = Self::new(self.ptrs[0], &fs.get_data_block(device, self.ptrs[0])?);
            self.clear();
            for i in 0..child.len() {
                self.push(child.offsets[i], child.ptrs[i]);
            }
            fs.release_block(child.block_count);
            self.sync(device, self.block_count)?;
            return Ok(depth - 1);
        }
        Ok(depth)
    }
    fn offset_remove_internal<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.len() {
                if i < self.len() - 1 && offset >= self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.ptrs[i]) {
                        self.ptrs[i] = fs.block_copy_out(device, self.ptrs[i])?;
                        self.sync(device, self.block_count)?;
                    }
                    let child_block = fs.get_data_block(device, self.ptrs[i])?;
                    let mut child_node = Self::new(self.ptrs[i], &child_block);

                    child_node.offset_remove_internal(fs, device, offset, depth - 1)?;
                    /* when child_node is empty, self.len() must be 0 */
                    if child_node.is_empty() {
                        self.remove(i);
                    } else if child_node.len() < MAX_IDS / 2 {
                        if i > 0 {
                            if fs.is_multireference(self.ptrs[i - 1]) {
                                self.ptrs[i - 1] = fs.block_copy_out(device, self.ptrs[i - 1])?;
                                self.sync(device, self.block_count)?;
                            }
                            let previous_node_block =
                                fs.get_data_block(device, self.ptrs[i - 1])?;
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
                                child_node.sync(device, child_node.block_count)?;
                                self.offsets[i] = id;
                            }
                            previous_node.sync(device, previous_node.block_count)?;
                        } else if i < self.len() - 1 {
                            if fs.is_multireference(self.ptrs[i + 1]) {
                                self.ptrs[i + 1] = fs.block_copy_out(device, self.ptrs[i + 1])?;
                                self.sync(device, self.block_count)?;
                            }
                            let next_node_block = fs.get_data_block(device, self.ptrs[i + 1])?;
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
                                child_node.sync(device, child_node.block_count)?;
                                self.offsets[i + 1] = *next_node.offsets.first().unwrap();
                            }
                            next_node.sync(device, next_node.block_count)?;
                        }
                    }
                    self.sync(device, self.block_count)?;
                }
            }
        } else {
            /* find and remove */
            for i in 0..self.len() {
                if self.offsets[i] == offset {
                    self.remove(i);
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
    pub fn offset_lookup<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        offset: u64,
        depth: usize,
    ) -> Option<u64>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.len() {
                if i < self.len() - 1 && offset >= self.offsets[i] && offset < self.offsets[i + 1]
                    || i == self.len() - 1
                {
                    let block = fs.get_data_block(device, self.ptrs[i]).unwrap();
                    let child = Self::new(offset, &block);

                    return child.offset_lookup(fs, device, offset, depth - 1);
                }
            }
        } else {
            for i in 0..self.offsets.len() {
                if offset == self.offsets[i] {
                    return Some(self.ptrs[i]);
                }
            }
        }
        None
    }
    fn find_unused_internal<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        depth: usize,
    ) -> (Option<u64>, Option<u64>)
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.len() {
                let block = fs.get_data_block(device, self.ptrs[i]).unwrap();
                let child = Self::new(self.ptrs[i], &block);
                let result = child.find_unused_internal(fs, device, depth - 1);

                if let Some(id) = result.0 {
                    return (Some(id), None);
                } else if let Some(id) = result.1 {
                    if i < self.len() - 1 && id + 1 < self.offsets[i + 1] || i == self.len() - 1 {
                        return (Some(id + 1), None);
                    }
                }
            }
        } else if self.len() > 1 {
            for i in 0..self.len() - 1 {
                if self.offsets[i] + 1 < self.offsets[i + 1] {
                    return (Some(self.offsets[i] + 1), None);
                }
            }
            return (None, Some(*self.offsets.last().unwrap() + 1));
        } else if self.len() == 1 {
            return (None, Some(*self.offsets.last().unwrap() + 1));
        }
        (None, None)
    }
    /** Find unused id */
    pub fn find_unused<D>(&self, fs: &mut Filesystem, device: &mut D, depth: usize) -> u64
    where
        D: Write + Read + Seek,
    {
        let result = self.find_unused_internal(fs, device, depth);

        if let Some(id) = result.0 {
            id
        } else if let Some(id) = result.1 {
            id
        } else {
            0
        }
    }
    /** Clone the full B-Tree */
    pub fn clone_tree<D>(&mut self, fs: &mut Filesystem, device: &mut D, depth: usize)
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for i in 0..self.len() {
                fs.clone_block(self.ptrs[i]);
            }
            fs.clone_block(self.block_count);
        } else {
            for i in 0..self.offsets.len() {
                let mut child_node = Self::new(
                    self.ptrs[i],
                    &fs.get_data_block(device, self.ptrs[i]).unwrap(),
                );
                child_node.clone_tree(fs, device, depth - 1);
            }
            fs.clone_block(self.block_count);
        }
    }
    /** Destroy the full B-Tree */
    pub fn destroy<D>(&mut self, fs: &mut Filesystem, device: &mut D, depth: usize)
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for i in 0..self.len() {
                fs.release_block(self.ptrs[i]);
            }
            fs.release_block(self.block_count);
        } else {
            for i in 0..self.offsets.len() {
                let mut child_node = Self::new(
                    self.ptrs[i],
                    &fs.get_data_block(device, self.ptrs[i]).unwrap(),
                );
                child_node.destroy(fs, device, depth - 1);
            }
            fs.release_block(self.block_count);
        }
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
