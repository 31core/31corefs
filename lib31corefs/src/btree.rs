use crate::block::*;
use crate::Filesystem;
use std::io::Error;
use std::io::{ErrorKind, Read, Result as IOResult, Seek, Write};

const MAX_IDS: usize = BLOCK_SIZE / UNIT_SIZE;
const UNIT_SIZE: usize = 8 + 8;

#[derive(Default, Debug)]
pub struct BtreeNode {
    pub block_count: u64,
    pub keys: Vec<u64>,
    pub values: Vec<u64>,
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
            node.push_item(id, ptr);
        }
        node
    }
    fn dump(&self) -> [u8; BLOCK_SIZE] {
        let mut block = [0; BLOCK_SIZE];

        for (i, _) in self.keys.iter().enumerate() {
            block[UNIT_SIZE * i..UNIT_SIZE * i + 8].copy_from_slice(&self.keys[i].to_be_bytes());
            block[UNIT_SIZE * i + 8..UNIT_SIZE * i + UNIT_SIZE]
                .copy_from_slice(&self.values[i].to_be_bytes());
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
        if self.keys.is_empty() {
            self.push_item(id, ptr);
        } else {
            for (i, _) in self.keys.iter().enumerate() {
                if i == 0 && id < self.keys[0] {
                    self.insert_item(0, id, ptr);
                    break;
                } else if i < self.len() - 1 && id > self.keys[i] && id < self.keys[i + 1]
                    || i == self.len() - 1
                {
                    self.insert_item(i + 1, id, ptr);
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
        let mut another = Self::default();
        for _ in 0..self.len() / 2 {
            another.insert_item(0, self.keys.pop().unwrap(), self.values.pop().unwrap());
        }

        let another_block = fs.new_block()?;
        another.block_count = another_block;
        another.sync(device, another_block)?;
        self.sync(device, self.block_count)?;

        Ok((*another.keys.first().unwrap(), another.block_count))
    }
    /** Insert an offset into B-Tree */
    pub fn insert<D>(
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
        if let Some((id, block)) = self.insert_internal(fs, device, offset, block, depth)? {
            let mut left = Self::default();
            for i in 0..self.len() {
                left.push_item(self.keys[i], self.values[i]);
            }

            let left_block = fs.new_block()?;
            left.block_count = left_block;
            left.sync(device, left_block)?;

            self.clear();
            self.push_item(*left.keys.first().unwrap(), left_block);
            self.push_item(id, block);
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
            if self.len() > MAX_IDS {
                return Ok(Some(self.part(fs, device)?));
            } else {
                self.sync(device, self.block_count)?;
            }
        } else {
            /* find child node to insert */
            for i in 0..self.len() {
                if i < self.len() - 1 && offset > self.keys[i] && offset < self.keys[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.values[i]) {
                        self.values[i] = fs.block_copy_out(device, self.values[i])?;
                    }
                    let child = fs.get_data_block(device, self.values[i])?;
                    let mut child_node = Self::new(self.values[i], &child);

                    /* if parted into tow sub trees */
                    if let Some((id, block)) =
                        child_node.insert_internal(fs, device, offset, block, depth - 1)?
                    {
                        self.add(id, block);
                    }

                    if self.len() > MAX_IDS {
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
        depth: usize,
    ) -> IOResult<usize>
    where
        D: Write + Read + Seek,
    {
        if fs.is_multireference(self.block_count) {
            self.block_count = fs.block_copy_out(device, self.block_count)?;
        }
        self.modify_internal(fs, device, key, value, depth)?;
        Ok(depth)
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
            for i in 0..self.len() {
                if self.keys[i] == key {
                    self.values[i] = value;
                    self.sync(device, self.block_count)?;
                    break;
                }
            }
        } else {
            for i in 0..self.len() {
                if i < self.len() - 1 && key >= self.keys[i] && key < self.keys[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.values[i]) {
                        self.values[i] = fs.block_copy_out(device, self.values[i])?;
                        self.sync(device, self.block_count)?;
                    }
                    let child_block = fs.get_data_block(device, self.values[i])?;
                    let mut child_node = Self::new(self.values[i], &child_block);

                    child_node.modify_internal(fs, device, key, value, depth - 1)?;
                }
            }
        }
        Ok(())
    }
    /** Remove an offset from B-Tree */
    pub fn remove<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        depth: usize,
    ) -> IOResult<usize>
    where
        D: Write + Read + Seek,
    {
        if fs.is_multireference(self.block_count) {
            self.block_count = fs.block_copy_out(device, self.block_count)?;
        }
        self.remove_internal(fs, device, key, depth)?;
        if self.len() == 1 {
            let child = Self::new(self.values[0], &fs.get_data_block(device, self.values[0])?);
            self.clear();
            for i in 0..child.len() {
                self.push_item(child.keys[i], child.values[i]);
            }
            fs.release_block(child.block_count);
            self.sync(device, self.block_count)?;
            return Ok(depth - 1);
        }
        Ok(depth)
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
            for i in 0..self.len() {
                if i < self.len() - 1 && key >= self.keys[i] && key < self.keys[i + 1]
                    || i == self.len() - 1
                {
                    if fs.is_multireference(self.values[i]) {
                        self.values[i] = fs.block_copy_out(device, self.values[i])?;
                        self.sync(device, self.block_count)?;
                    }
                    let child_block = fs.get_data_block(device, self.values[i])?;
                    let mut child_node = Self::new(self.values[i], &child_block);

                    child_node.remove_internal(fs, device, key, depth - 1)?;
                    /* when child_node is empty, self.len() must be 0 */
                    if child_node.is_empty() {
                        self.remove_item(i);
                    } else if child_node.len() < MAX_IDS / 2 {
                        if i > 0 {
                            if fs.is_multireference(self.values[i - 1]) {
                                self.values[i - 1] =
                                    fs.block_copy_out(device, self.values[i - 1])?;
                                self.sync(device, self.block_count)?;
                            }
                            let previous_node_block =
                                fs.get_data_block(device, self.values[i - 1])?;
                            let mut previous_node =
                                Self::new(self.values[i - 1], &previous_node_block);
                            /* merge this child node into previous node */
                            if previous_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in 0..child_node.len() {
                                    previous_node.push_item(
                                        child_node.keys[child_i],
                                        child_node.values[child_i],
                                    );
                                }
                                fs.release_block(child_node.block_count);
                                self.remove_item(i);
                            } else {
                                let id = previous_node.keys.pop().unwrap();
                                let ptr = previous_node.values.pop().unwrap();
                                child_node.insert_item(0, id, ptr);
                                child_node.sync(device, child_node.block_count)?;
                                self.keys[i] = id;
                            }
                            previous_node.sync(device, previous_node.block_count)?;
                        } else if i < self.len() - 1 {
                            if fs.is_multireference(self.values[i + 1]) {
                                self.values[i + 1] =
                                    fs.block_copy_out(device, self.values[i + 1])?;
                                self.sync(device, self.block_count)?;
                            }
                            let next_node_block = fs.get_data_block(device, self.values[i + 1])?;
                            let mut next_node = Self::new(self.values[i + 1], &next_node_block);
                            /* merge this child node into next node */
                            if next_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in (0..child_node.len()).rev() {
                                    next_node.insert_item(
                                        0,
                                        child_node.keys[child_i],
                                        child_node.values[child_i],
                                    );
                                }
                                self.keys[i + 1] = *next_node.keys.first().unwrap();
                                fs.release_block(child_node.block_count);
                                self.remove_item(i);
                            } else {
                                let id = *next_node.keys.first().unwrap();
                                let ptr = *next_node.values.first().unwrap();
                                next_node.remove_item(0);
                                child_node.push_item(id, ptr);
                                child_node.sync(device, child_node.block_count)?;
                                self.keys[i + 1] = *next_node.keys.first().unwrap();
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
                if self.keys[i] == key {
                    self.remove_item(i);
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
    pub fn lookup<D>(
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        key: u64,
        depth: usize,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        if depth > 0 {
            for i in 0..self.len() {
                if i < self.len() - 1 && key >= self.keys[i] && key < self.keys[i + 1]
                    || i == self.len() - 1
                {
                    let block = fs.get_data_block(device, self.values[i])?;
                    let child = Self::new(key, &block);

                    return child.lookup(fs, device, key, depth - 1);
                }
            }
        } else {
            for i in 0..self.keys.len() {
                if key == self.keys[i] {
                    return Ok(self.values[i]);
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
            for i in 0..self.len() {
                let block = fs.get_data_block(device, self.values[i])?;
                let child = Self::new(self.values[i], &block);
                let result = child.find_unused_internal(fs, device, depth - 1)?;

                if let Some(id) = result.0 {
                    return Ok((Some(id), None));
                } else if let Some(id) = result.1 {
                    if i < self.len() - 1 && id + 1 < self.keys[i + 1] || i == self.len() - 1 {
                        return Ok((Some(id + 1), None));
                    }
                }
            }
        } else if self.len() > 1 {
            for i in 0..self.len() - 1 {
                if self.keys[i] + 1 < self.keys[i + 1] {
                    return Ok((Some(self.keys[i] + 1), None));
                }
            }
            return Ok((None, Some(*self.keys.last().unwrap() + 1)));
        } else if self.len() == 1 {
            return Ok((None, Some(*self.keys.last().unwrap() + 1)));
        }
        Ok((None, None))
    }
    /** Find unused id */
    pub fn find_unused<D>(&self, fs: &mut Filesystem, device: &mut D, depth: usize) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let result = self.find_unused_internal(fs, device, depth)?;

        if let Some(id) = result.0 {
            Ok(id)
        } else if let Some(id) = result.1 {
            Ok(id)
        } else {
            Ok(0)
        }
    }
    /** Clone the full B-Tree */
    pub fn clone_tree<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        depth: usize,
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for i in 0..self.len() {
                fs.clone_block(self.values[i]);
            }
        } else {
            for i in 0..self.keys.len() {
                let mut child_node =
                    Self::new(self.values[i], &fs.get_data_block(device, self.values[i])?);
                child_node.clone_tree(fs, device, depth - 1)?;
            }
        }
        fs.clone_block(self.block_count);
        Ok(())
    }
    /** Destroy the full B-Tree */
    pub fn destroy<D>(&mut self, fs: &mut Filesystem, device: &mut D, depth: usize) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        if depth == 0 {
            for i in 0..self.len() {
                fs.release_block(self.values[i]);
            }
        } else {
            for i in 0..self.keys.len() {
                let mut child_node =
                    Self::new(self.values[i], &fs.get_data_block(device, self.values[i])?);
                child_node.destroy(fs, device, depth - 1)?;
            }
        }

        fs.release_block(self.block_count);
        Ok(())
    }
    fn len(&self) -> usize {
        self.keys.len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn push_item(&mut self, id: u64, ptr: u64) {
        self.keys.push(id);
        self.values.push(ptr);
    }
    fn insert_item(&mut self, index: usize, id: u64, ptr: u64) {
        self.keys.insert(index, id);
        self.values.insert(index, ptr);
    }
    fn remove_item(&mut self, index: usize) {
        self.keys.remove(index);
        self.values.remove(index);
    }
    fn clear(&mut self) {
        self.keys.clear();
        self.values.clear();
    }
}
