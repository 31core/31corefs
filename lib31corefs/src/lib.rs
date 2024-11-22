pub mod block;
pub mod inode;

mod btree;
mod dir;
mod file;
mod path_util;
mod subvol;
mod symlink;

pub use dir::Directory;
pub use file::File;
pub use subvol::Subvolume;

use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use block::{Block, BlockGroup, SuperBlock};
use path_util::{base_name, dir_path};
use subvol::{SubvolumeEntry, SubvolumeManager};

pub const FS_MAGIC_HEADER: [u8; 4] = [0x31, 0xc0, 0x8e, 0xf5];
pub const FS_VERSION: u8 = 1;

#[derive(Debug, Default, Clone)]
pub struct Filesystem {
    pub sb: SuperBlock,
    groups: Vec<BlockGroup>,
}

impl Filesystem {
    pub fn create<D>(device: &mut D, block_size: usize) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        const BLOCK_GROUP_MINIMAL_SZIE: usize = 3;

        let mut fs = Self::default();
        fs.sb.uuid = *uuid::Uuid::new_v4().as_bytes();
        fs.sb.total_blocks = block_size as u64;

        let mut group_start = 1;
        while group_start <= (block_size - BLOCK_GROUP_MINIMAL_SZIE) as u64 {
            let mut group = BlockGroup::create(group_start, block_size as u64 - group_start);
            group.meta_data.id = fs.groups.len() as u64;

            group_start += group.blocks();
            fs.groups.push(group);
        }

        fs.sb.groups = fs.groups.len() as u64;
        fs.sb.subvol_mgr = SubvolumeManager::allocate_on_block(&mut fs, device)?;
        fs.sb.creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        fs.sb.default_subvol = fs.new_subvolume(device)?;

        Ok(fs)
    }
    pub fn load<D>(device: &mut D) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let sb = SuperBlock::load(block::load_block(device, 0)?);

        let mut groups = Vec::new();

        let mut group_start = 1;
        loop {
            let mut group = BlockGroup {
                start_block: group_start,
                ..Default::default()
            };
            group.load(device)?;
            group_start = group.meta_data.next_group;

            groups.push(group);

            if group_start == 0 {
                break;
            }
        }

        Ok(Self { sb, groups })
    }
    /** Allocate a data block */
    pub(crate) fn new_block(&mut self) -> IOResult<u64> {
        for group in &mut self.groups {
            if let Some(count) = group.new_block() {
                self.sb.used_blocks += 1;
                self.sb.real_used_blocks += 1;
                return Ok(group.to_absolute_block(count));
            }
        }
        Err(Error::new(ErrorKind::Other, "No enough block"))
    }
    /** Release a data block */
    pub(crate) fn release_block(&mut self, count: u64) {
        let mut group_count = 0;
        while !(group_count < self.groups.len() - 2
            && count > self.groups[group_count].start_block
            && count < self.groups[group_count + 1].start_block)
        {
            group_count += 1;
        }

        let relative_count = self.groups[group_count].to_relative_block(count);
        self.groups[group_count].release_block(relative_count);
        self.sb.used_blocks -= 1;
        self.sb.real_used_blocks -= 1;
    }
    /** Synchronize meta data to disk */
    pub fn sync_meta_data<D>(&mut self, device: &mut D) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        self.sb.sync(device, 0)?;
        for group in &mut self.groups {
            group.sync(device)?;
        }

        Ok(())
    }
    /** Create a subvolume and return it's ID */
    pub fn new_subvolume<D>(&mut self, device: &mut D) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        let subvol_mgr = self.sb.subvol_mgr;
        SubvolumeManager::new_subvolume(self, device, subvol_mgr)
    }
    pub fn remove_subvolume<D>(&mut self, device: &mut D, id: u64) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let subvol_mgr = self.sb.subvol_mgr;
        SubvolumeManager::remove_subvolume(self, device, subvol_mgr, id)
    }
    pub fn get_subvolume<D>(&self, device: &mut D, id: u64) -> IOResult<Subvolume>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::get_subvolume(device, self.sb.subvol_mgr, id)
    }
    pub fn get_default_subvolume<D>(&self, device: &mut D) -> IOResult<Subvolume>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::get_subvolume(device, self.sb.subvol_mgr, self.sb.default_subvol)
    }
    /** Create a snapshot and return it's ID */
    pub fn create_snapshot<D>(&mut self, device: &mut D, id: u64) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::create_snapshot(self, device, self.sb.subvol_mgr, id)
    }
    /** List submolumes */
    pub fn list_subvolumes<D>(&mut self, device: &mut D) -> IOResult<Vec<SubvolumeEntry>>
    where
        D: Read + Write + Seek,
    {
        SubvolumeManager::list_subvols(device, self.sb.subvol_mgr)
    }
    /** Create a regular file */
    pub fn create_file<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<File>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        File::create(self, subvol, device, path)
    }
    /** Open a regular file */
    pub fn open_file<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<File>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        File::open(self, subvol, device, path)
    }
    /** Remove a regular file or a symbol link */
    pub fn remove_file<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        File::remove(self, subvol, device, path)
    }
    pub fn is_file<D, P>(&mut self, subvol: &mut Subvolume, device: &mut D, path: P) -> bool
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        File::open(self, subvol, device, path.as_ref()).is_ok()
    }
    pub fn is_dir<D, P>(&mut self, subvol: &mut Subvolume, device: &mut D, path: P) -> bool
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        Directory::open(self, subvol, device, path).is_ok()
    }
    pub fn is_link<D, P>(&mut self, subvol: &mut Subvolume, device: &mut D, path: P) -> bool
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        if let Ok(fd) = file::File::open(self, subvol, device, path.as_ref()) {
            if fd.get_inode().is_symlink() {
                return true;
            }
        }

        false
    }
    /** List a diretory */
    pub fn list_dir<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Vec<String>>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        Ok(Directory::open(self, subvol, device, path)?
            .list_dir(self, subvol, device)?
            .keys()
            .cloned()
            .collect::<Vec<String>>())
    }
    /** Create a directory */
    pub fn mkdir<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Directory>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        Directory::create(self, subvol, device, path)
    }
    /** Remove a directory */
    pub fn rmdir<D, P>(&mut self, subvol: &mut Subvolume, device: &mut D, path: P) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        Directory::remove(self, subvol, device, path)
    }
    /** Create sybmol link */
    pub fn link<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
        point_to: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        symlink::create(self, subvol, device, path, point_to)?;

        Ok(())
    }
    pub fn read_link<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<PathBuf>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        symlink::read_link(self, subvol, device, path)
    }
    /** Rename a regular file, directory or a symbol link */
    pub fn rename<D, P>(
        &mut self,
        subvol: &mut Subvolume,
        device: &mut D,
        src: P,
        dst: P,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let mut src_dir = Directory::open(self, subvol, device, dir_path(src.as_ref()))?;
        let inode = *src_dir
            .list_dir(self, subvol, device)?
            .get(base_name(src.as_ref()))
            .unwrap();
        src_dir.remove_file(self, subvol, device, base_name(src.as_ref()))?;

        Directory::open(self, subvol, device, dir_path(dst.as_ref()))?.add_file(
            self,
            subvol,
            device,
            base_name(dst.as_ref()),
            inode,
        )?;

        Ok(())
    }
}
