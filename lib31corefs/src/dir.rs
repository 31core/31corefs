use crate::file::File;
use crate::inode::{INode, ACL_DIRECTORY};
use crate::subvol::Subvolume;
use crate::Filesystem;
use crate::{base_name, dir_name};

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, Write};

pub struct Directory {
    fd: File,
}

impl Directory {
    /** Create a directory */
    pub fn create<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let inode_count = create(fs, subvol, device)?;

        let mut dir = Directory::open(fs, subvol, device, &dir_name!(path))?;
        dir.add_file(fs, subvol, device, &base_name!(path), inode_count)?;

        Ok(Self {
            fd: File::open_by_inode(fs, subvol, device, inode_count)?,
        })
    }
    pub fn open<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let mut path: Vec<&std::ffi::OsStr> = std::path::Path::new(path).iter().collect();
        path.remove(0);

        let mut dir = Self {
            fd: File::open_by_inode(fs, subvol, device, 0)?,
        };

        for file in path {
            let dirs = dir.list_dir(fs, subvol, device).unwrap();

            let inode_count;
            match dirs.get(&file.to_string_lossy().to_string()) {
                Some(count) => inode_count = *count,
                None => {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        format!("'{}' no such file", file.to_string_lossy()),
                    ))
                }
            }
            let inode = subvol.get_inode(fs, device, inode_count)?;

            /* read link and open orignal directory */
            if inode.is_symlink() {
                let mut symlink = File::open_by_inode(fs, subvol, device, inode_count)?;
                let original_path = symlink.read_link(fs, subvol, device)?;
                return Self::open(fs, subvol, device, &original_path);
            } else if !inode.is_dir() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("'{}' is not a directory", file.to_string_lossy()),
                ));
            }
            dir = Self {
                fd: File::from_inode(fs, device, inode_count, inode)?,
            };
        }

        Ok(dir)
    }
    pub fn list_dir<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
    ) -> IOResult<HashMap<String, u64>>
    where
        D: Read + Write + Seek,
    {
        let mut files: HashMap<String, u64> = HashMap::new();

        let mut dir_data = vec![0; self.fd.get_inode().size as usize];
        self.fd.read(
            fs,
            subvol,
            device,
            0,
            &mut dir_data,
            self.fd.get_inode().size,
        )?;

        let mut offset = 0;
        while offset < self.fd.get_inode().size as usize {
            let inode = u64::from_be_bytes(dir_data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let str_len = dir_data[offset] as usize;
            offset += 1;
            let file_name =
                String::from_utf8_lossy(&dir_data[offset..offset + str_len]).to_string();
            offset += str_len;
            files.insert(file_name, inode);
        }

        Ok(files)
    }
    pub fn get_inode(&self) -> INode {
        self.fd.get_inode()
    }
    /** Add file into directory */
    pub fn add_file<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        file_name: &str,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        if self.list_dir(fs, subvol, device)?.get(file_name).is_some() {
            return Err(Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("'{}' does already esist", file_name),
            ));
        }
        let mut dir_data = Vec::new();

        dir_data.extend(inode.to_be_bytes());
        dir_data.push(file_name.len() as u8);
        dir_data.extend(file_name.as_bytes());

        self.fd
            .write(fs, subvol, device, self.fd.get_inode().size, &dir_data)?;

        Ok(())
    }
    /** Remove a file into directory */
    pub fn remove_file<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        file_name: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut dir_data = vec![0; self.fd.get_inode().size as usize];
        self.fd.read(
            fs,
            subvol,
            device,
            0,
            &mut dir_data,
            self.fd.get_inode().size,
        )?;

        let mut offset = 0;
        while offset < self.fd.get_inode().size as usize {
            offset += 8;
            let str_len = dir_data[offset] as usize;
            offset += 1;
            let this_file_name =
                String::from_utf8_lossy(&dir_data[offset..offset + str_len]).to_string();
            offset += str_len;

            if this_file_name == file_name {
                for _ in 0..str_len + 8 + 1 {
                    dir_data.remove(offset - str_len - 8 - 1);
                }
                break;
            }
        }
        self.fd.write(fs, subvol, device, 0, &dir_data)?;
        self.fd
            .truncate(fs, subvol, device, dir_data.len() as u64)?;

        Ok(())
    }
    /** Create a hard link into directory */
    pub fn add_hard_link<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        inode: u64,
        file_name: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut fd = subvol.get_inode(fs, device, inode)?;
        fd.hlinks += 1;
        subvol.set_inode(fs, device, inode, fd)?;
        self.add_file(fs, subvol, device, file_name, inode)?;
        Ok(())
    }
    /** Remove a directory */
    pub fn remove<D>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let dir = Self::open(fs, subvol, device, path)?;

        if dir.fd.get_inode().size > 0 {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                format!("'{}' is not empty.", path),
            ))
        } else {
            remove_by_inode(fs, subvol, device, dir.fd.get_inode_count())?;
            Directory::open(fs, subvol, device, &dir_name!(path))?.remove_file(
                fs,
                subvol,
                device,
                &base_name!(path),
            )?;
            Ok(())
        }
    }
}

/** Create a directory and return the inode count */
pub fn create<D>(fs: &mut Filesystem, subvol: &mut Subvolume, device: &mut D) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = crate::file::create(fs, subvol, device)?;
    let mut inode = subvol.get_inode(fs, device, inode_count)?;
    inode.permission = ACL_DIRECTORY;
    subvol.set_inode(fs, device, inode_count, inode)?;
    Ok(inode_count)
}

/** Remove a directory */
pub fn remove_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(fs, device, inode_count)?;
    if inode.size > 0 {
        Err(Error::new(
            ErrorKind::PermissionDenied,
            "Directory isn't empty",
        ))
    } else {
        crate::file::remove_by_inode(fs, subvol, device, inode_count)?;
        Ok(())
    }
}
