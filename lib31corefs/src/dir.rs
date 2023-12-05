use crate::file::File;
use crate::inode::ACL_DIRECTORY;
use crate::Filesystem;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Read, Result as IOResult, Seek, Write};

pub struct Directory {
    fd: File,
}

impl Directory {
    pub fn open<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let mut path: Vec<&std::ffi::OsStr> = std::path::Path::new(path).iter().collect();
        path.remove(0);

        let mut dir = Self {
            fd: File::open_by_inode(fs, device, fs.sb.root_inode).unwrap(),
        };

        for file in path {
            let dirs = dir.list_dir(fs, device).unwrap();
            let inode_count = *dirs.get(&file.to_string_lossy().to_string()).unwrap();
            let inode = fs.get_inode(device, inode_count)?;
            if !inode.is_dir() {
                return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
            }
            dir = Self {
                fd: File::from_inode(fs, device, inode_count, inode)?,
            };
        }

        Ok(dir)
    }
    pub fn list_dir<D>(&self, fs: &mut Filesystem, device: &mut D) -> IOResult<HashMap<String, u64>>
    where
        D: Read + Write + Seek,
    {
        let mut files: HashMap<String, u64> = HashMap::new();

        let mut dir_data = vec![0; self.fd.get_size() as usize];
        self.fd
            .read(fs, device, 0, &mut dir_data, self.fd.get_size())?;

        let mut offset = 0;
        while offset < self.fd.get_size() as usize {
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
    /** Add file into directory */
    pub fn add_file<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        file_name: &str,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        if self.list_dir(fs, device)?.get(file_name).is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("'{}' does already esist", file_name),
            ));
        }
        let mut dir_data = vec![0; self.fd.get_size() as usize];
        self.fd
            .read(fs, device, 0, &mut dir_data, self.fd.get_size())?;

        dir_data.extend(inode.to_be_bytes());
        dir_data.push(file_name.len() as u8);
        dir_data.extend(file_name.as_bytes());

        self.fd.write(fs, device, self.fd.get_size(), &dir_data)?;

        Ok(())
    }
    /** Remove a file into directory */
    pub fn remove_file<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        file_name: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut dir_data = vec![0; self.fd.get_size() as usize];
        self.fd
            .read(fs, device, 0, &mut dir_data, self.fd.get_size())?;

        let mut offset = 0;
        while offset < self.fd.get_size() as usize {
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
        self.fd.write(fs, device, 0, &dir_data)?;
        self.fd.truncate(fs, device, dir_data.len() as u64)?;

        Ok(())
    }
    /** Create a hard link into directory */
    pub fn add_hard_link<D>(
        &mut self,
        fs: &mut Filesystem,
        device: &mut D,
        inode: u64,
        file_name: &str,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut fd = fs.get_inode(device, inode)?;
        fd.hlinks += 1;
        fs.set_inode(device, inode, fd)?;
        self.add_file(fs, device, file_name, inode)?;
        Ok(())
    }
}

/** Create a directory and return the inode count */
pub fn create<D>(fs: &mut Filesystem, device: &mut D) -> Option<u64>
where
    D: Read + Write + Seek,
{
    if let Some(inode_count) = crate::file::create(fs, device) {
        let mut inode = fs.get_inode(device, inode_count).unwrap();
        inode.permission |= ACL_DIRECTORY;
        fs.set_inode(device, inode_count, inode).unwrap();
        Some(inode_count)
    } else {
        None
    }
}

/** Remove a directory */
pub fn remove<D>(fs: &mut Filesystem, device: &mut D, inode_count: u64) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let inode = fs.get_inode(device, inode_count)?;
    if inode.size > 0 {
        Err(Error::new(
            ErrorKind::PermissionDenied,
            "Directory isn't empty",
        ))
    } else {
        crate::file::remove(fs, device, inode_count)?;
        Ok(())
    }
}
