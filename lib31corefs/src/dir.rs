use crate::{
    Filesystem,
    file::File,
    inode::{ACL_DIRECTORY, INode, PERMISSION_BITS},
    subvol::Subvolume,
    symlink::read_link_from_inode,
    utils::{base_name, dir_path},
};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result as IOResult},
    io::{Read, Seek, Write},
    path::Path,
};

macro_rules! no_such_file {
    ($path:expr) => {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("'{}' no such file", $path),
        ))
    };
}

pub struct Directory {
    fd: File,
}

impl Directory {
    /** Create a directory */
    pub(crate) fn create<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let inode_count = create(fs, subvol, device)?;

        let mut dir = Directory::open(fs, subvol, device, dir_path(path.as_ref()))?;
        dir.add_file(fs, subvol, device, base_name(path.as_ref()), inode_count)?;

        Ok(Self {
            fd: File::open_by_inode(subvol, device, inode_count)?,
        })
    }
    pub fn open<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let mut dir = Self {
            fd: File::open_by_inode(subvol, device, subvol.entry.root_inode)?,
        };

        for file in path.as_ref().iter().skip(1) {
            let dirs = dir.list_dir(fs, subvol, device)?;

            let inode_count = match dirs.get(&file.to_string_lossy().to_string()) {
                Some(count) => *count,
                None => no_such_file!(file.to_string_lossy()),
            };
            let inode = subvol.get_inode(device, inode_count)?;

            /* read link and open orignal directory */
            if inode.is_symlink() {
                let original_path = read_link_from_inode(subvol, device, inode_count)?;
                return Self::open(fs, subvol, device, &original_path);
            } else if !inode.is_dir() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("'{}' is not a directory", file.to_string_lossy()),
                ));
            }
            dir = Self {
                fd: File::from_inode(device, inode_count, inode)?,
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
    /* Find inode under the directory */
    pub(crate) fn find_inode_by_name<D>(
        &mut self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        name: &str,
    ) -> IOResult<u64>
    where
        D: Read + Write + Seek,
    {
        match self.list_dir(fs, subvol, device)?.get(name) {
            Some(inode) => Ok(*inode),
            None => no_such_file!(name),
        }
    }
    pub fn get_inode(&self) -> INode {
        self.fd.get_inode()
    }
    /** Add file into directory */
    pub(crate) fn add_file<D>(
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
        if self.list_dir(fs, subvol, device)?.contains_key(file_name) {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!("'{}' does already esist", file_name),
            ));
        }
        let mut dir_data = Vec::new();

        dir_data.extend(inode.to_be_bytes());
        dir_data.push(file_name.len() as u8);
        dir_data.extend(file_name.as_bytes());

        self.fd
            .write(fs, subvol, device, self.fd.get_inode().size, &dir_data)
    }
    /** Remove a file into directory */
    pub(crate) fn remove_file<D>(
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
        self.fd.truncate(fs, subvol, device, dir_data.len() as u64)
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
        let mut fd = subvol.get_inode(device, inode)?;
        fd.hlinks += 1;
        subvol.set_inode(fs, device, inode, fd)?;
        self.add_file(fs, subvol, device, file_name, inode)
    }
    /** Remove a directory */
    pub(crate) fn remove<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let dir = Self::open(fs, subvol, device, &path)?;

        if dir.fd.get_inode().size > 0 {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                format!("'{}' is not empty.", path.as_ref().to_str().unwrap()),
            ))
        } else {
            remove_by_inode(fs, subvol, device, dir.fd.get_inode_number())?;
            Directory::open(fs, subvol, device, dir_path(path.as_ref()))?.remove_file(
                fs,
                subvol,
                device,
                base_name(path.as_ref()),
            )
        }
    }
}

/** Create a directory and return the inode count */
pub(crate) fn create<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_number = crate::file::create(fs, subvol, device)?;
    let mut inode = subvol.get_inode(device, inode_number)?;
    inode.acl = ACL_DIRECTORY << PERMISSION_BITS;
    subvol.set_inode(fs, device, inode_number, inode)?;
    Ok(inode_number)
}

/** Remove a directory */
pub(crate) fn remove_by_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_number: u64,
) -> IOResult<()>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(device, inode_number)?;
    if inode.size > 0 {
        Err(Error::new(
            ErrorKind::PermissionDenied,
            "Directory isn't empty",
        ))
    } else {
        crate::file::remove_by_inode(fs, subvol, device, inode_number)
    }
}
