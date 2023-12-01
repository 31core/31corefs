use crate::file::File;
use crate::Filesystem;
use std::collections::HashMap;
use std::io::{Read, Result as IOResult, Seek, Write};

pub struct Directory {
    fd: File,
}

impl Directory {
    pub fn open<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> Self
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
            dir = Self {
                fd: File::open_by_inode(
                    fs,
                    device,
                    *dirs.get(&file.to_string_lossy().to_string()).unwrap(),
                )
                .unwrap(),
            };
        }

        dir
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
}
