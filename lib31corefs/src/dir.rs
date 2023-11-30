use crate::inode::INode;
use crate::Filesystem;
use std::collections::HashMap;
use std::io::{Read, Result as IOResult, Seek, Write};

#[derive(Default)]
pub struct Directory {
    fd: INode,
    inode: u64,
}

impl Directory {
    pub fn open<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> Self
    where
        D: Read + Write + Seek,
    {
        let mut path_1 = std::path::Path::new(path);
        let mut path = Vec::new();

        if let Some(parent) = path_1.file_stem() {
            path.insert(0, parent.to_str().unwrap().to_string());
        }

        while let Some(parent) = path_1.parent() {
            path.insert(0, parent.to_str().unwrap().to_string());
            path_1 = parent;
        }
        if !path.is_empty() {
            path.remove(0);
        }

        let mut dir = Self {
            fd: fs.get_inode(device, fs.sb.root_inode).unwrap(),
            inode: fs.sb.root_inode,
        };
        for file in path {
            let dirs = dir.list_dir(fs, device).unwrap();
            dir = Self {
                fd: fs.get_inode(device, *dirs.get(&file).unwrap()).unwrap(),
                inode: *dirs.get(&file).unwrap(),
            };
        }

        dir
    }
    pub fn list_dir<D>(&self, fs: &mut Filesystem, device: &mut D) -> IOResult<HashMap<String, u64>>
    where
        D: Read + Write + Seek,
    {
        let mut files: HashMap<String, u64> = HashMap::new();

        let mut dir_data = vec![0; self.fd.size as usize];
        crate::file::File::open_by_inode(fs, device, self.inode)?.read(
            fs,
            device,
            0,
            &mut dir_data,
            self.fd.size,
        )?;

        let mut offset = 0;
        while offset < self.fd.size as usize {
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
        &self,
        fs: &mut Filesystem,
        device: &mut D,
        file_name: &str,
        inode: u64,
    ) -> IOResult<()>
    where
        D: Read + Write + Seek,
    {
        let mut dir_data = vec![0; self.fd.size as usize];
        crate::file::File::open_by_inode(fs, device, self.inode)?.read(
            fs,
            device,
            0,
            &mut dir_data,
            self.fd.size,
        )?;

        dir_data.extend(inode.to_be_bytes());
        dir_data.push(file_name.len() as u8);
        dir_data.extend(file_name.as_bytes());

        crate::file::File::open_by_inode(fs, device, self.inode)?.write(
            fs,
            device,
            self.fd.size,
            &dir_data,
        )?;

        Ok(())
    }
}
