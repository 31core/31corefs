use crate::block::LinkedContentTable;
use crate::dir::Directory;
use crate::inode::{INode, ACL_SYMBOLLINK};
use crate::subvol::Subvolume;
use crate::{base_name, dir_name, Block, Filesystem};

use std::io::Result as IOResult;
use std::io::{Read, Seek, Write};

/** Create a symbol link */
pub fn create<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    path: &str,
    mut point_to: &str,
) -> IOResult<u64>
where
    D: Read + Write + Seek,
{
    let inode_count = subvol.new_inode(fs, device)?;

    let mut content_ptr = LinkedContentTable::allocate_on_block_subvol(fs, subvol, device)?;
    let inode = INode {
        permission: ACL_SYMBOLLINK,
        btree_root: content_ptr,
        ..Default::default()
    };

    loop {
        let mut lct = LinkedContentTable::default();
        let size = std::cmp::min(point_to.len(), lct.data.len());
        lct.data[..size].copy_from_slice(point_to[..size].as_bytes());
        point_to = &point_to[size..];

        if point_to.is_empty() {
            lct.sync(device, content_ptr)?;
            break;
        } else {
            content_ptr = subvol.new_block(fs, device)?;
            lct.next = content_ptr;
            lct.sync(device, content_ptr)?;
        }
    }

    subvol.set_inode(fs, device, inode_count, inode)?;

    let mut dir = Directory::open(fs, subvol, device, &dir_name!(path))?;
    dir.add_file(fs, subvol, device, &base_name!(path), inode_count)?;

    Ok(inode_count)
}

/** Read symbol link */
pub fn read_link<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    path: &str,
) -> IOResult<String>
where
    D: Read + Write + Seek,
{
    let inode_count = Directory::open(fs, subvol, device, &dir_name!(path))?.find_inode_by_name(
        fs,
        subvol,
        device,
        &base_name!(path),
    )?;

    read_link_from_inode(fs, subvol, device, inode_count)
}

/** Read symbol link by inode count */
pub(crate) fn read_link_from_inode<D>(
    fs: &mut Filesystem,
    subvol: &mut Subvolume,
    device: &mut D,
    inode_count: u64,
) -> IOResult<String>
where
    D: Read + Write + Seek,
{
    let inode = subvol.get_inode(fs, device, inode_count)?;

    let mut point_to = String::new();
    let mut content_ptr = inode.btree_root;
    'main: loop {
        let lct = LinkedContentTable::load(fs.get_data_block(device, content_ptr)?);

        for byte in lct.data {
            if byte == 0 {
                break 'main;
            } else {
                point_to.push(byte as char);
            }
        }

        content_ptr = lct.next;
    }

    Ok(point_to)
}
