use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[inline]
pub fn base_name(path: &Path) -> &str {
    path.file_name().unwrap().to_str().unwrap()
}

#[inline]
pub fn dir_path(path: &Path) -> &Path {
    path.parent().unwrap()
}

#[inline]
pub fn get_sys_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
