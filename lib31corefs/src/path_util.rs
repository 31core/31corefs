use std::path::Path;

#[inline]
pub fn base_name(path: &Path) -> &str {
    path.file_name().unwrap().to_str().unwrap()
}

#[inline]
pub fn dir_path(path: &Path) -> &Path {
    path.parent().unwrap()
}
