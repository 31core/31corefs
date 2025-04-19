use clap::Parser;
use lib31corefs::{Filesystem, block::BLOCK_SIZE};
use std::io::{Result as IOResult, Seek};

#[derive(Parser, Debug)]
struct Args {
    /// Device path to format
    device: String,

    /// Filesystem label;
    #[arg(short = 'L', long, default_value_t = String::from(""))]
    label: String,
}

fn get_size(fd: &mut std::fs::File) -> IOResult<u64> {
    fd.seek(std::io::SeekFrom::End(0))
}

fn main() -> IOResult<()> {
    let args = Args::parse();

    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let size = get_size(&mut device)? as usize / BLOCK_SIZE;
    let mut fs = Filesystem::create(&mut device, size)?;

    fs.sb.set_label(&args.label);

    fs.sync_meta_data(&mut device)
}
