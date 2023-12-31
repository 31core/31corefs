use clap::Parser;
use lib31corefs::block::BLOCK_SIZE;
use lib31corefs::Filesystem;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Device path to format
    #[arg(short, long)]
    device: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let size = std::fs::metadata(&args.device)?.len() as usize / BLOCK_SIZE;

    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let mut fs = Filesystem::create(&mut device, size)?;

    fs.sync_meta_data(&mut device)?;

    Ok(())
}
