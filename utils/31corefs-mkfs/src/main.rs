use clap::Parser;
use lib31corefs::block::BLOCK_SIZE;
use lib31corefs::Filesystem;

#[derive(Parser, Debug)]
struct Args {
    /// Device path to format
    device: String,

    /// Filesystem label;
    #[arg(short = 'L', long, default_value_t = String::from(""))]
    label: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let size = std::fs::metadata(&args.device)?.len() as usize / BLOCK_SIZE;

    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let mut fs = Filesystem::create(&mut device, size)?;

    fs.sb.set_label(&args.label);

    fs.sync_meta_data(&mut device)?;

    Ok(())
}
