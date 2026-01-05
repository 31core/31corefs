use clap::Parser;
use fs31core::Filesystem;

#[derive(Parser, Debug)]
struct Args {
    /// Device path
    device: String,

    /// Filesystem label;
    label: Option<String>,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let mut fs = Filesystem::load(&mut device)?;

    match args.label {
        Some(label) => {
            fs.sb.set_label(label);
            fs.sync_meta_data(&mut device)?;
        }
        None => println!("{}", fs.sb.get_label()),
    }

    Ok(())
}
