use clap::Parser;
use lib31corefs::Filesystem;

#[derive(Parser, Debug)]
struct Args {
    /** Path to device */
    device: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let fs = Filesystem::load(&mut device)?;

    println!("Label: {}", fs.sb.get_label());
    println!("UUID: {}", uuid::Uuid::from_bytes(fs.sb.uuid));
    println!(
        "Creation time: {}",
        chrono::DateTime::from_timestamp(
            (fs.sb.creation_time / 1_000_000_000) as i64,
            (fs.sb.creation_time % 1_000_000_000) as u32
        )
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
    );
    println!("Dufault subvolume: {}", fs.sb.default_subvol);
    println!("Total blocks: {}", fs.sb.total_blocks);
    println!("Used blocks: {}", fs.sb.used_blocks);
    println!("Real used blocks: {}", fs.sb.real_used_blocks);

    Ok(())
}
