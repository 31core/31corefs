use clap::{Parser, Subcommand};
use lib31corefs::{block::BLOCK_SIZE, Filesystem};

#[derive(Parser)]
struct Args {
    /// Device path to format
    device: String,

    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List subvolumes
    List,
    /// Create a subvolume
    Create,
    /// Create a snapshot
    Snap { id: u64 },
    /// Remove a subvolume
    Remove { id: u64 },
}

fn to_size_str(size: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;
    const GIB: usize = 1024 * MIB;
    const TIB: usize = 1024 * GIB;
    if size < KIB {
        format!("{} B", size)
    } else if size < MIB {
        format!("{} KiB", size / KIB)
    } else if size < GIB {
        format!("{} MiB", size / MIB)
    } else if size < TIB {
        format!("{} GiB", size / GIB)
    } else {
        format!("{} TiB", size / TIB)
    }
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let mut device = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .open(args.device)?;
    let mut fs = Filesystem::load(&mut device)?;

    match args.commands {
        Commands::Snap { id } => {
            let snap_id = fs.create_snapshot(&mut device, id)?;
            println!("Created snapshot '{}' of subvolume '{}'.", snap_id, id);
            fs.sync_meta_data(&mut device)?;
        }
        Commands::Create => {
            let id = fs.new_subvolume(&mut device)?;
            println!("Created subvolume '{}'.", id);
            fs.sync_meta_data(&mut device)?;
        }
        Commands::Remove { id } => {
            fs.remove_subvolume(&mut device, id)?;
            println!("Removed submovume '{}'.", id);
            fs.sync_meta_data(&mut device)?;
        }
        Commands::List => {
            let list = fs.list_subvolumes(&mut device)?;

            println!("+{}+{}+{}+", "-".repeat(5), "-".repeat(20), "-".repeat(8));
            println!("|{:5}|{:20}|{:8}|", "ID", "Creation Date", "Size");
            println!("+{}+{}+{}+", "-".repeat(5), "-".repeat(20), "-".repeat(8));
            for entry in list {
                println!(
                    "|{:<5}|{:20}|{:8}|",
                    entry.id,
                    chrono::DateTime::from_timestamp(entry.creation_date as i64, 0)
                        .unwrap()
                        .format("%Y-%m-%d %H:%M:%S"),
                    to_size_str(entry.real_used_blocks as usize * BLOCK_SIZE),
                );
                println!("+{}+{}+{}+", "-".repeat(5), "-".repeat(20), "-".repeat(8));
            }
        }
    }

    Ok(())
}
