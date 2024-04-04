use clap::{Parser, Subcommand};
use lib31corefs::Filesystem;

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
        }
        Commands::Create => {
            let id = fs.new_subvolume(&mut device)?;
            println!("Created subvolume '{}'.", id);
        }
        Commands::Remove { id } => {
            fs.remove_subvolume(&mut device, id)?;
            println!("Removed submovume '{}'.", id);
        }
        Commands::List => {
            let list = fs.list_subvolumes(&mut device)?;

            println!("|{:5}|{:20}|", "ID", "Creation Date");
            for entry in list {
                println!(
                    "|{:5}|{:20}|",
                    entry.id,
                    chrono::DateTime::from_timestamp(entry.creation_date as i64, 0)
                        .unwrap()
                        .format("%Y-%m-%d %H-%M-%S")
                );
            }
        }
    }

    Ok(())
}
