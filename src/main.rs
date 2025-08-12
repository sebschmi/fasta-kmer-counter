use std::{
    path::{Path, PathBuf},
    pin::Pin,
};

use async_recursion::async_recursion;
use clap::Parser;
use log::{LevelFilter, debug};
use simplelog::{TermLogger, TerminalMode};
use tokio::{
    fs::{File, ReadDir, read_dir},
    io::{AsyncRead, AsyncReadExt, BufReader},
};
use tokio_stream::StreamExt;
use tokio_tar::Archive;

#[derive(clap::Parser)]
struct Cli {
    #[clap(short, long)]
    k: usize,

    #[clap(long, default_value = "info")]
    log_level: LevelFilter,

    #[clap(index = 1)]
    input: Vec<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    TermLogger::init(
        cli.log_level,
        Default::default(),
        TerminalMode::Stderr,
        Default::default(),
    )
    .unwrap();

    let mut total_count = 0;
    for input in &cli.input {
        total_count += count_path(input, cli.k).await;
    }

    println!("{total_count}");
}

async fn count_path(path: impl AsRef<Path>, k: usize) -> usize {
    let path = path.as_ref();

    let result = if let Ok(directory) = read_dir(path).await {
        count_directory(directory, k).await
    } else if let Ok(file) = File::open(path).await {
        count_file(file, k).await
    } else {
        // If everything fails, just ignore it.
        0
    };

    debug!("path {} contains {result} {k}-mers", path.display());
    result
}

#[async_recursion]
async fn count_directory(mut directory: ReadDir, k: usize) -> usize {
    let mut sum = 0;

    while let Ok(Some(entry)) = directory.next_entry().await {
        sum += count_path(entry.path(), k).await;
    }

    sum
}

#[allow(clippy::manual_unwrap_or_default, clippy::manual_unwrap_or)]
async fn count_file(mut file: impl AsyncRead + Unpin + Send, k: usize) -> usize {
    if let Some(amount) = count_tar_file(Box::new(&mut file), k).await {
        amount
    } else if let Some(amount) = count_fasta_file(&mut file, k).await {
        amount
    } else {
        // If everything fails, just ignore it.
        0
    }
}

fn count_tar_file<'file>(
    file: Box<dyn 'file + AsyncRead + Unpin + Send>,
    k: usize,
) -> Pin<Box<dyn 'file + Future<Output = Option<usize>> + Send>> {
    Box::pin(async move {
        let mut archive = Archive::new(file);
        let Ok(mut entries) = archive.entries() else {
            return None;
        };
        let mut sum = 0;

        while let Some(Ok(file)) = entries.next().await {
            sum += count_file(file, k).await;
        }

        if sum == 0 {
            // It is unclear how to discover if the file is actually a tar archive.
            // So we simply say that if the tokio-tar crate finds no file, or no file that would be fasta, then it is not a tar file.
            None
        } else {
            Some(sum)
        }
    })
}

async fn count_fasta_file(file: impl AsyncRead + Unpin + Send, k: usize) -> Option<usize> {
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut sum = 0;

    // Skip to entry header start.
    let mut last_is_newline = true;
    while let Ok(b) = reader.read_u8().await {
        if last_is_newline && b == b'>' {
            // Found entry header.
            break;
        }

        last_is_newline = b == b'\n' || b == b'\r';
    }

    loop {
        // Skip entry header.
        let mut has_characters = false;
        while let Ok(b) = reader.read_u8().await {
            has_characters = true;
            if b == b'\n' {
                break;
            }
        }

        if !has_characters {
            break;
        }

        // Count entry characters.
        let mut character_count = 0usize;
        let mut last_is_newline = true;
        while let Ok(b) = reader.read_u8().await {
            if b == b'>' {
                if last_is_newline {
                    // Found entry header.
                    break;
                } else {
                    // Found entry header character  without preceding newline.
                    return None;
                }
            }

            if b != b'\n' && b != b'\r' {
                character_count += 1;
            }

            last_is_newline = b == b'\n' || b == b'\r';
        }

        sum += character_count - k + 1;
    }

    Some(sum)
}
