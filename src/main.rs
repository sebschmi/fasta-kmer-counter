use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
    pin::Pin,
};

use async_recursion::async_recursion;
use clap::Parser;
use log::{LevelFilter, debug};
use simplelog::{TermLogger, TerminalMode};
use tokio::{
    fs::{File, ReadDir, read_dir},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader},
};
use tokio_stream::StreamExt;
use tokio_tar::Archive;

use crate::async_file::AsyncFile;

mod async_file;

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

    if let Ok(directory) = read_dir(path).await {
        count_directory(path, directory, k).await
    } else if let Ok(file) = File::open(path).await {
        count_file(path, file, k).await
    } else {
        // If everything fails, just ignore it.
        0
    }
}

#[async_recursion]
async fn count_directory(path: &Path, mut directory: ReadDir, k: usize) -> usize {
    let mut sum = 0;

    while let Ok(Some(entry)) = directory.next_entry().await {
        sum += count_path(entry.path(), k).await;
    }

    debug!("directory {} contains {sum} {k}-mers", path.display());
    sum
}

#[allow(clippy::manual_unwrap_or_default, clippy::manual_unwrap_or)]
async fn count_file(path: &Path, mut file: impl AsyncFile, k: usize) -> usize {
    if let Some(amount) = count_tar_file(path, Box::new(&mut file), k).await {
        amount
    } else {
        if file.seek(SeekFrom::Start(0)).await.is_err() {
            return 0;
        }
        if let Some(amount) = count_fasta_file(path, &mut file, k).await {
            amount
        } else {
            // If everything fails, just ignore it.
            0
        }
    }
}

fn count_tar_file<'result>(
    path: &'result Path,
    file: Box<dyn 'result + AsyncFile>,
    k: usize,
) -> Pin<Box<dyn 'result + Future<Output = Option<usize>> + Send>> {
    Box::pin(async move {
        let mut archive = Archive::new(file);
        let Ok(mut entries) = archive.entries() else {
            debug!("file {} is not tar", path.display());
            return None;
        };
        let mut sum = 0;

        while let Some(Ok(file)) = entries.next().await {
            sum += count_fasta_file(&path.join(file.path().unwrap_or_default()), file, k)
                .await
                .unwrap_or_default();
        }

        if sum == 0 {
            // It is unclear how to discover if the file is actually a tar archive.
            // So we simply say that if the tokio-tar crate finds no file, or no file that would be fasta, then it is not a tar file.
            debug!("file {} is not tar", path.display());
            None
        } else {
            debug!("archive {} contains {sum} {k}-mers", path.display());
            Some(sum)
        }
    })
}

async fn count_fasta_file(
    path: &Path,
    file: impl AsyncRead + Unpin + Send,
    k: usize,
) -> Option<usize> {
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
            if b == b'\n' || b == b'\r' {
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
                    debug!("file {} is not fasta", path.display());
                    return None;
                }
            }

            if b != b'\n' && b != b'\r' {
                character_count += 1;
            }

            last_is_newline = b == b'\n' || b == b'\r';
        }

        sum += (character_count + 1).saturating_sub(k);
    }

    debug!("file {} contains {sum} {k}-mers", path.display());
    Some(sum)
}
