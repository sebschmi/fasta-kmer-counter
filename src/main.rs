use std::ffi::{OsStr, OsString};

use clap::Parser;

#[derive(clap::Parser)]
struct Cli {
    #[clap(index = 1)]
    input: OsString,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let total_count = count_path(&cli.input).await;
    println!("{total_count}");
}

async fn count_path(path: &OsStr) -> usize {
    todo!()
}
