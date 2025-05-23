use aws_sdk_s3::Client;
use clap::Parser;
use color_eyre::{eyre::Error, Result};
use tokio::runtime::Runtime;
use tools::{log::setup_logging, s3::wrapper::S3Wrapper};

pub use tools as this_crate;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// Verbose mode (-v, -vv, -vvv)
    #[structopt(short, long, action = clap::ArgAction::Count, default_value="1")]
    verbose: u8,

    /// Bucket
    #[structopt(long)]
    bucket: String,

    /// Prefix
    #[structopt(long, default_value="")]
    prefix: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    setup_logging(cli.verbose);
    let runtime = Runtime::new().unwrap();

    runtime.block_on(async {
        let config = aws_config::load_from_env().await;

        let s3 = S3Wrapper{
            client: Client::new(&config),
        };

        s3.purge_all_versions_of_everything(&cli.bucket, &cli.prefix, true).await?;

        Ok::<(),Error>(())
    })?;

    Ok(())
}