use aws_sdk_s3::Client;

use clap::Parser;
use dialoguer::Confirm;
use tokio::runtime::Runtime;
use color_eyre::{Result};
use tools::{log::setup_logging, s3::{size::CSVSizeReport, types::S3Location, wrapper::S3Wrapper}};

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli{
    /// Verbose mode (-v, -vv, -vvv)
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command{
    #[clap(name = "size", about = "Report on a single bucket/prefix to console")]
    Size{
        /// S3 URL
        #[clap(required = true)]
        url: String,
    },
    #[clap(name = "size-report", about = "Report on a multiple buckets/prefixes to CSV")]
    SizeReport{
        /// Comma separated S3 URLs
        #[clap(required = true, value_delimiter = ',', num_args = 1..)]
        urls: Vec<String>,

        /// CSV output file
        #[clap(short, long, default_value="bucket_usage.csv")]
        out_file: String,
    },
    #[clap(name = "destroy", about = "Delete all objects and versions under bucket/prefix")]
    Destroy{
        /// S3 URL to purge all objects and versions from
        #[arg(required = true)]
        url: String
    }
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

        match cli.command {
            Command::Destroy { url } => {
                if Confirm::new()
                    .with_prompt(format!(" Are you sure you want to destroy all objects and versions under {}?", url))
                    .default(false)
                    .interact()
                    .expect("Interaction error") {

                    println!("*** Action confirmed ");
                    let s3_location = S3Location::parse(&url)?;
                    s3.purge_all_versions_of_everything(&s3_location.bucket, &s3_location.prefix, true).await?
                } else {
                    println!("*** Action dismissed")
                }
            }
            Command::Size { url } => {
                let s3_location = S3Location::parse(&url)?;
                log::info!("Analysing: {}", &s3_location);
                let report = tools::s3::size::build_size_report(&s3_location, &s3, true).await?;
                println!("{}", report);    
            },
            Command::SizeReport { urls, out_file } => {
                let urls = urls.iter().map(|u|S3Location::parse(u)).collect::<Result<Vec<S3Location>>>()?;
                
                //Quick check to fail fast if we don't have access
                for url in &urls {
                    log::info!("Check access for {}", url);
                    let versioning_enabled = s3.is_versioning_enabled(&url.bucket).await?;
                    log::info!(" - version check result: {}", versioning_enabled);
                }
                
                let mut writer = csv::Writer::from_path(&out_file)?;
                for url in &urls {
                    log::info!("Analysing: {}", url);
                    let report = tools::s3::size::build_size_report(url, &s3, true).await?;
                    println!("Writing to {}: {}", &out_file, report);  
                    writer.serialize::<CSVSizeReport>((&report).into())?;
                    writer.flush()?;
                }
                
            },
        };

        Ok(())
    })
}
