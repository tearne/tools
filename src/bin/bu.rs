use std::{borrow::Borrow, collections::HashSet, fmt::Display};

use aws_sdk_s3::{operation::{get_object_attributes::GetObjectAttributesOutput, list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output}, types::{BucketVersioningStatus, Delete, Object, ObjectIdentifier, ObjectVersion}, Client};
use bytesize::ByteSize;
use clap::Parser;
use tokio::runtime::{Handle, Runtime};
use color_eyre::{eyre::OptionExt, Result};
use tools::{log::setup_logging, s3::S3Wrapper};

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli {
    /// Verbose mode (-v, -vv, -vvv)
    #[structopt(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Bucket
    #[structopt(short, long)]
    bucket: String,

    /// Prefix
    #[structopt(short, long, default_value="")]
    prefix: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    setup_logging(cli.verbose);
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle().clone();

    runtime.block_on(async {
        let config = aws_config::load_from_env().await;

        let s3 = S3Wrapper{
            handle,
            client: Client::new(&config),
        };

        do_stuff(&cli.bucket, &cli.prefix, &s3).await.unwrap();    
    });

    Ok(())
}

#[derive(Debug)]
struct Stats {
    num_objects: usize,
    total_size: ByteSize,
}
impl Stats {
    fn from<T: Borrow<ObjectVersion>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.unwrap()).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            total_size: size,
        }
    }
}

struct Report {
    url: String,
    total: Stats,
    current: Stats,
    current_versions: Stats,
    orphaned_versions: Stats,
}

async fn do_stuff(bucket: &str, prefix: &str, s3: &S3Wrapper) -> Result<()> {
    println!("{bucket}/{prefix}");

    // let objects = s3.list_objects_v2(bucket, prefix).await.unwrap();
    // let size = ByteSize::b(objects.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64);
    // println!(" * {} across {} objects", size, objects.len());
    // println!("---------------");

    if s3.is_versioning_enabled(bucket).await.unwrap() {
        let versions = s3.get_object_versions(bucket, prefix).await.unwrap();
        
        let total = Stats::from(&versions);
        println!("Total: {:#?}", total);
        
        let current: Vec<_> = versions.iter().filter(|t|{
            t.is_latest.unwrap_or(false)
        }).collect();
        let current_object_keys: HashSet<String> = current.iter().map(|t|{
            t.key.as_ref().unwrap().clone()
        }).collect();
        let current = Stats::from(&current);
        println!("Current objects: {:#?}", current);

        let (current_versions, orphaned_versions): (Vec<_>, Vec<_>) = versions.iter()
            .filter(|t|!t.is_latest.unwrap())
            .partition(|t|{
                t.key().map(|k|current_object_keys.contains(k)).unwrap()
            });

        let current = Stats::from(&current_versions);
        println!("Current versions: {:#?}", current);
        let orphaned = Stats::from(&orphaned_versions);
        println!("Orphaned versions: {:#?}", &orphaned);
    } else {
        println!(" * Versioning is NOT active");
        let objects = s3.list_objects_v2(bucket, prefix).await.unwrap();
        let size = ByteSize::b(objects.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64);
        println!(" * {} across {} objects", size, objects.len());
    }
        
    
    Ok(())
}
