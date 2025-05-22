use std::{borrow::Borrow, collections::HashSet, path::Path};

use aws_sdk_s3::{types::ObjectVersion, Client};
use bytesize::ByteSize;
use clap::Parser;
use serde::Serialize;
use tokio::runtime::Runtime;
use color_eyre::{Result};
use tools::{log::setup_logging, s3::{S3Path, S3Wrapper}};

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli{
    /// Verbose mode (-v, -vv, -vvv)
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(subcommand)]
    command: Command,

    /// CSV output filepath
    #[structopt(short, long, default_value="bucket_usage.csv")]
    out_file: Option<String>,
}

#[derive(Parser)]
enum Command{
    #[clap(name = "size", about = "Report on a single bucket/prefix to console")]
    Size{
        /// S3 URL
        #[clap(short, long)]
        url: String,
    },
    #[clap(name = "report", about = "Report on a multiple buckets/prefixes to CSV")]
    Report{
        /// Comma separated S3 URLs
        #[clap(short, long, value_delimiter = ',', num_args = 1..)]
        urls: Vec<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    setup_logging(cli.verbose);
    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle().clone();

    let s3 = runtime.block_on(async {
        let config = aws_config::load_from_env().await;

        S3Wrapper{
            handle,
            client: Client::new(&config),
        }
    });


    runtime.block_on(async {
        match cli.command {
            Command::Size { url } => {
                let path = S3Path::parse(&url).unwrap();
                do_stuff(&path, &s3, cli.out_file).await.unwrap();    
            },
            Command::Report { urls } => {
                todo!()
            },
        }
    });


    Ok(())
}

#[derive(Debug)]
struct Stats {
    num_objects: usize,
    size: ByteSize,
}
impl Stats {
    fn from<T: Borrow<ObjectVersion>>(items: &[T]) -> Self {
        let size = ByteSize::b(items.iter().map(|o|o.borrow().size.unwrap()).sum::<i64>() as u64);
        Stats {
            num_objects: items.len(),
            size,
        }
    }
}

#[derive(Debug)]
struct Report {
    url: String,
    total: Stats,
    current_objects: Stats,
    versions: Option<Versions>,
}
impl AsRef<Report> for Report {
    fn as_ref(&self) -> &Report {
        self
    }
}

#[derive(Debug)]
struct Versions {
    current: Stats,
    orphaned: Stats,
}

#[derive(Debug, Serialize)]
struct CSVFlattened {
    url: String,
    total_b: u64,
    total_human: String,
    total_qty: usize,
    current_b: u64,
    current_human: String,
    current_qty: usize,
    c_ver_b: u64,
    c_ver_human: String,
    c_ver_qty: usize,
    o_ver_b: u64,
    o_ver_human: String,
    o_ver_qty: usize,
}
impl<T: AsRef<Report>> From<T> for CSVFlattened{
    fn from(value: T) -> CSVFlattened {
        let report = value.as_ref();
        CSVFlattened { 
            url: report.url.clone(), 
            total_b: report.total.size.0, 
            total_human: report.total.size.to_string(), 
            total_qty: report.total.num_objects, 
            current_b: report.current_objects.size.0, 
            current_human: report.current_objects.size.to_string(), 
            current_qty: report.current_objects.num_objects, 
            c_ver_b: report.versions.as_ref().map(|v|v.current.size.0).unwrap_or_default(), 
            c_ver_human: report.versions.as_ref().map(|v|v.current.size.to_string()).unwrap_or_default(), 
            c_ver_qty: report.versions.as_ref().map(|v|v.current.num_objects).unwrap_or_default(), 
            o_ver_b: report.versions.as_ref().map(|v|v.orphaned.size.0).unwrap_or_default(), 
            o_ver_human: report.versions.as_ref().map(|v|v.orphaned.size.to_string()).unwrap_or_default(), 
            o_ver_qty: report.versions.as_ref().map(|v|v.orphaned.num_objects).unwrap_or_default(), 
        }
    }
}

async fn do_stuff<P>(s3_path: &S3Path, s3: &S3Wrapper, out_file: Option<P>) -> Result<()> 
where 
    P: AsRef<Path>,
{
    // println!("{s3_path.bucket}/{s3_path.prefix}");

    let report = if s3.is_versioning_enabled(&s3_path.bucket).await? {
        let versions = s3.get_object_versions(&s3_path.bucket, &s3_path.prefix).await.unwrap();
        
        let total = Stats::from(&versions);
        
        let current: Vec<_> = versions.iter().filter(|t|{
            t.is_latest.unwrap_or(false)
        }).collect();
        let current_object_keys: HashSet<String> = current.iter().map(|t|{
            t.key.as_ref().unwrap().clone()
        }).collect();
        let current_objects = Stats::from(&current);

        let (current, orphaned): (Vec<_>, Vec<_>) = versions.iter()
            .filter(|t|!t.is_latest.unwrap())
            .partition(|t|{
                t.key().map(|k|current_object_keys.contains(k)).unwrap()
            });

        let current_versions = Stats::from(&current);
        let orphaned_versions = Stats::from(&orphaned);

        let report = Report {
            url: format!("{}/{}", &s3_path.bucket, &s3_path.prefix),
            total,
            current_objects,
            versions: Some(Versions{
                current: current_versions,
                orphaned: orphaned_versions,
            })
        };

        println!("{:#?}", &report);

        report
    } else {
        println!(" * Versioning is NOT active");
        let objects = s3.list_objects_v2(&s3_path.bucket, &s3_path.prefix).await.unwrap();
        let size = ByteSize::b(objects.iter().map(|o|o.size.unwrap()).sum::<i64>() as u64);
        println!(" * {} across {} objects", size, objects.len());

        todo!()
    };


    match out_file {
        Some(path_string) => {
            let mut writer = csv::Writer::from_path(path_string)?;
            writer.serialize::<CSVFlattened>((&report).into())?;
            Ok::<(), color_eyre::Report>(())
        },
        None => Ok(()),
    }?;
    
    Ok(())
}
