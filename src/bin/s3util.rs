use std::{borrow::Borrow, collections::HashSet, fmt::Display};

use aws_sdk_s3::{types::{Object, ObjectVersion}, Client};
use bytesize::ByteSize;
use clap::Parser;
use serde::Serialize;
use tokio::runtime::Runtime;
use color_eyre::{Result};
use tools::{log::setup_logging, s3::{types::{S3Path, Stats}, wrapper::S3Wrapper}};

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
        #[clap(short, long)]
        url: String,
    },
    #[clap(name = "size-report", about = "Report on a multiple buckets/prefixes to CSV")]
    SizeReport{
        /// Comma separated S3 URLs
        #[clap(short, long, value_delimiter = ',', num_args = 1..)]
        urls: Vec<String>,

        /// CSV output file
        #[clap(short, long, default_value="bucket_usage.csv")]
        out_file: String,
    },
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

        match cli.command {
            Command::Size { url } => {
                let url = S3Path::parse(&url)?;
                log::info!("Analysing: {}", &url);
                let report = build_report(&url, &s3).await?;
                println!("{}", report);    
            },
            Command::SizeReport { urls, out_file } => {
                let urls = urls.iter().map(|u|S3Path::parse(u)).collect::<Result<Vec<S3Path>>>()?;
                
                //Quick check to fail fast if we don't have access
                for url in &urls {
                    log::info!("Check access for {}", url);
                    let versioning_enabled = s3.is_versioning_enabled(&url.bucket).await?;
                    log::info!(" - version check result: {}", versioning_enabled);
                }
                
                let mut writer = csv::Writer::from_path(&out_file)?;
                for url in &urls {
                    log::info!("Analysing: {}", url);
                    let report = build_report(url, &s3).await?;
                    println!("Writing to {}: {}", &out_file, report);  
                    writer.serialize::<CSVFlattened>((&report).into())?;
                    writer.flush()?;
                }
                
            },
        };

        Ok(())
    })
}

#[derive(Debug)]
struct Report {
    url: String,
    total: Stats,
    versions: Option<VersionData>,
}
impl AsRef<Report> for Report {
    fn as_ref(&self) -> &Report {
        self
    }
}
impl Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(
            format_args!(
                "{}:\n  {} (Current obj. {}, Current vers. {}, Orphaned vers. {})", 
                self.url, 
                self.total.size, 
                self.versions.as_ref().unwrap().current_objects.size, 
                self.versions.as_ref().unwrap().current_obj_vers.size, 
                self.versions.as_ref().unwrap().orphaned_vers.size
            )
        )
    }
}

#[derive(Debug)]
struct VersionData {
    current_objects: Stats,
    current_obj_vers: Stats,
    orphaned_vers: Stats,
}

#[derive(Debug, Serialize)]
struct CSVFlattened {
    url: String,
    
    total_human: String,
    total_b: u64,
    total_qty: usize,
    
    versioning_active: bool,

    current_obj_human: String,
    current_ver_human: String,
    orphan_ver_human: String,

    current_obj_b: u64,
    current_ver_b: u64,
    orphan_ver_b: u64,

    current_ver_qty: usize,
    current_obj_qty: usize,
    orphan_ver_qty: usize,
}
impl<T: AsRef<Report>> From<T> for CSVFlattened{
    fn from(value: T) -> CSVFlattened {
        let report = value.as_ref();
        CSVFlattened { 
            url: report.url.clone(), 
            total_human: report.total.size.to_string(), 
            total_b: report.total.size.0, 
            total_qty: report.total.num_objects, 
            versioning_active: report.versions.is_some(),

            current_obj_human: report.versions.as_ref().map(|v|v.current_objects.size.to_string()).unwrap_or_default(), 
            current_ver_human: report.versions.as_ref().map(|v|v.current_obj_vers.size.to_string()).unwrap_or_default(), 
            orphan_ver_human: report.versions.as_ref().map(|v|v.orphaned_vers.size.to_string()).unwrap_or_default(), 

            current_obj_b: report.versions.as_ref().map(|v|v.current_objects.size.0).unwrap_or_default(), 
            current_ver_b: report.versions.as_ref().map(|v|v.current_obj_vers.size.0).unwrap_or_default(), 
            orphan_ver_b: report.versions.as_ref().map(|v|v.orphaned_vers.size.0).unwrap_or_default(), 

            current_obj_qty: report.versions.as_ref().map(|v|v.current_objects.num_objects).unwrap_or_default(), 
            current_ver_qty: report.versions.as_ref().map(|v|v.current_obj_vers.num_objects).unwrap_or_default(), 
            orphan_ver_qty: report.versions.as_ref().map(|v|v.orphaned_vers.num_objects).unwrap_or_default(), 
        }
    }
}

async fn build_report(s3_path: &S3Path, s3: &S3Wrapper) -> Result<Report> {
    if s3.is_versioning_enabled(&s3_path.bucket).await? {
        let versions = s3.get_object_versions(&s3_path.bucket, &s3_path.prefix).await.unwrap();
        
        let total = Stats::from_object_versions(&versions);
        
        let current: Vec<_> = versions.iter().filter(|t|{
            t.is_latest.unwrap_or(false)
        }).collect();
        let current_object_keys: HashSet<String> = current.iter().map(|t|{
            t.key.as_ref().unwrap().clone()
        }).collect();
        let current_objects = Stats::from_object_versions(&current);

        let (current, orphaned): (Vec<_>, Vec<_>) = versions.iter()
            .filter(|t|!t.is_latest.unwrap())
            .partition(|t|{
                t.key().map(|k|current_object_keys.contains(k)).unwrap()
            });

        let current_obj_vers = Stats::from_object_versions(&current);
        let orphaned_vers = Stats::from_object_versions(&orphaned);

        let report = Report {
            url: s3_path.to_string(),
            total,
            versions: Some(VersionData{
                current_objects,
                current_obj_vers,
                orphaned_vers,
            })
        };

        Ok(report)
    } else {
        log::warn!("Versioning is NOT active on {}", s3_path);
        let objects = s3.list_objects_v2(&s3_path.bucket, &s3_path.prefix).await.unwrap();
        let stats = Stats::from_objects(&objects);

        Ok(Report{
            url: s3_path.to_string(),
            total: stats,
            versions: None,
        })

    }
}
