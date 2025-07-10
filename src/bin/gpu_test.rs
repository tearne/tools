// cargo run --bin gpu_test

use std::path::Path;
use std::process::Command;

use chrono::{DateTime, Local};
use clap::Parser;
use nvml_wrapper::error::NvmlError;
use tools::gpu::Gpu;
use color_eyre::eyre::Result;
use tools::log::setup_logging;

/*
Tested on g4dn.xlarge
Installed GPU as per https://docs.nvidia.com/cuda/cuda-installation-guide-linux/

Useful tools:
- git clone https://github.com/wilicc/gpu-burn
- sudo apt install gpustat
*/

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli {
    /// Verbose mode (-v, -vv, -vvv)
    #[structopt(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// CPU polling interval (seconds)
    #[structopt(short, long, default_value = "1")]
    interval: u64,

    /// Command to run
    #[arg(last = true, required = true)]
    command: Vec<String>,

    /// Output CSV file
    #[structopt(short, long, default_value = "gpu_process_usage.csv")]
    file: String,
}

fn main() -> Result<()> {
    // Check we have the hardware
    {
        let bytes = Command::new("lspci").output().unwrap().stdout;

        let stdout = str::from_utf8(&bytes).unwrap();
        if stdout.contains("NVIDIA") {
            log::info!("Yep, you got a GPU");
        } else {
            panic!("No GPU found")
        }
    }

    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let out_file = Path::new(&cli.file);

    let mut wtr = csv::Writer::from_path(Path::new(out_file)).unwrap();

    let mut child = Command::new(&cli.command[0])
        .args(&cli.command[1..])
        .spawn()
        .expect("Command failed to start.");

    let pid = child.id();
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();

    let gpu = Gpu::init().expect("Didn't initialise an NVidia GPU");
    let mut last_seen_timestamp: Option<u64> = None;
    loop {
        match child.try_wait().unwrap() {
            None => std::thread::sleep(pause),            
            Some(_) => {
                log::info!("He's dead, Jim");
                break;
            }
        }
        
        let all_gpu_utilisation = gpu.get_all_gpu_utilisation(last_seen_timestamp);
        for device_utilisation in all_gpu_utilisation.iter() {
            match device_utilisation {
                Ok(result) => {
                    let process_utilisation = gpu.get_process_utilisation(pid, result);
                    last_seen_timestamp = Some(result[0].timestamp);
                    let record = UsageRecord::new(start_time, process_utilisation);
                    // let writer = wrt.as_mut().unwrap();
                    wtr.serialize(record).unwrap();
                    wtr.flush().unwrap();
                    break;
                }
                Err(e) => match e {
                    NvmlError::Uninitialized => panic!("{e}"),
                    _ => {
                        println!("{e}");
                        continue;
                    }
                }
            }
        }
    }

    log::info!("Usage report written to {}", &out_file.to_string_lossy());

    Ok(())
}


#[derive(Debug, serde::Serialize)]
struct UsageRecord {
    timestamp: String,
    elapsed_seconds: usize,
    gpu_percent: String,
}

impl UsageRecord {
    fn new(start_time: DateTime<Local>, gpu_percent: u32) -> Self {
        let now = Local::now();
        let elapsed_seconds = (now - start_time).as_seconds_f32();
        Self {
            timestamp: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            elapsed_seconds: elapsed_seconds.round() as usize,
            gpu_percent: format!("{:.1}", gpu_percent),
        }
    }
}
