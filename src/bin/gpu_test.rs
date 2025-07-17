// cargo run --bin gpu_test

use std::path::Path;
use std::process::Command;

use chrono::{DateTime, Local};
use clap::Parser;
use color_eyre::eyre::Result;
use sysinfo::Pid;
use tools::log::setup_logging;
use tools::process::gpu::GpuApi;
use tools::process::system::System;

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
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let gpu_api = GpuApi::new()?;
    let gpu_devices = gpu_api.build_devices()?;

    let out_file = Path::new(&cli.file);

    let mut wtr = csv::Writer::from_path(Path::new(out_file))?;

    let mut child = Command::new(&cli.command[0])
        .args(&cli.command[1..])
        .spawn()
        .expect("Command failed to start.");

    let pid = Pid::from_u32(child.id());
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();

    let mut system = System::new();

    let mut last_seen_timestamp: Option<u64> = None;
    loop {
        match child.try_wait().unwrap() {
            None => std::thread::sleep(pause),            
            Some(_) => {
                log::info!("He's dead, Jim");
                break;
            }
        }
        
        let usage = gpu_api.get_pid_utilisation(&gpu_devices, pid, last_seen_timestamp, &mut system)?;
        last_seen_timestamp = Some(usage.last_seen_timestamp);

        let record = UsageRecord::new(
            start_time, 
            usage.percent
        );

        wtr.serialize(record).unwrap();
        wtr.flush().unwrap();
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
