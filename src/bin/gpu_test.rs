// cargo run --bin gpu_test

use core::time;
use std::process::Command;
use std::thread;

use clap::Parser;
use tools::gpu::Gpu;
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
    #[structopt(short, long, default_value = "process_usage.csv")]
    file: String,
}

pub fn main() {
    // Check we have the hardware
    {
        let bytes = Command::new("lspci").output().unwrap().stdout;

        let stdout = str::from_utf8(&bytes).unwrap();
        if stdout.contains("NVIDIA") {
            println!("Yep, you got a GPU");
        } else {
            panic!("No GPU found")
        }
    }

    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let child = Command::new(&cli.command[0])
        .args(&cli.command[1..])
        .spawn()
        .expect("Command failed to start.");

    let pid = child.id();
    let pause = std::time::Duration::from_secs(cli.interval);
    let gpu = Gpu::init().expect("Didn't initialise an NVidia GPU");
    let mut last_seen_timestamp: u64 = 0;
    for _ in 0..10 {
        std::thread::sleep(pause);
        let result = gpu.get_all_gpu_utilisation(last_seen_timestamp);
        let usage = gpu.get_process_utilisation(pid, &result.1);
        last_seen_timestamp = result.0;
        println!("{usage}");

    }
}
