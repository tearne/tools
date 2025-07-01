// cargo run --bin gpu_test

use std::process::Command;

use tools::gpu::Gpu;
use clap::Parser;

/*
Tested on g4dn.xlarge
Installed GPU as per https://docs.nvidia.com/cuda/cuda-installation-guide-linux/

Useful tools:
- git clone https://github.com/wilicc/gpu-burn
- sudo apt install gpustat
*/

#[derive(Parser)]
struct Cli {
    /// Command to run
    // getting the pid of gpu_burn using `gpu_burn & echo $!`
    #[arg(short, long, default_value = None)]
    pid: Option<u32>,

    /// Command to run
    // supplying a command doesn't seem to work - the PID returned
    // doesn't seem to be correct
    #[arg(short, long, default_value = None)]
    command: Option<Vec<String>>,
}


pub fn main() {
    // Check we have the hardware
    {
        let bytes = Command::new("lspci")
            .output()
            .unwrap()
            .stdout;

        let stdout = str::from_utf8(&bytes).unwrap();
        if stdout.contains("NVIDIA") {
            println!("Yep, you got a GPU");
        } else {
            panic!("No GPU found")
        }
    }

    let cli = Cli::parse();

    match cli.command {
        Some(command) => {
            let child = Command::new(&command[0])
                .args(&command[1..])
                .spawn()
                .expect("Command failed to start.");

            let pid = child.id();
            println!("{pid}");
            let gpu = Gpu::init().expect("Didn't initialise an NVidia GPU");
            gpu.check_usage_all(pid);
        }

        None => {
            match cli.pid {
                Some(pid) => {
                    let gpu = Gpu::init().expect("Didn't initialise an NVidia GPU");
                    gpu.check_usage_all(pid);
                }
                None => {
                    panic!("Must supply either --command or --pid arguments");
                }
            }
        }
    }


}