// cargo run --bin gpu_test

use std::process::Command;

use tools::gpu::Gpu;

/*
Tested on g4dn.xlarge
Installed GPU as per https://docs.nvidia.com/cuda/cuda-installation-guide-linux/

Useful tools:
- git clone https://github.com/wilicc/gpu-burn
- sudo apt install gpustat
*/

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

    let gpu = Gpu::init().expect("Didn't initialise an NVidia GPU");
    gpu.check_usage_all()
}