use backtrace::Backtrace;
use chrono::{DateTime, Local};
use clap::Parser;
use color_eyre::eyre::Result;
use std::{
    path::Path,
    process::{Child, Command},
};
use sysinfo::Pid;
use tools::{
    log::setup_logging,
    process::{
        gpu::{Gpu, GpuApi},
        system::{CpuRamUsage, System},
    },
};

static MI_B: f32 = 2u64.pow(20) as f32;

trait GracefulExit<T, E> {
    fn warn_and_exit(self, msg: &str, child_process: Option<&mut Child>) -> T;
}

impl<T, E: std::fmt::Debug> GracefulExit<T, E> for Result<T, E> {
    fn warn_and_exit(self, msg: &str, child_process: Option<&mut Child>) -> T {
        match self {
            Ok(val) => val,
            Err(e) => {
                log::warn!("{}: {:?}", msg, e);
                child_process.map(|child| {
                    log::info!("Killing child process: {}", child.id());
                    child.kill()
                });
                log::debug!("{:?}", Backtrace::new());
                std::process::exit(1);
            }
        }
    }
}

#[derive(Parser)]
#[command(version, about)]
/// Run a command, monitoring CPU and RAM usage at regular intervals and saving to a CSV file.
struct Cli {
    /// Verbose mode (-v, -vv, -vvv)
    #[structopt(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[structopt(short, long, action)]
    nvml: bool,

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

fn start_process(command: &Vec<String>) -> Child {
    Command::new(&command[0])
        .args(&command[1..])
        .spawn()
        .warn_and_exit(
            &format!("Command failed to start: {:?}", command.join(" ")),
            None,
        )
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let mut system = System::new();
    let system_memory = system.total_memory() as f32;

    let gpu_api_opt = if cli.nvml { Some(GpuApi::new()?) } else { None };
    let mut gpu_dev_opt = gpu_api_opt.as_ref().map(|api| Gpu::new(&api)).transpose()?;

    let out_file = Path::new(&cli.file);

    let mut wtr = csv::Writer::from_path(Path::new(out_file))
        .warn_and_exit(&format!("Problem opening file: {}", cli.file), None);

    let mut command_process = start_process(&cli.command);

    let pid = Pid::from_u32(command_process.id());
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();

    system.refresh_process_stats();

    loop {
        std::thread::sleep(pause);
        match command_process.try_wait().warn_and_exit(
            &format!("Command process failed: {}", &cli.command.join(" ")),
            Some(&mut command_process),
        ) {
            None => std::thread::sleep(pause),
            Some(_) => {
                log::info!("pid {} is dead", pid);
                break;
            }
        }

        let gpu_usage_opt = gpu_api_opt
            .as_ref()
            .map(|api| api.get_pid_utilisation(gpu_dev_opt.as_mut().unwrap(), pid, &mut system))
            .transpose()?;

        let cpu_ram = system.get_pid_tree_utilisation(pid);

        let record = UsageRecord::new(start_time, system_memory, cpu_ram, gpu_usage_opt);

        wtr.serialize(&record).warn_and_exit(
            &format!("Failed to serialize record: {:?}", record),
            Some(&mut command_process),
        );
        wtr.flush().warn_and_exit(
            "Problem writing to underlying writer",
            Some(&mut command_process),
        );
    }

    log::info!("Waiting for command to complete...");
    command_process
        .wait()
        .warn_and_exit("Command wasn't running", Some(&mut command_process));

    log::info!("Usage report written to {}", &cli.file);

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct UsageRecord {
    timestamp: String,
    elapsed_seconds: usize,
    cpu_percent: String,
    ram_percent: String,
    ram_mb: String,
    gpu_percent: String,
}

impl UsageRecord {
    fn new(
        start_time: DateTime<Local>,
        system_memory: f32,
        cpu_ram: CpuRamUsage,
        gpu_percent: Option<u32>,
    ) -> Self {
        let now = Local::now();
        let elapsed_seconds = (now - start_time).as_seconds_f32();

        Self {
            timestamp: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            elapsed_seconds: elapsed_seconds.round() as usize,
            cpu_percent: format!("{:.1}", cpu_ram.cpu_percent),
            ram_percent: format!(
                "{:.1}",
                100.0 * (cpu_ram.memory_bytes as f32 / system_memory)
            ),
            ram_mb: format!("{:.1}", cpu_ram.memory_bytes as f32 / MI_B),
            gpu_percent: gpu_percent.as_ref().map(|value| format!("{:.1}", value)).unwrap_or_else(||"NA".into()),
        }
    }
}
