use chrono::{DateTime, Local};
use clap::Parser;
use color_eyre::eyre::{Context, Result};
use std::{
    path::Path,
    process::Command,
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

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let mut system = System::new();
    let system_memory = system.total_memory() as f32;

    let gpu_api_opt = if cli.nvml { Some(GpuApi::new()?) } else { None };
    let mut gpu_dev_opt = gpu_api_opt.as_ref().map(|api| Gpu::new(&api)).transpose()?;

    let out_file = Path::new(&cli.file);

    let mut wtr = csv::Writer::from_path(Path::new(out_file))?;

    let mut child_process = Command::new(&&cli.command[0])
        .args(&cli.command[1..])
        .spawn()?;

    let pid = Pid::from_u32(child_process.id());
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();

    system.refresh_process_stats();

    loop {
        let exit_status = child_process.try_wait().wrap_err_with(|| {
            format!("Abnormal User command status ({})", &cli.command.join(" "))
        })?;
        match exit_status {
            Some(_) => {
                log::info!("pid {} is dead", pid);
                break;
            }
            None => std::thread::sleep(pause),
        }

        let gpu_usage_opt = gpu_api_opt
            .as_ref()
            .map(|api| api.get_pid_utilisation(gpu_dev_opt.as_mut().unwrap(), pid, &mut system))
            .transpose()?;

        let cpu_ram = system.get_pid_tree_utilisation(pid);

        let record = UsageRecord::new(start_time, system_memory, cpu_ram, gpu_usage_opt);

        wtr.serialize(&record)
            .wrap_err_with(|| format!("Failed to serialize record: {:?}", record))?;
        wtr.flush()?;
    }

    log::info!("Waiting for command to complete...");
    child_process.wait()?;

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
            gpu_percent: gpu_percent
                .as_ref()
                .map(|value| format!("{:.1}", value))
                .unwrap_or_else(|| "NA".into()),
        }
    }
}
