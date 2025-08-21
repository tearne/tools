use chrono::{DateTime, Local};
use clap::Parser;
use color_eyre::eyre::{Context, Result};
use std::{
    fs::canonicalize,
    path::Path,
    process::{Child, Command},
    sync::{Arc, Mutex},
};
use sysinfo::Pid;
use tools::{
    log::setup_logging,
    process::{
        gpu::GpuApi,
        system::{CpuRamUsage, System},
    },
};
use backtrace::Backtrace;

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
                child_process.map(|child| child.kill());
                log::trace!("{:?}", Backtrace::new());
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
        .warn_and_exit(&format!("Command failed to start: {:?}", command.join(" ")), None)
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let mut system = System::new();
    let system_memory = system.total_memory() as f32;

    match cli.nvml {
        true => {
            let gpu_api = GpuApi::new()?;
            let gpu_devices = gpu_api.build_devices()?;

            let out_file = Path::new(&cli.file);

            let mut wtr = csv::Writer::from_path(Path::new(out_file))?;

            let mut command_process = start_process(&cli.command);

            let pid = Pid::from_u32(command_process.id());
            let pause = std::time::Duration::from_secs(cli.interval);
            let start_time = Local::now();

            let mut last_seen_timestamp: Option<u64> = None;
            loop {
                match command_process.try_wait().warn_and_exit(
                    &format!("Command process failed: {:?}", &cli.command.join(" ")),
                    Some(&mut command_process),
                ) {
                    None => std::thread::sleep(pause),
                    Some(_) => {
                        log::info!("pid {} is dead", pid);
                        break;
                    }
                }

                let usage = gpu_api.get_pid_utilisation(
                    &gpu_devices,
                    pid,
                    last_seen_timestamp,
                    &mut system,
                )?;
                match usage {
                    Some(proc_usage) => {
                        last_seen_timestamp = Some(proc_usage.last_seen_timestamp);

                        let record = UsageRecord::new(
                            start_time,
                            system_memory,
                            None,
                            Some(proc_usage.percent),
                        );

                        wtr.serialize(&record).warn_and_exit(
                            &format!("Failed to serialize record: {:?}", record),
                            Some(&mut command_process),
                        );
                        wtr.flush().warn_and_exit(
                            "Problem writing to underlying writer",
                            Some(&mut command_process),
                        );
                    }
                    None => {
                        log::info!("GPU process not found. Most likely it has finished");
                        continue;
                    }
                }
            }
            log::info!("Waiting for command to complete...");
            command_process.wait()?;
        }

        false => {
            let writer = canonicalize(Path::new(&cli.file))
                .wrap_err("failed to canonicalize path")
                .and_then(|abs_path| {
                    csv::Writer::from_path(&abs_path)
                        .wrap_err("failed to create output file writer")
                })
                .warn_and_exit("Error", None);

            let writer = Arc::new(Mutex::new(writer));

            let mut command_process = start_process(&cli.command);

            let pid = Pid::from_u32(command_process.id());

            log::trace!("Started pid {}", pid);

            let pause = std::time::Duration::from_secs(cli.interval);
            let start_time = Local::now();
            let writer_cloned = writer.clone();

            let thread = std::thread::spawn(move || {
                log::info!("System memory: {}", system_memory);
                let mut wrt_guard = writer_cloned.lock().unwrap();

                system.refresh_process_stats();
                loop {
                    std::thread::sleep(pause);

                    if !system.pid_is_alive(pid) {
                        log::info!("pid {} is dead", pid);
                        break;
                    }
                    let cpu_ram = system.get_pid_tree_utilisation(pid);

                    let record = UsageRecord::new(start_time, system_memory, Some(cpu_ram), None);
                    wrt_guard.serialize(&record).unwrap();

                    wrt_guard.flush().unwrap();
                }
            });

            log::info!("Waiting for command to complete...");
            command_process
                .wait()
                .warn_and_exit("Problem waiting for command process", Some(&mut command_process));
            log::info!("Waiting for monitoring thread...");
            thread.join().warn_and_exit("Problem joining thread", None);
            log::info!("Flushing report...");
            writer
                .lock()
                .warn_and_exit("Problem acquiring lock for writer", None)
                .flush()
                .warn_and_exit("Problem writing to underlying writer", None)
        }
    }

    log::info!("Usage report written to {}", &cli.file);

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct UsageRecord {
    timestamp: String,
    elapsed_seconds: usize,
    cpu_percent: Option<String>,
    ram_percent: Option<String>,
    ram_mb: Option<String>,
    gpu_percent: Option<String>,
}

impl UsageRecord {
    fn new(
        start_time: DateTime<Local>,
        system_memory: f32,
        cpu_ram: Option<CpuRamUsage>,
        gpu_percent: Option<u32>,
    ) -> Self {
        let now = Local::now();
        let elapsed_seconds = (now - start_time).as_seconds_f32();

        Self {
            timestamp: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            elapsed_seconds: elapsed_seconds.round() as usize,
            cpu_percent: cpu_ram
                .as_ref()
                .map(|value| format!("{:.1}", value.cpu_percent)),
            ram_percent: cpu_ram
                .as_ref()
                .map(|value| format!("{:.1}", 100.0 * (value.memory_bytes as f32 / system_memory))),
            ram_mb: cpu_ram
                .as_ref()
                .map(|value| format!("{:.1}", value.memory_bytes as f32 / MI_B)),
            gpu_percent: gpu_percent.as_ref().map(|value| format!("{:.1}", value)),
        }
    }
}
