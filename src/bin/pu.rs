use std::{fs::canonicalize, path::Path, process::Command, sync::{Arc, Mutex}};
use chrono::{DateTime, Local};
use clap::Parser;
use color_eyre::eyre::{Context, Result};
use sysinfo::Pid;
use tools::{log::setup_logging, process::system::{CpuRamUsage, System}};

static MI_B: f32 = 2u64.pow(20) as f32;

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

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let writer = canonicalize(Path::new(&cli.file))
        .wrap_err("failed to canonicalize path")
        .and_then(|abs_path|{
            csv::Writer::from_path(&abs_path)
                .wrap_err("Failed to create output file writer")
        })?;

    let writer = Arc::new(Mutex::new(writer));
    let mut command_process = Command::new(&cli.command[0])
        .args(&cli.command[1..])
        .spawn()
        .expect("Command failed to start.");
    let mut system = System::new();

    let pid = Pid::from_u32(command_process.id());
    log::trace!("Started pid {}", pid);
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();
    let writer_cloned = writer.clone();

    let thread = std::thread::spawn(move ||{     
        let system_memory = system.total_memory() as f32;
        log::info!("System memory: {}", system_memory);
        let mut wrt_guard = writer_cloned.lock().unwrap();

        loop{
            std::thread::sleep(pause);

            if !system.pid_is_alive(pid) {
                log::info!("pid {} is dead", pid);
                break;
            }

            let cpu_ram = system.get_pid_tree_utilisation(pid);

            let record = UsageRecord::new(start_time, cpu_ram, system_memory);
            // let writer = wrt_guard.as_mut().unwrap();
            wrt_guard.serialize(record).unwrap();
            wrt_guard.flush().unwrap();
        }
    });

    log::info!("Waiting for command to complete...");
    command_process.wait()?;
    log::info!("Waiting for monitoring thread...");
    thread.join().unwrap();
    log::info!("Flushing report...");
    writer.lock().unwrap().flush()?;
    log::info!("Usage report written to {}", &cli.file);

    Ok(())
}


#[derive(Debug, serde::Serialize)]
struct UsageRecord {
    timestamp: String,
    elapsed_seconds: usize,
    cpu_percent: String,
    ram_percent: String,
    ram_mb: String
}

impl UsageRecord {
    fn new(start_time: DateTime<Local>, cpu_ram: CpuRamUsage, system_memory: f32) -> Self {
        let now = Local::now();
        let elapsed_seconds = (now - start_time).as_seconds_f32();
        Self {
            timestamp: now.format("%Y-%m-%d %H:%M:%S").to_string(),
            elapsed_seconds: elapsed_seconds.round() as usize,
            cpu_percent: format!("{:.1}", cpu_ram.cpu_percent),
            ram_percent: format!("{:.1}", 100.0 * (cpu_ram.memory_bytes as f32 / system_memory)),
            ram_mb: format!("{:.1}", cpu_ram.memory_bytes as f32 / MI_B),
        }
    }
}