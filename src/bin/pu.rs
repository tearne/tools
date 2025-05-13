use std::{path::Path, process::Command, sync::{Arc, Mutex}};

use chrono::{DateTime, Local};
use clap::Parser;
use log::LevelFilter;
use sysinfo::{Pid, Process, ProcessRefreshKind, ProcessesToUpdate, System, ThreadKind};
use color_eyre::eyre::Result;

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

pub fn setup_logging(level: u8) {
    fn set_log_level(local_level: LevelFilter, dep_level:  LevelFilter) {
        let prog: String = std::env::current_exe()
            .unwrap()
            .file_name().unwrap()
            .to_str().unwrap()
            .to_owned();

        env_logger::builder()
            .filter_level(dep_level)
            .filter(Some(&prog), local_level)
            .init();
        log::trace!("Program name detected as {} for logging purposes.", &prog);
        log::info!("Local log level set to {}.", local_level);
        log::info!("Default Log level set to {}.", dep_level);
    }

    match level {
        0 => set_log_level(LevelFilter::Warn, LevelFilter::Warn),
        1 => set_log_level(LevelFilter::Info, LevelFilter::Warn),
        2 => set_log_level(LevelFilter::Debug, LevelFilter::Warn),
        3 => set_log_level(LevelFilter::Trace, LevelFilter::Info),
        _ => panic!("Too many levels of verbosity.  You can have up to 3."),
    };
}


fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    setup_logging(cli.verbose);

    let out_file = Path::new(&cli.file);

    let wtr = {
        Arc::new(Mutex::new(csv::Writer::from_path(Path::new(out_file))))
    };

    let mut child = Command::new(&cli.command[0])
        .args(&cli.command[1..])
        .spawn()
        .expect("Command failed to start.");

    let pid = child.id();
    let pause = std::time::Duration::from_secs(cli.interval);
    let start_time = Local::now();

    let wtr_cloned = wtr.clone();
    let thread = std::thread::spawn(move ||{
        let pid = Pid::from_u32(pid);

        let mut sys = System::new_all();
        let system_memory = sys.total_memory() as f32;
        log::info!("System has {} MiB RAM", system_memory / MI_B);

        let mut wrt_guard = wtr_cloned.lock().unwrap();
        
        loop{
            std::thread::sleep(pause);

            // sys.refresh_all();
            // sys.refresh_cpu_usage();
            // sys.refresh_processes_specifics(
            //     ProcessesToUpdate::All,
            //     true,
            //     ProcessRefreshKind::nothing().with_cpu()
            // );
            // sys.refresh_memory();
            sys.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing()
                    .with_memory()
                    .with_cpu()
                    // .with_disk_usage()
                    // .with_exe(UpdateKind::OnlyIfNotSet)
            );

            if let Some(process) = sys.process(pid) {
                let cpu_ram = get_usage(process, &sys);

                let record = UsageRecord::new(start_time, cpu_ram, system_memory);
                let writer = wrt_guard.as_mut().unwrap();
                writer.serialize(record).unwrap();
                writer.flush().unwrap();
            } else {
                log::info!("He's dead, Jim");
                break;
            }

            sys.refresh_processes_specifics(
                ProcessesToUpdate::All,
                true,
                ProcessRefreshKind::nothing().with_cpu()
            );
        }
    });

    log::info!("Waiting for command to complete...");
    child.wait()?;
    log::info!("Waiting for monitoring thread...");
    thread.join().unwrap();
    log::info!("Flushing report...");
    wtr.lock().unwrap().as_mut().unwrap().flush()?;

    log::info!("Usage report written to {}", &out_file.to_string_lossy());

    Ok(())
}

fn get_usage(process: &Process, sys: &System) -> CpuRam {
    let process_pid = process.pid();

    let children: Vec<_> = sys.processes()
        .iter()
        .filter(|(_pid, process)|{
            let is_child = process.parent()
                .map(|ppid|ppid == process_pid)
                .unwrap_or(false);

            let is_user_thread = process.thread_kind()
                .map(|k|k==ThreadKind::Userland)
                .unwrap_or(false);

            is_child && !is_user_thread
        }
        )
        .collect();

    log::trace!("Process {} has Children {:?}", process_pid, &children.iter().map(|(pid, _)|pid).collect::<Vec<_>>());
    let children_load: CpuRam = children
        .iter()
        .map(|(_, child)|
            get_usage(child, sys)
        )
        .sum();

    log::trace!("Process {} has child load {:?}", process_pid, children_load);
    log::trace!("Process {}: CPU: {}, RAM_mem: {}, RAM_virt: {}", process_pid, process.cpu_usage(), process.memory(), process.virtual_memory());

    children_load + CpuRam { 
        cpu_percent: process.cpu_usage(), 
        memory_bytes: process.memory() 
    }
}

#[derive(derive_more::Add, derive_more::Sum, serde::Serialize, Debug)]
struct CpuRam{
    cpu_percent: f32,
    memory_bytes: u64,
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
    fn new(start_time: DateTime<Local>, cpu_ram: CpuRam, system_memory: f32) -> Self {
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