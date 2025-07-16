use sysinfo::{Pid, System, ThreadKind};

use crate::process::get_pid_tree;


pub fn get_pid_utilisation(pid: u32, sys: &mut System) -> CpuRamUsage {
    let children = get_pid_tree(pid, sys);
    log::trace!("Descendants of {}: {:#?}", pid, &children);
   
    let usage = children.iter()
        .filter_map(|pid|{
            let proc_opt = sys.process(Pid::from_u32(*pid));
            log::trace!("Found child: {:?}", proc_opt.map(|p|p.pid()));
            proc_opt
        })
        .filter(|&proc|{
            match proc.thread_kind() {
                Some(kind) => {
                    let is_userland = kind == ThreadKind::Userland;
                    log::trace!("pid {} has kind {:?}", proc.pid(), kind);
                    is_userland
                },
                None => {
                    log::info!("Discarding process {} as no thread kind", proc.pid());
                    false
                },
            }
        })
        .map(|proc|{
            let usage = CpuRamUsage{
                cpu_percent: proc.cpu_usage(),
                memory_bytes: proc.memory(),
            };
            log::info!("{} -> {:?}", proc.pid(), usage);
            usage
        })
        .sum();

    usage
}

#[derive(derive_more::Add, derive_more::Sum, serde::Serialize, Debug)]
pub struct CpuRamUsage{
    pub cpu_percent: f32,
    pub memory_bytes: u64,
}