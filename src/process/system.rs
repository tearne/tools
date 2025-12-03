use std::collections::HashSet;

use sysinfo::{
    Pid, Process, ProcessRefreshKind, ProcessesToUpdate, System as SysInfoSystem, ThreadKind,
};

pub struct System {
    sys_info: SysInfoSystem,
}

impl System {
    pub fn new() -> Self {
        let mut instance = Self {
            sys_info: SysInfoSystem::new(),
        };
        instance.sys_info.refresh_all();
        instance
    }

    pub fn refresh_process_stats(&mut self) {
        self.sys_info.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing()
                .with_memory()
                .with_cpu()
                .with_tasks(),
        );
    }

    pub fn total_memory(&self) -> u64 {
        self.sys_info.total_memory()
    }

    pub fn get_pid_tree_utilisation(&mut self, pid: Pid) -> CpuRamUsage {
        let children = self.get_pid_tree(pid, true);
        log::trace!("Descendants of {}: {:#?}", pid, &children);

        children
            .iter()
            .filter_map(|pid| {
                let proc_opt = self.sys_info.process(*pid);
                log::trace!("Found child: {:?}", proc_opt.map(|p| p.pid()));
                proc_opt
            })
            .map(|proc| {
                let usage = CpuRamUsage {
                    cpu_percent: proc.cpu_usage(),
                    memory_bytes: proc.memory(),
                };
                log::info!("{} -> {:?}", proc.pid(), usage);
                usage
            })
            .sum()
    }

    pub fn get_pid_tree(&mut self, root_pid: Pid, exclude_userland: bool) -> HashSet<Pid> {
        self.refresh_process_stats();

        fn find_children(
            pid: Pid,
            sys_info: &SysInfoSystem,
            exclude_userland: bool,
        ) -> HashSet<Pid> {
            let children_it = sys_info
                .processes()
                .iter()
                .filter(|(_pid, proc)| proc.parent().map(|ppid| ppid == pid).unwrap_or(false));

            let children_it: Box<dyn Iterator<Item = (&Pid, &Process)>> = if exclude_userland {
                // Filter out processes in userland
                Box::new(children_it.filter(|(_, proc)| {
                    proc.thread_kind()
                        .map(|k| k != ThreadKind::Userland)
                        .unwrap_or(true)
                }))
            } else {
                // Keep all child processes
                Box::new(children_it)
            };

            children_it.map(|(&pid, _)| pid).collect()
        }

        let mut to_visit: Vec<Pid> = vec![root_pid];
        let mut acc: HashSet<Pid> = HashSet::new();

        while let Some(pid) = to_visit.pop() {
            acc.insert(pid);
            to_visit.extend(find_children(pid, &self.sys_info, exclude_userland));
        }

        acc
    }

    /**
     * Assumes process stats were recently refreshed
     */
    pub fn pid_is_alive(&mut self, pid: Pid) -> bool {
        let t = self.sys_info.process(pid);
        t.is_some()
    }
}

impl Default for System {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(derive_more::Add, derive_more::Sum, serde::Serialize, Debug)]
pub struct CpuRamUsage {
    pub cpu_percent: f32,
    pub memory_bytes: u64,
}
