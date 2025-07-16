use std::collections::HashSet;

use sysinfo::{Pid, System, ThreadKind};

pub mod gpu;
pub mod cpu;



pub fn pid_is_alive(process_id: u32, sys: &System) -> bool {
    sys.process(Pid::from_u32(process_id)).is_some()
}

pub fn get_pid_tree(process_id: u32, sys: &System) -> HashSet<u32> {
    fn find_children(pid: u32, sys: &System) -> HashSet<u32> { 
        let pid = Pid::from_u32(pid);
        sys.processes()
            .iter()
            .filter(|(_pid, process)| {
                let is_child = process.parent().map(|ppid| ppid == pid).unwrap_or(false);

                let is_user_thread = process
                    .thread_kind()
                    .map(|k| k == ThreadKind::Userland)
                    .unwrap_or(false);
                is_child && !is_user_thread
            })
            .map(|x| x.0.as_u32())
            .collect()
    }
    
    let mut to_visit = vec![process_id];
    let mut acc = Vec::new();

    while let Some(pid) = to_visit.pop() {
        acc.push(pid);
        to_visit.extend(find_children(pid, sys));
    }

    HashSet::from_iter(acc.into_iter())
}