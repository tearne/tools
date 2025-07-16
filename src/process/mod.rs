use std::collections::HashSet;

use sysinfo::{Pid, Process, System, ThreadKind};

pub mod gpu;
pub mod cpu;

pub fn pid_is_alive(process_id: u32, sys: &System) -> bool {
    let t = sys.process(Pid::from_u32(process_id));
    t.is_some()
}

pub fn get_pid_tree(process_id: u32, sys: &System, exclude_userland: bool) -> HashSet<u32> {
    fn find_children(pid: u32, sys: &System, exclude_userland: bool) -> HashSet<u32> { 
        let pid = Pid::from_u32(pid);
        let children_it = sys.processes()
            .iter()
            .filter(|(_pid, proc)| {
                proc.parent().map(|ppid| ppid == pid).unwrap_or(false)
            });
            
        let children_it: Box<dyn Iterator<Item = (&Pid, &Process)>> = if exclude_userland {
            // Filter out processes in userland
            Box::new(children_it.filter(|(_,proc)|{
                proc.thread_kind()
                    .map(|k| k != ThreadKind::Userland)
                    .unwrap_or(true)
            }))
        } else {
            // Keep all child processes
            Box::new(children_it)
        };
            
        children_it.map(|x| x.0.as_u32())
            .collect()
    }
    
    let mut to_visit = vec![process_id];
    let mut acc = HashSet::new();

    while let Some(pid) = to_visit.pop() {
        acc.insert(pid);
        to_visit.extend(find_children(pid, sys, exclude_userland));
    }

    acc
}