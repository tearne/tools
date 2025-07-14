use nvml_wrapper::{Nvml, error::NvmlError, struct_wrappers::device::ProcessUtilizationSample};
use sysinfo::{Pid, System, ThreadKind};


pub struct Gpu {
    nvml: Nvml,
}

impl Gpu {
    pub fn init() -> Option<Gpu> {
        match Nvml::init() {
            Ok(nvml) => Some(Gpu { nvml }),
            Err(e) => {
                println!("Gpu didn't initialise: {}", e);
                None
            }
        }
    }

    pub fn get_all_gpu_utilisation<T>(
        &self,
        last_seen_timestamp: T,
    ) -> Vec<Result<Vec<ProcessUtilizationSample>, NvmlError>>
    where
        T: Into<Option<u64>>,
        T: Clone,
    {
        let num_devices = self.nvml.device_count().unwrap();
        let mut all_utilisation = Vec::new();
        for idx in 0..num_devices {
            let device = self.nvml.device_by_index(idx).unwrap();
            all_utilisation.push(device.process_utilization_stats(last_seen_timestamp.clone()))
        }
        return all_utilisation;
    }

    pub fn get_process_utilisation(
        &self,
        process_pid: u32,
        device_utilisation: &Vec<ProcessUtilizationSample>,
    ) -> u32 {
        let children = Self::get_children(process_pid);
        log::trace!(
            "Process {} has Children {:?}",
            process_pid,
            &children.iter().map(|pid| pid).collect::<Vec<_>>()
        );
        let child_utilisation: u32 = children
            .into_iter()
            .map(|child| Self::get_process_utilisation(&self, child, device_utilisation))
            .sum();

        let mut process_utilisation: u32 = 0;
        for util in device_utilisation.iter() {
            if process_pid == util.pid {
                process_utilisation = util.sm_util;
                break;
            }
        }

        return process_utilisation + child_utilisation;
    }

    fn get_children(process_id: u32) -> Vec<u32> {
        let sys = System::new_all();
        let pid = Pid::from_u32(process_id);

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
}
