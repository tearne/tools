use env_logger::fmt::Timestamp;
use nvml_wrapper::{Nvml, error::NvmlError, struct_wrappers::device::ProcessUtilizationSample};
use sysinfo::{Pid, System, ThreadKind};

use crate::gpu;

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
        device_id: Option<u32>,
    ) -> Vec<Vec<ProcessUtilizationSample>>
    where
        T: Into<Option<u64>>,
        T: Clone,
    {
        let mut all_utilisation = Vec::new();
        match device_id {
            Some(device_id) => {
                let device = self.nvml.device_by_index(device_id).unwrap();
                all_utilisation.push(
                    device
                        .process_utilization_stats(last_seen_timestamp.clone())
                        .unwrap(),
                )
            }
            None => {
                let num_devices = self.nvml.device_count().unwrap();
                println!("You have {} GPU devices", num_devices);
                let mut all_utilisation = Vec::new();
                for idx in 0..num_devices {
                    let device = self.nvml.device_by_index(idx).unwrap();
                    all_utilisation.push(
                        match device.process_utilization_stats(last_seen_timestamp.clone()) {
                            Ok(result) => result,
                            Err(e) => match e {
                                NvmlError::Uninitialized => panic!("{e}"),
                                _ => {
                                    println!("{e}");
                                    continue;
                                }
                            },
                        },
                    )
                }
            }
        }
        println!("{:#?}", all_utilisation);
        return all_utilisation;
    }

    pub fn get_process_utilisation(
        &self,
        process_pid: u32,
        all_gpu_utilisation: &Vec<Vec<ProcessUtilizationSample>>,
        device_id: Option<u32>,
        timestamp: Option<u64>,
    ) -> (u32, u64, u32) {
        let children = Self::get_children(process_pid);
        log::trace!(
            "Process {} has Children {:?}",
            process_pid,
            &children.iter().map(|pid| pid).collect::<Vec<_>>()
        );
        let child_utilisation: u32 = Self::get_children(process_pid)
            .into_iter()
            .map(|child| {
                Self::get_process_utilisation(
                    &self,
                    child,
                    all_gpu_utilisation,
                    device_id,
                    timestamp,
                )
                .2
            })
            .sum();

        let mut process_utilisation: u32 = 0;
        for device_id in 0..all_gpu_utilisation.size_of() {
            for gpu_process in all_gpu_utilisation[device_id] {
                if process_pid == gpu_process.pid {
                    let device_id = device_id;
                    let timestamp = gpu_process.timestamp;
                    let process_utilisation = gpu_process.sm_util;
                }
            }
        }
        return (device_id, timestamp, process_utilisation + child_utilisation)
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
