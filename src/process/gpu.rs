use std::{process::Command, str::from_utf8};

use color_eyre::{
    Result,
    eyre::{self, Context, ContextCompat, bail},
};
use nvml_wrapper::{
    Device, Nvml, error::NvmlError, struct_wrappers::device::ProcessUtilizationSample,
};
use sysinfo::Pid;

use crate::process::system::System;

pub struct GpuDevices<'a>(Vec<Device<'a>>);

pub struct Usage {
    pub percent: u32,
    pub last_seen_timestamp: u64,
}

pub struct GpuApi {
    nvml: Nvml,
}

impl GpuApi {
    pub fn new() -> Result<Self> {
        let bytes = Command::new("lspci").output().unwrap().stdout;
        let stdout = from_utf8(&bytes).unwrap();
        if stdout.contains("NVIDIA") {
            log::debug!("`lspci`, confirms existence of a GPU");
        } else {
            bail!("`lspci` did not confirm the presence of a GPU")
        }

        Ok(Self {
            nvml: Nvml::init()?,
        })
    }

    pub fn build_devices<'a>(&'a self) -> Result<GpuDevices<'a>> {
        let num_devices = self.nvml.device_count()?;
        let devices = (0..num_devices)
            .map(|idx| {
                self.nvml
                    .device_by_index(idx)
                    .wrap_err("Device initialisation failure")
            })
            .collect::<Result<Vec<Device<'a>>>>()?;

        log::debug!("Found devices: {:?}", &devices);

        Ok(GpuDevices(devices))
    }

    fn get_all_utilisation(
        &self,
        devices: &GpuDevices,
        last_seen_timestamp: Option<u64>,
    ) -> std::result::Result<Vec<ProcessUtilizationSample>, NvmlError> {
        devices
            .0
            .iter()
            .map(|d| d.process_utilization_stats(last_seen_timestamp))
            .try_fold(
                Vec::<ProcessUtilizationSample>::new(),
                |mut acc, res_samples| -> std::result::Result<_, NvmlError> {
                    acc.extend(res_samples?);
                    Result::Ok(acc)
                },
            )
    }

    pub fn get_pid_utilisation(
        &self,
        devices: &GpuDevices,
        pid: Pid,
        last_seen_timestamp: Option<u64>,
        system: &mut System,
    ) -> Result<Option<Usage>> {
        let children = system.get_pid_tree(pid, false);
        log::trace!("Process {} has Children {:?}", pid, children);

        let timeout_seconds = 5;
        let pause_seconds = 1;
        let max_iterations = timeout_seconds / pause_seconds;
        let pause = std::time::Duration::from_secs(pause_seconds);
        let mut i = 0;
        let all_utilisation = 
            // before Nvml has detected a GPU PID
                loop {
                    match self.get_all_utilisation(devices, last_seen_timestamp) {
                        Ok(result) => break result,
                        Err(e) => match e {
                            NvmlError::NotFound => {
                                match last_seen_timestamp {
                                    None => {
                                        if i > max_iterations {
                                            return Err(eyre::eyre!(
                                                "Time out waiting for GPU process PID"
                                            ))
                                            .wrap_err("Failed to get device utilisation sample");
                                        }
                                        log::info!("Waiting for GPU process PID");
                                        i += 1;
                                        std::thread::sleep(pause);
                                        continue;
                                    }
                                    Some(_) => return Ok(None)
                                }
                            }
                            _ => return Err(e).wrap_err("Failed to get device utilisation sample"),
                        },
                    }
                };

        let max_timestamp: u64 = all_utilisation
            .iter()
            .max_by_key(|sample| sample.timestamp)
            .map(|sample| sample.timestamp)
            .wrap_err("Failed to identify max timestamp from GPU process utilisation data.")?;

        let sum = all_utilisation
            .iter()
            .filter_map(
                |p_sample| match children.contains(&Pid::from_u32(p_sample.pid)) {
                    true => {
                        log::info!("{} -> {:?}", p_sample.pid, p_sample);
                        Some(p_sample.sm_util)
                    }
                    false => None,
                },
            )
            .sum();

        Ok(Some(Usage {
            percent: sum,
            last_seen_timestamp: max_timestamp,
        }))
    }
}
