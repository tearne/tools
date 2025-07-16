use std::{process::Command, str::from_utf8};

use nvml_wrapper::{struct_wrappers::device::ProcessUtilizationSample, Device, Nvml};
use color_eyre::{eyre::{bail, Context, ContextCompat}, Result};
use sysinfo::System;

use crate::process::get_pid_tree;

pub struct GpuDevices<'a>(Vec<Device<'a>>);

pub struct Usage{
    pub percent: u32,
    pub last_seen_timestamp: u64
}

pub struct GpuApi {
    nvml: Nvml,
}

impl GpuApi {
    pub fn new() -> Result<Self> {
        let bytes = Command::new("lspci").output().unwrap().stdout;
        let stdout = from_utf8(&bytes).unwrap();
        if stdout.contains("NVIDIA") {
            log::info!("`lspci`, confirms existence of a GPU");
        } else {
            bail!("`lspci` did not confirm the presence of a GPU")
        }

        Ok(Self { nvml: Nvml::init()? })
    }

    pub fn build_devices<'a>(&'a self) -> Result<GpuDevices<'a>> {
        let num_devices = self.nvml.device_count()?;
        let devices = (0..num_devices)
            .map(|idx|self.nvml.device_by_index(idx).wrap_err("Device initialisation failure"))
            .collect::<Result<Vec<Device<'a>>>>()?;
        
        Ok(GpuDevices(devices))
    }

    fn get_all_utilisation(
        &self,
        devices: &GpuDevices,
        last_seen_timestamp: Option<u64>,
    ) -> Result<Vec<ProcessUtilizationSample>>
    {
        let stats: Result<Vec<ProcessUtilizationSample>> = devices.0
            .iter()
            .map(|d|{
                d.process_utilization_stats(last_seen_timestamp)
                    .wrap_err("Failed to get device utilisation sample")
            })
            .try_fold(Vec::<ProcessUtilizationSample>::new(), |mut acc, res_samples| -> Result<_>{
                acc.extend(res_samples?);
                Result::Ok(acc)
            });

        stats
    }

    pub fn get_pid_utilisation(
        &self,
        devices: &GpuDevices,
        pid: u32,
        last_seen_timestamp: Option<u64>,
        sys: &System
    ) -> Result<Usage> {
        let children = get_pid_tree(pid, sys);
        log::trace!(
            "Process {} has Children {:?}",
            pid,
            children
        );

        let all_utilisation = self.get_all_utilisation(devices, last_seen_timestamp)?;
        let max_timestamp: u64 = all_utilisation.iter()
            .max_by_key(|sample| sample.timestamp)
            .map(|sample| sample.timestamp)
            .wrap_err("Failed to identify max timestamp from GPU process utilisation data.")?;

        let sum = all_utilisation
            .iter()
            .filter_map(|p_sample|
                match children.contains(&p_sample.pid) {
                    true => Some(p_sample.sm_util),
                    false => None,
                }
            )
            .sum();

        Ok(Usage { 
            percent: sum, 
            last_seen_timestamp: max_timestamp
        })
    }

}
