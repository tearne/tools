use std::{process::Command, str::from_utf8};

use color_eyre::{
    Result,
    eyre::{Context, bail},
};
use nvml_wrapper::{
    Device, Nvml, error::NvmlError, struct_wrappers::device::ProcessUtilizationSample,
};
use sysinfo::Pid;

use crate::process::system::System;

pub struct Gpu<'a>{
    devices: Vec<Device<'a>>,
    last_sample_time: Option<u64>,
}
impl<'a> Gpu<'a> {
    pub fn new(api: &'a GpuApi) -> Result<Self> {
        let num_devices = api.nvml.device_count()?;
        let devices = (0..num_devices)
            .map(|idx| {
                api.nvml
                    .device_by_index(idx)
                    .wrap_err("Device initialisation failure")
            })
            .collect::<Result<Vec<Device<'a>>>>()?;

        log::debug!("Found devices: {:?}", &devices);

        Ok(Gpu{
            devices,
            last_sample_time: None,
        })
    }
}

pub struct GpuApi {
    nvml: Nvml,
}

impl GpuApi {
    pub fn new() -> Result<Self> {
        let bytes = Command::new("lspci")
            .output()
            .wrap_err("Failed to run `lspci`")?
            .stdout;
        let stdout = from_utf8(&bytes)?;
        if stdout.contains("NVIDIA") {
            log::debug!("`lspci`, confirms existence of a GPU");
        } else {
            bail!("`lspci` did not confirm the presence of a GPU")
        }

        Ok(Self {
            nvml: Nvml::init()?,
        })
    }

    fn get_all_utilisation(
        &self,
        gpu: &Gpu,
    ) -> Result<Vec<ProcessUtilizationSample>> {
        gpu.devices
            .iter()
            .map(|d|
                d.process_utilization_stats(gpu.last_sample_time).or_else(|e|{
                    match e {
                        // It's ok if we don't find the PID, just assume zero usage
                        NvmlError::NotFound => Ok(Vec::new()), 
                        // But if we get another error, that's serious enough to propagate
                        _ => Err(e).wrap_err_with(||"Unexpected NvmlError when querying usage")
                    }
                })
            )
            .try_fold(
                Vec::<ProcessUtilizationSample>::new(),
                |mut acc, res_samples| {
                    acc.extend(res_samples?);
                    Result::Ok(acc)
                },
            )
    }


    pub fn get_pid_utilisation(
        &self,
        gpu: &mut Gpu,
        pid: Pid,
        system: &mut System,
    ) -> Result<u32> {
        let children = system.get_pid_tree(pid, false);
        log::trace!("Process {} has Children {:?}", pid, children);

        let all_utilisation = self.get_all_utilisation(gpu)?;

        // Needed to keep track of when we last looked at GPU utilisation
        let max_timestamp: Option<u64> = all_utilisation
            .iter()
            .max_by_key(|sample| sample.timestamp)
            .map(|sample| sample.timestamp);

        gpu.last_sample_time = max_timestamp;

        //TODO sum is a percentage?
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

        Ok(sum)
    }
}
