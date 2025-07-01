use nvml_wrapper::{Nvml, error::NvmlError};
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

    pub fn check_usage_all(&self, process_id: u32) {
        let num_devices = self.nvml.device_count().unwrap();
        println!("You have {} GPU devices", num_devices);
    
        // I think that it's actually a child process of the process given
        // by `gpu_burn & $!`` that's directly utilising the gpu 
        let children = Self::get_children(process_id);

        // show me the children processes
        println!("Children: {:#?}", Self::get_children(process_id));


        'outer: for idx in 0..num_devices {
            let mut device = self.nvml.device_by_index(idx).unwrap();

            // enables "accounting" for the device (requires sudo) - appaently accounting is 
            // required for accounting_stats_for()
            println!("accounting enabled: {:#?}", device.is_accounting_enabled().unwrap());
            let _ = device.set_accounting(true).unwrap();
            println!("accounting enabled: {:#?}", device.is_accounting_enabled().unwrap());

            // lists the "accounting pids" - says there are none - don't understand this
            println!("accounting pids: {:#?}", device.accounting_pids().unwrap());
            for child in &children {

                // show me me the running compute processes - should be one with a pid matching one of the children
                println!("running compute processes: {:#?}", device.running_compute_processes().unwrap());

                // show me utilisation for "relevant currently running processes" 
                println!("some stats: {:#?}", device.process_utilization_stats(None).unwrap());

                // can't find "accounting stats" for the process - I guess
                // this is because it's not an "accounting process" but I don't know how
                // to set this.
                match device.accounting_stats_for(*child) {
                    Ok(stats) => {
                        match stats.gpu_utilization {
                            Some(usage) => {
                                println!("Usage for process {} = {}", *child, usage);
                            }
                            None => {
                                println!("Device.Utilization_rates() not supported for this device");
                            }
                        }
                        let _ = &self.nvml.device_by_index(idx).unwrap().set_accounting(false).unwrap();
                        break 'outer;
                    }
                    Err(e) => match e {
                        NvmlError::NotFound => {
                            if idx == num_devices-1 {
                                println!("Process {} not found", *child);
                            }
                            continue;
                        }
                        _ => {
                            panic!("{e}");
                        }
                    },
                }
            }
            let _ = &self.nvml.device_by_index(idx).unwrap().set_accounting(false).unwrap();
        }
        println!("...done");
    }

    fn get_children(process_id: u32) -> Vec<u32> {
        let sys = System::new_all();
        let pid = Pid::from_u32(process_id);

        sys.processes()
            .iter()
            .filter(|(_pid, process)| {
                let is_child = process.parent()
                    .map(|ppid| ppid == pid)
                    .unwrap_or(false);

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
