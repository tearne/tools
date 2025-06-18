use nvml_wrapper::{Device, Nvml};


struct Gpu {
    nvml: Nvml,
}
impl Gpu {
    pub fn init() -> Option<Gpu> {
        match Nvml::init() {
            Ok(nvml) => Some(Gpu{nvml}),
            Err(e) => {
                println!("Gpu didn't initialise: {}", e);
                None
            },
        }
    }

    pub fn get_usage_all(&self) -> f64 {
        let num_devices = self.nvml.device_count().unwrap();
        println!("You have {} GPU devices", num_devices);

        for idx in 0..num_devices {
            let usage = Self::get_usage_device(&self.nvml.device_by_index(idx).unwrap());
            println!("Usage for device {} = {}", idx, usage);
        }

        todo!()
    }

    fn get_usage_device(device: &Device) -> u32 {
        let t = device.utilization_rates().unwrap();
        t.gpu
    }
}