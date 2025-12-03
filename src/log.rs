use log::LevelFilter;
use color_eyre::{Result, eyre::{Context, ContextCompat}};

pub fn setup_logging(level: u8) -> Result<(), color_eyre::eyre::Error> {
    fn set_log_level(local_level: LevelFilter, dep_level:  LevelFilter) -> Result<(), color_eyre::eyre::Error> {
        let prog: String = std::env::current_exe().wrap_err("Error getting current_exe")?
            .file_name().wrap_err("File path terminated in ..")?
            .to_str().wrap_err("utf-8 validity failed")?
            .to_owned();

        let crate_name: &'static str = env!("CARGO_CRATE_NAME");

        env_logger::builder()
            .filter_level(dep_level)
            .filter_module(&prog, local_level)
            .filter_module(crate_name, local_level)            
            .init();
        println!("Logging filter level for '{}' and '{}': {}", &prog, crate_name, local_level);
        println!("Dependency logging filter level: {}", dep_level);

        log::info!("Logging filter level for '{}' and '{}': {}", &prog, crate_name, local_level);
        log::info!("Dependency logging filter level: {}", dep_level);
        Ok(())
    }

    match level {
        0 => set_log_level(LevelFilter::Warn, LevelFilter::Warn)?,
        1 => set_log_level(LevelFilter::Info, LevelFilter::Warn)?,
        2 => set_log_level(LevelFilter::Debug, LevelFilter::Warn)?,
        3 => set_log_level(LevelFilter::Trace, LevelFilter::Info)?,
        _ => panic!("Too many levels of verbosity.  You can have up to 3."),
    };
    Ok(())
}