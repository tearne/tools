use log::LevelFilter;

pub fn setup_logging(level: u8) {
    fn set_log_level(local_level: LevelFilter, dep_level:  LevelFilter) {
        let prog: String = std::env::current_exe()
            .unwrap()
            .file_name().unwrap()
            .to_str().unwrap()
            .to_owned();

        let crate_name = env!("CARGO_CRATE_NAME");

        env_logger::builder()
            .filter_level(dep_level)
            .filter(Some(&prog), local_level)
            .filter(Some(&crate_name), local_level)
            .init();
        println!("Logging set to: {}", local_level);

        log::info!("Local executable name detected as {} for logging.", &prog);
        log::info!("Local crate name detected as {} for logging.", &crate_name);

        log::info!("Local log level set to {}.", local_level);
        log::info!("Default Log level set to {}.", dep_level);
    }

    match level {
        0 => set_log_level(LevelFilter::Warn, LevelFilter::Warn),
        1 => set_log_level(LevelFilter::Info, LevelFilter::Warn),
        2 => set_log_level(LevelFilter::Debug, LevelFilter::Warn),
        3 => set_log_level(LevelFilter::Trace, LevelFilter::Info),
        _ => panic!("Too many levels of verbosity.  You can have up to 3."),
    };
}