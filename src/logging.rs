use tracing::{info};
use tracing_subscriber::{
    Layer, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

pub fn init_logger(level_filter: LevelFilter) {
    let log_filename = "log/ruuvitag-listener.log";
    if !std::path::Path::new(log_filename).exists() {
        std::fs::create_dir_all("log").unwrap();
    }

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(true)
                .with_filter(level_filter)
        )
        .with(tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_filename)
                    .unwrap()
            ).with_filter(level_filter)).init();

    info!("Logger initialized");
}

pub fn init_logger_service(level_filter: LevelFilter) {
    let log_filename = "C:/temp/ruuvitag-listener.log";

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_filename)
                    .unwrap()
            ).with_filter(level_filter)).init();

    info!("Logger initialized");
}