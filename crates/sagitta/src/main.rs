//! Arrow Flight server entry point.

use clap::Parser;
use sagitta::{Config, LoggingConfig, Sagitta};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Arrow Flight SQL server for data infrastructure.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the configuration file (TOML format).
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,
}

fn init_logging(config: &LoggingConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let registry = tracing_subscriber::registry().with(filter);

    if config.format == "json" {
        registry.with(fmt::layer().json()).init();
    } else {
        registry.with(fmt::layer()).init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = match args.config {
        Some(path) => Config::from_file(&path)?,
        None => Config::load()?,
    };

    init_logging(&config.logging);

    Sagitta::builder().config(config).serve().await
}
