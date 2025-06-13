extern crate libprism;

use tracing_subscriber::{EnvFilter, FmtSubscriber};

fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    if let Err(_) = libprism::run() {
        eprintln!("prism exited with error");
        std::process::exit(1);
    }
}
