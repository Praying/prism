extern crate libprism;

#[tokio::main]
async fn main() {
    if let Err(e) = libprism::run().await {
        eprintln!("prism exited with error: {}", e);
        std::process::exit(1);
    }
}
