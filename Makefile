debug:
	cargo build --all
	RUST_LOG=libprism=debug RUST_BACKTRACE=full ./target/debug/prism-proxy default.toml

release:
	cargo build --all --release
	RUST_LOG=libprism=info RUST_BACKTRACE=full ./target/release/prism-proxy default.toml

test:
	cargo test --all

bench:
	cargo bench

clean:
	cargo clean

metrics:
	cargo build --manifest-path ./libprism/Cargo.toml --features metrics