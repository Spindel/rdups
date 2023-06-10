help:
	@echo "build-linux       - Build for linux"
	@echo "build-linux-musl  - Build for linux (static)"

build-linux:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc cargo build --release --target=x86_64-unknown-linux-gnu

build-linux-musl:
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-musl-gcc cargo build --release --target=x86_64-unknown-linux-musl