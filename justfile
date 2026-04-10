# List available commands
default:
    @just --list

# --- Toolchain ---
configure-toolchain:
    rustup component add clippy rustfmt
    cargo install cargo-deny

configure-toolchain-ci: configure-toolchain
    cargo install cargo-codspeed

# --- Lint & formatting ---
lint:
    cargo fmt -- --check
    cargo clippy -- -D warnings -D clippy::all
    # cargo deny check

# --- Tests ---
test:
    cargo test --workspace --exclude rdf-fusion-examples

test-examples:
    cargo run --package rdf-fusion-examples --example custom_function
    cargo run --package rdf-fusion-examples --example custom_storage
    cargo run --package rdf-fusion-examples --example plan_builder
    cargo run --package rdf-fusion-examples --example query_store
    cargo run --package rdf-fusion-examples --example use_store

# --- Documentation ---
rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc

# --- Prepare benchmarks ---
[working-directory: '.']
prepare-benches-tests:
    cd bench && cargo run --profile test prepare bsbm-explore --num-products 1000
    cd bench && cargo run --profile test prepare wind-farm --num-turbines 4

[working-directory: '.']
prepare-benches: prepare-watdiv
    cd bench && cargo run --profile profiling-nonlto prepare bsbm-explore --num-products 10000
    cd bench && cargo run --profile profiling-nonlto prepare wind-farm --num-turbines 16

[working-directory: '.']
prepare-watdiv: check-watdiv-deps
    cd bench && cargo run --profile profiling-nonlto prepare wat-div

# Checks that the dependencies for building the WatDiv C++ binary are available.
# On Debian/Ubuntu: sudo apt install -y g++ make libboost-date-time-dev
# On Fedora:        sudo dnf install -y gcc-c++ make boost-date-time
check-watdiv-deps:
    @which g++ > /dev/null 2>&1 || (echo "Error: g++ not found. Debian/Ubuntu: sudo apt install g++  |  Fedora: sudo dnf install gcc-c++" && exit 1)
    @which make > /dev/null 2>&1 || (echo "Error: make not found. Debian/Ubuntu: sudo apt install make  |  Fedora: sudo dnf install make" && exit 1)

# --- Run benchmarks ---
bench-watdiv:
    cd bench && cargo bench --profile release-nonlto --bench watdiv

# --- Serve the RDF Fusion server ---
serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878

# --- Prepare release ---
prepare-release:
    #!/usr/bin/env bash
    if [[ `git status --porcelain` ]]; then \
        echo "The working directory is not clean. Commit ongoing work before creating a release archive."; \
        exit 1; \
    fi
    git archive --format=tar.gz -o target/rdf-fusion-source.tar.gz HEAD
    echo "Source archive created. Move the archive to a new folder and extract it. Then run just release."

release: lint prepare-benches-tests test test-examples rustdoc
    (cd lib/model && cargo publish)
    (cd lib/encoding && cargo publish)
    (cd lib/extensions && cargo publish)
    (cd lib/functions && cargo publish)
    (cd lib/logical && cargo publish)
    (cd lib/physical && cargo publish)
    (cd lib/storage && cargo publish)
    (cd lib/execution && cargo publish)
    (cd lib/rdf-fusion && cargo publish)
    (cd lib/web && cargo publish)
    (cd cli && cargo publish)
    (cd bench && cargo publish)
    echo "All crates released. Please rename the archive, upload the tarball to GitHub, and create a Git tag."