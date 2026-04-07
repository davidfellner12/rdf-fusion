# List available commands
default:
    @just --list


configure-toolchain:
    rustup component add clippy rustfmt
    cargo install cargo-deny

configure-toolchain-ci: configure-toolchain
    cargo install cargo-codspeed


lint:
    cargo fmt -- --check
    cargo clippy -- -D warnings -D clippy::all
    # cargo deny check

test:
    cargo test --workspace --exclude rdf-fusion-examples

test-examples:
    cargo run --package rdf-fusion-examples --example custom_function
    cargo run --package rdf-fusion-examples --example custom_storage
    cargo run --package rdf-fusion-examples --example plan_builder
    cargo run --package rdf-fusion-examples --example query_store
    cargo run --package rdf-fusion-examples --example use_store

rustdoc:
    RUSTDOCFLAGS="-D warnings" cargo doc


[working-directory: '.']
prepare-benches-tests:
    cd bench && cargo run --profile test prepare bsbm-explore --num-products 1000
    cd bench && cargo run --profile test prepare wind-farm --num-turbines 4

[working-directory: '.']
prepare-benches: prepare-watdiv
    cd bench && cargo run --profile profiling-nonlto prepare bsbm-explore --num-products 10000
    cd bench && cargo run --profile profiling-nonlto prepare wind-farm --num-turbines 16


[working-directory: '.']
prepare-watdiv:
    docker image inspect watdiv-gen > /dev/null 2>&1 || \
        sudo docker build -t watdiv-gen ./bench/benches/watdiv_queries/queries/watdiv-docker/

    mkdir -p bench/data/watdiv
    sudo docker run --rm -v $(pwd)/bench/data/watdiv:/output watdiv-gen -s 1 -q 1 -r 1

    mkdir -p bench/data/watdiv_queries/queries
    for txt in bench/data/watdiv/queries/*.txt; do \
        template=$(basename "$txt" .txt); \
        cp "$txt" "bench/data/watdiv_queries/queries/${template}.sparql"; \
    done


bench-watdiv:
    cd bench && cargo bench --profile release-nonlto --bench watdiv

serve-dbg:
    cargo run --bin rdf-fusion -- serve --bind 0.0.0.0:7878

serve:
    RUSTFLAGS="-C target-cpu=native" cargo run --profile profiling --bin rdf-fusion -- serve --bind 0.0.0.0:7878


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