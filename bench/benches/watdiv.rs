//! WatDiv Benchmark for RDF Fusion

mod utils;

use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::execution::sparql::{OptimizationLevel, QueryOptions};
use rdf_fusion_bench::benchmarks::watdiv::get_watdiv_raw_sparql_operation;
use rdf_fusion_bench::benchmarks::watdiv::{WatDivBenchmark, WatDivQueryName};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use rdf_fusion_bench::benchmarks::Benchmark;
use std::path::PathBuf;
use std::time::Duration;

fn opts(level: OptimizationLevel, max_passes: Option<usize>) -> QueryOptions {
    QueryOptions {
        optimization_level: level,
        max_optimizer_passes: max_passes,
    }
}

fn watdiv_1_partition(c: &mut Criterion) {
    let ctx = RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    watdiv_bench(c, ctx);
}

fn watdiv_bench(c: &mut Criterion, benchmarking_context: RdfFusionBenchContext) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();
    let runtime = create_runtime(target_partitions);

    let benchmark = WatDivBenchmark::new(None);
    let benchmark_name = benchmark.name();
    let bench_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&bench_context))
        .context("Failed to prepare store. Place watdiv.nt in data/watdiv/ and .sparql files in data/watdiv_queries/queries/.")
        .unwrap();

    let profiles: &[(&'static str, OptimizationLevel, Option<usize>, u8)] = &[
        ("None_1", OptimizationLevel::None, Some(1), 1),
        ("None_2", OptimizationLevel::None, Some(2), 2),
        ("None_3", OptimizationLevel::None, Some(3), 3),

        ("Default_1", OptimizationLevel::Default, Some(1), 1),
        ("Default_2", OptimizationLevel::Default, Some(2), 2),
        ("Default_3", OptimizationLevel::Default, Some(3), 3),

        ("Full_1", OptimizationLevel::Full, Some(1), 1),
        ("Full_2", OptimizationLevel::Full, Some(2), 2),
        ("Full_3", OptimizationLevel::Full, Some(3), 3),
    ];

    for query_name in WatDivQueryName::list_queries() {
        let label = format!(
            "WatDiv (target_partitions={target_partitions}) - {query_name}"
        );

        let operation =
            get_watdiv_raw_sparql_operation(&bench_context, query_name).unwrap();

        for &(level_name, level, max_passes, passes) in profiles {
            c.bench_function(
                &format!("Planning {level_name} passes={passes}: {label}"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let result = store
                            .query_opt(operation.text(), opts(level, max_passes))
                            .await;
                        assert!(result.is_ok());
                    });
                },
            );

            c.bench_function(
                &format!("Execution {level_name} passes={passes}: {label}"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let result = store
                            .query_opt(operation.text(), opts(level, max_passes))
                            .await
                            .unwrap();
                        consume_results(result).await.unwrap();
                    });
                },
            );
        }
    }
}

criterion_group!(
    name = watdiv;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(30));
    targets = watdiv_1_partition
);

criterion_main!(watdiv);