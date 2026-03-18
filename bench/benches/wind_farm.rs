//! Runs the queries from the Wind Farm Benchmark.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::execution::sparql::{OptimizationLevel, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::windfarm::{
    NumTurbines, WindFarmBenchmark, WindFarmQueryName, get_wind_farm_raw_sparql_operation,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;
use std::time::Duration;

fn opts(level: OptimizationLevel) -> QueryOptions {
    QueryOptions {
        optimization_level: level,
    }
}

fn wind_farm_16_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    wind_farm_16(c, benchmarking_context);
}

fn wind_farm_16(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();

    execute_benchmark(
        c,
        benchmarking_context,
        &|benchmark: WindFarmBenchmark,
          benchmark_context: BenchmarkContext| {
            let disabled_queries = vec![
                WindFarmQueryName::MultiGrouped1,
                WindFarmQueryName::MultiGrouped2,
                WindFarmQueryName::MultiGrouped3,
                WindFarmQueryName::MultiGrouped4,
            ];

            WindFarmQueryName::list_queries()
                .into_iter()
                .filter(|q| !disabled_queries.contains(q))
                .map(|query_name| {
                    (
                        format!(
                            "Wind Farm 16 (target_partitions={target_partitions}) - {query_name}"
                        ),
                        get_wind_farm_raw_sparql_operation(&benchmark_context, query_name)
                            .unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        },
    )
}

criterion_group!(
    name = wind_farm;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(3));
    targets = wind_farm_16_1_partition
);

criterion_main!(wind_farm);

fn execute_benchmark(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
    queries: &dyn Fn(
        WindFarmBenchmark,
        BenchmarkContext,
    ) -> Vec<(String, SparqlRawOperation<WindFarmQueryName>)>,
) {
    let verbose = is_verbose();
    let runtime = create_runtime(benchmarking_context.options().target_partitions.unwrap());

    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);
    let benchmark_name = benchmark.name();

    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context(
            "
Failed to prepare store.

Execute `just prepare-benches` before running benchmarks.
",
        )
        .unwrap();

    let profiles = [
        ("None", OptimizationLevel::None),
        ("Default", OptimizationLevel::Default),
        ("Full", OptimizationLevel::Full),
    ];

    for (benchmark_name, query) in queries(benchmark, benchmark_context) {
        if verbose {
            runtime
                .block_on(print_query_details(
                    &store,
                    opts(OptimizationLevel::Default),
                    &query.query_name().to_string(),
                    query.text(),
                ))
                .unwrap();
        }

        for (name, level) in profiles {
            c.bench_function(&format!("Planning {name}: {benchmark_name}"), |b| {
                b.to_async(&runtime).iter(|| async {
                    let result = store
                        .query_opt(query.text(), opts(level))
                        .await;
                    assert!(result.is_ok());
                });
            });

            c.bench_function(&format!("Execution {name}: {benchmark_name}"), |b| {
                b.to_async(&runtime).iter(|| async {
                    let result = store
                        .query_opt(query.text(), opts(level))
                        .await
                        .unwrap();
                    consume_results(result).await.unwrap();
                });
            });
        }
    }
}