//! BSBM Benchmark for RDF Fusion
//! Measures planning vs execution time for SPARQL queries
//! Aligned with bachelor thesis: Tuning a Relational Optimizer for SPARQL Queries

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::execution::sparql::{OptimizationLevel, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase, NumProducts,
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

fn bsbm_business_intelligence_10000_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    bsbm_business_intelligence_10000(c, benchmarking_context);
}

fn bsbm_business_intelligence_10000(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();

    execute_benchmark(
        c,
        benchmarking_context,
        &|benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
          benchmark_context: BenchmarkContext| {
            let mut queries = BsbmBusinessIntelligenceQueryName::list_queries()
                .into_iter()
                .filter(|q|
                    *q != BsbmBusinessIntelligenceQueryName::Q4
                        && *q != BsbmBusinessIntelligenceQueryName::Q7
                )
                .map(|query_name| {
                    (
                        format!(
                            "BSBM BI 10000 (target_partitions={target_partitions}) - {query_name}"
                        ),
                        get_query_to_execute(benchmark.clone(), &benchmark_context, query_name),
                    )
                })
                .collect::<Vec<_>>();

            queries.push((
                format!(
                    "BSBM BI 10000 (target_partitions={target_partitions}) - Query 64"
                ),
                get_nth_query_to_execute(benchmark.clone(), &benchmark_context, 64),
            ));

            queries
        },
    )
}

criterion_group!(
    name = bsbm_business_intelligence;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(3));
    targets = bsbm_business_intelligence_10000_1_partition
);

criterion_main!(bsbm_business_intelligence);

fn execute_benchmark(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
    queries: &dyn Fn(
        BsbmBenchmark<BusinessIntelligenceUseCase>,
        BenchmarkContext,
    ) -> Vec<(String, SparqlRawOperation<BsbmBusinessIntelligenceQueryName>)>,
) {
    let verbose = is_verbose();
    let runtime = create_runtime(benchmarking_context.options().target_partitions.unwrap());

    let benchmark =
        BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(NumProducts::N10_000, None)
            .unwrap();

    let benchmark_name = benchmark.name();

    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context(
            "
Failed to prepare store. Have you downloaded the data?

Execute `just prepare-benches` before running benchmarks.
",
        )
        .unwrap();

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

        let profiles = [
            ("None", OptimizationLevel::None),
            ("Default", OptimizationLevel::Default),
            ("Full", OptimizationLevel::Full),
        ];

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

fn get_nth_query_to_execute(
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    n: usize,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(benchmark_context)
        .unwrap()
        .into_iter()
        .nth(n - 1)
        .unwrap()
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmBusinessIntelligenceQueryName,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(benchmark_context)
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}