mod utils;

use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::execution::sparql::{OptimizationLevel, QueryOptions};
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase, NumProducts,
};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;
use std::time::Duration;
use rdf_fusion_bench::benchmarks::Benchmark;

fn opts(level: OptimizationLevel, max_passes: Option<usize>) -> QueryOptions {
    QueryOptions {
        optimization_level: level,
        max_optimizer_passes: max_passes,
    }
}

const PROFILES: &[(&str, OptimizationLevel, Option<usize>, u8)] = &[
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

const BENCH_QUERY_PREFIXES: &[&str] = &["Q7", "Q8"];

fn bsbm_business_intelligence_10000_1_partition(c: &mut Criterion) {
    let ctx = RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    bsbm_business_intelligence_10000(c, ctx);
}

fn bsbm_business_intelligence_10000(c: &mut Criterion, ctx: RdfFusionBenchContext) {
    let runtime = create_runtime(ctx.options().target_partitions.unwrap());

    let benchmark = BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(
        NumProducts::N10_000,
        None,
    )
        .unwrap();

    let benchmark_name = benchmark.name();
    let bench_context = ctx.create_benchmark_context(benchmark_name).unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&bench_context))
        .context("Failed to prepare store")
        .unwrap();

    let queries = BsbmBusinessIntelligenceQueryName::list_queries()
        .into_iter()
        .map(|q| {
            let name = q.to_string();

            let op = benchmark
                .list_raw_operations(&bench_context)
                .unwrap()
                .into_iter()
                .find(|op| op.query_name() == q)
                .unwrap();

            (name, op)
        })
        .filter(|(name, _)| {
            BENCH_QUERY_PREFIXES
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .collect::<Vec<_>>();

    for (name, query) in queries {
        for &(level_name, level, max_passes, passes) in PROFILES {
            if name.starts_with("Q7") && passes == 1 {
                continue;
            }

            c.bench_function(
                &format!("Planning {level_name} passes={passes}: {name}"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let result = store
                            .query_opt(query.text(), opts(level, max_passes))
                            .await;

                        assert!(result.is_ok(), "{:?}", result.err());
                    });
                },
            );

            c.bench_function(
                &format!("Execution {level_name} passes={passes}: {name}"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let result = store
                            .query_opt(query.text(), opts(level, max_passes))
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
    name = bsbm_business_intelligence;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(30));
    targets = bsbm_business_intelligence_10000_1_partition
);

criterion_main!(bsbm_business_intelligence);