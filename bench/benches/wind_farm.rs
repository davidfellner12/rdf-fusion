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
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};

fn opts(level: OptimizationLevel, max_passes: Option<usize>) -> QueryOptions {
    QueryOptions {
        optimization_level: level,
        max_optimizer_passes: max_passes,
    }
}

#[derive(Clone)]
struct CsvCollector(Arc<Mutex<Vec<CsvRow>>>);

struct CsvRow {
    query:   String,
    phase:   &'static str,
    level:   &'static str,
    passes:  u8,
    mean_ms: f64,
}

impl CsvCollector {
    fn new() -> Self { Self(Arc::new(Mutex::new(Vec::new()))) }

    fn measure<F: FnMut()>(
        &self, query: &str, phase: &'static str,
        level: &'static str, passes: u8, iters: usize, mut f: F,
    ) {
        let mut total = Duration::ZERO;
        for _ in 0..iters { let t = Instant::now(); f(); total += t.elapsed(); }
        let mean_ms = total.as_secs_f64() * 1000.0 / iters as f64;
        self.0.lock().unwrap().push(CsvRow { query: query.to_owned(), phase, level, passes, mean_ms });
    }

    fn write_csv(&self, path: &str) {
        use std::fmt::Write as _;
        let rows = self.0.lock().unwrap();

        let queries: Vec<String> = {
            let mut seen = std::collections::LinkedList::new();
            let mut set  = std::collections::HashSet::new();
            for r in rows.iter() { if set.insert(r.query.clone()) { seen.push_back(r.query.clone()); } }
            seen.into_iter().collect()
        };

        let get = |q: &str, phase: &str, level: &str, passes: u8| -> String {
            rows.iter()
                .find(|r| r.query == q && r.phase == phase && r.level == level && r.passes == passes)
                .map(|r| format!("{:.3}", r.mean_ms))
                .unwrap_or_default()
        };
        let none_exec = |q: &str| -> Option<f64> {
            rows.iter().find(|r| r.query == q && r.phase == "Execution" && r.level == "None").map(|r| r.mean_ms)
        };
        let pct = |base: f64, new: &str| -> String {
            new.parse::<f64>().ok().map(|v| format!("{:+.1}%", (base - v) / base * 100.0)).unwrap_or_default()
        };

        let mut out = String::new();

        writeln!(out, "=== Optimization: None ===").unwrap();
        writeln!(out, "Query,Planning None (ms),Execution None (ms)").unwrap();
        for q in &queries {
            writeln!(out, "{},{},{}", q, get(q,"Planning","None",1), get(q,"Execution","None",1)).unwrap();
        }

        writeln!(out, "\n=== Optimization: Default ===").unwrap();
        writeln!(out, "Query,Planning Default (ms) (1),Planning Default (ms) (2),Planning Default (ms) (3),Execution Default (ms) (1),Execution Default (ms) (2),Execution Default (ms) (3),Delta% Exec vs None").unwrap();
        for q in &queries {
            let e1 = get(q,"Execution","Default",1);
            let delta = none_exec(q).map(|b| pct(b,&e1)).unwrap_or_default();
            writeln!(out, "{},{},{},{},{},{},{},{}", q,
                     get(q,"Planning","Default",1), get(q,"Planning","Default",2), get(q,"Planning","Default",3),
                     e1, get(q,"Execution","Default",2), get(q,"Execution","Default",3), delta).unwrap();
        }

        writeln!(out, "\n=== Optimization: Full ===").unwrap();
        writeln!(out, "Query,Planning Full (ms) (1),Planning Full (ms) (2),Planning Full (ms) (3),Execution Full (ms) (1),Execution Full (ms) (2),Execution Full (ms) (3),Delta% Exec vs None").unwrap();
        for q in &queries {
            let e1 = get(q,"Execution","Full",1);
            let delta = none_exec(q).map(|b| pct(b,&e1)).unwrap_or_default();
            writeln!(out, "{},{},{},{},{},{},{},{}", q,
                     get(q,"Planning","Full",1), get(q,"Planning","Full",2), get(q,"Planning","Full",3),
                     e1, get(q,"Execution","Full",2), get(q,"Execution","Full",3), delta).unwrap();
        }

        std::fs::write(path, out).expect("Failed to write CSV");
        println!("\n✓ CSV written to {path}");
    }
}

fn wind_farm_16_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    wind_farm_16(c, benchmarking_context);
}

fn wind_farm_16(c: &mut Criterion, benchmarking_context: RdfFusionBenchContext) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();

    execute_benchmark(
        c,
        benchmarking_context,
        &|benchmark: WindFarmBenchmark, benchmark_context: BenchmarkContext| {
            let disabled_queries = vec![
                WindFarmQueryName::MultiGrouped1,
                WindFarmQueryName::MultiGrouped2,
                WindFarmQueryName::MultiGrouped3,
                WindFarmQueryName::MultiGrouped4,
            ];

            WindFarmQueryName::list_queries()
                .into_iter()
                .map(|query_name| {
                    (
                        format!("Wind Farm 16 (target_partitions={target_partitions}) - {query_name}"),
                        get_wind_farm_raw_sparql_operation(&benchmark_context, query_name).unwrap(),
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
    let collector = CsvCollector::new();

    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context("\nFailed to prepare store.\n\nExecute `just prepare-benches` before running benchmarks.\n")
        .unwrap();

    let profiles: &[(&'static str, OptimizationLevel, Option<usize>, u8)] = &[
        ("None",    OptimizationLevel::None,    None,    1),
        ("Default", OptimizationLevel::Default, Some(1), 1),
        ("Default", OptimizationLevel::Default, Some(2), 2),
        ("Default", OptimizationLevel::Default, Some(3), 3),
        ("Full",    OptimizationLevel::Full,    Some(1), 1),
        ("Full",    OptimizationLevel::Full,    Some(2), 2),
        ("Full",    OptimizationLevel::Full,    Some(3), 3),
    ];

    const TIMING_ITERS: usize = 20;

    for (benchmark_name, query) in queries(benchmark, benchmark_context) {
        if verbose {
            runtime.block_on(print_query_details(
                &store, opts(OptimizationLevel::Default, None),
                &query.query_name().to_string(), query.text(),
            )).unwrap();
        }

        for &(level_name, level, max_passes, passes) in profiles {
            c.bench_function(&format!("Planning {level_name} passes={passes}: {benchmark_name}"), |b| {
                b.to_async(&runtime).iter(|| async {
                    let result = store.query_opt(query.text(), opts(level, max_passes)).await;
                    assert!(result.is_ok());
                });
            });

            c.bench_function(&format!("Execution {level_name} passes={passes}: {benchmark_name}"), |b| {
                b.to_async(&runtime).iter(|| async {
                    let result = store.query_opt(query.text(), opts(level, max_passes)).await.unwrap();
                    consume_results(result).await.unwrap();
                });
            });

            {
                let col = collector.clone();
                let q_text = query.text().to_owned();
                let bench_name = benchmark_name.clone();

                col.measure(&bench_name, "Planning", level_name, passes, TIMING_ITERS, || {
                    runtime.block_on(async {
                        store.query_opt(&q_text, opts(level, max_passes)).await.unwrap();
                    });
                });
                col.measure(&bench_name, "Execution", level_name, passes, TIMING_ITERS, || {
                    runtime.block_on(async {
                        let result = store.query_opt(&q_text, opts(level, max_passes)).await.unwrap();
                        consume_results(result).await.unwrap();
                    });
                });
            }
        }
    }

    collector.write_csv("wind_farm_results.csv");
}