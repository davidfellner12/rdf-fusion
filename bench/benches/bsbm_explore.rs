//! Runs the queries from the BSBM explore workload.

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use crate::utils::{consume_results, create_runtime};
use anyhow::Context;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::execution::sparql::{OptimizationLevel, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmExploreQueryName, ExploreUseCase, NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;
use std::time::Duration;

fn opts(level: OptimizationLevel, max_passes: Option<usize>) -> QueryOptions {
    QueryOptions {
        optimization_level: level,
        max_optimizer_passes: max_passes,
    }
}

struct CsvRow {
    query:     String,
    phase:     String,
    level:     String,
    passes:    u8,
    mean_ms:   f64,
    stddev_ms: f64,
}

fn load_criterion_results(criterion_dir: &str) -> Vec<CsvRow> {
    let dir = std::path::Path::new(criterion_dir);
    if !dir.exists() {
        eprintln!("Warning: Criterion output dir not found: {criterion_dir}");
        return Vec::new();
    }

    let mut rows = Vec::new();

    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let bench_name = entry.file_name().to_string_lossy().to_string();
        let estimates_path = entry.path().join("new").join("estimates.json");
        if !estimates_path.exists() {
            continue;
        }

        let Some(colon_pos) = bench_name.find(": ") else { continue };
        let prefix = &bench_name[..colon_pos];
        let query  = bench_name[colon_pos + 2..].to_string();

        let parts: Vec<&str> = prefix.splitn(3, ' ').collect();
        if parts.len() < 3 { continue }
        let phase = parts[0].to_string();
        let level = parts[1].to_string();
        let passes: u8 = parts[2]
            .strip_prefix("passes=")
            .and_then(|p| p.parse().ok())
            .unwrap_or(1);

        if phase != "Planning" && phase != "Execution" { continue }

        let Ok(json_str) = std::fs::read_to_string(&estimates_path) else { continue };
        let Ok(parsed)   = serde_json::from_str::<serde_json::Value>(&json_str) else { continue };

        let mean_ms   = parsed["mean"]["point_estimate"].as_f64().unwrap_or(0.0) / 1_000_000.0;
        let stddev_ms = parsed["std_dev"]["point_estimate"].as_f64().unwrap_or(0.0) / 1_000_000.0;

        rows.push(CsvRow { query, phase, level, passes, mean_ms, stddev_ms });
    }

    rows
}

fn write_csv(rows: &[CsvRow], path: &str) {
    use std::fmt::Write as _;

    let queries: Vec<String> = {
        let mut seen = std::collections::HashSet::new();
        let mut ordered = Vec::new();
        for r in rows {
            if seen.insert(r.query.clone()) {
                ordered.push(r.query.clone());
            }
        }
        ordered
    };

    let get = |q: &str, phase: &str, level: &str, passes: u8| -> String {
        rows.iter()
            .find(|r| r.query == q && r.phase == phase && r.level == level && r.passes == passes)
            .map(|r| format!("{:.3} ±{:.3}", r.mean_ms, r.stddev_ms))
            .unwrap_or_default()
    };
    let get_val = |q: &str, phase: &str, level: &str, passes: u8| -> Option<f64> {
        rows.iter()
            .find(|r| r.query == q && r.phase == phase && r.level == level && r.passes == passes)
            .map(|r| r.mean_ms)
    };
    let pct = |base: f64, new: f64| -> String {
        format!("{:+.1}%", (base - new) / base * 100.0)
    };

    let mut out = String::new();
    
    writeln!(out, "=== Optimization: None ===").unwrap();
    writeln!(out, "Query,Planning None (ms),Execution None (ms)").unwrap();
    for q in &queries {
        writeln!(out, "{},{},{}",
                 q,
                 get(q, "Planning",  "None", 1),
                 get(q, "Execution", "None", 1),
        ).unwrap();
    }
    
    for level in ["Default", "Full"] {
        writeln!(out, "\n=== Optimization: {level} ===").unwrap();
        writeln!(out,
                 "Query,\
             Planning {level} (ms) (1),Planning {level} (ms) (2),Planning {level} (ms) (3),\
             Execution {level} (ms) (1),Execution {level} (ms) (2),Execution {level} (ms) (3),\
             Delta% Exec vs None,\
             ExecOnly (ms) (p=1)"
        ).unwrap();
        for q in &queries {
            let none_e = get_val(q, "Execution", "None",  1);
            let e1     = get_val(q, "Execution", level,   1);
            let p1     = get_val(q, "Planning",  level,   1);
            let delta  = none_e.zip(e1).map(|(b, n)| pct(b, n)).unwrap_or_default();
            let exec_only = e1.zip(p1)
                .map(|(e, p)| format!("{:.3}", e - p))
                .unwrap_or_default();
            writeln!(out, "{},{},{},{},{},{},{},{},{}",
                     q,
                     get(q, "Planning",  level, 1),
                     get(q, "Planning",  level, 2),
                     get(q, "Planning",  level, 3),
                     get(q, "Execution", level, 1),
                     get(q, "Execution", level, 2),
                     get(q, "Execution", level, 3),
                     delta,
                     exec_only,
            ).unwrap();
        }
    }

    std::fs::write(path, out).expect("Failed to write CSV");
    println!("\n✓ CSV written to {path}");
}

fn bsbm_explore_10000_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    bsbm_explore_10000(c, benchmarking_context);
}

fn bsbm_explore_10000(c: &mut Criterion, benchmarking_context: RdfFusionBenchContext) {
    let target_partitions = benchmarking_context.options().target_partitions.unwrap();

    execute_benchmark(
        c,
        benchmarking_context,
        &|benchmark: BsbmBenchmark<ExploreUseCase>, benchmark_context: BenchmarkContext| {
            BsbmExploreQueryName::list_queries()
                .into_iter()
                .map(|query_name| {
                    (
                        format!("BSBM Explore 10000 (target_partitions={target_partitions}) - {query_name}"),
                        get_query_to_execute(benchmark.clone(), &benchmark_context, query_name),
                    )
                })
                .collect::<Vec<_>>()
        },
    )
}

criterion_group!(
    name = bsbm_explore;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(30));
    targets = bsbm_explore_10000_1_partition
);

criterion_main!(bsbm_explore);

fn execute_benchmark(
    c: &mut Criterion,
    benchmarking_context: RdfFusionBenchContext,
    queries: &dyn Fn(
        BsbmBenchmark<ExploreUseCase>,
        BenchmarkContext,
    ) -> Vec<(String, SparqlRawOperation<BsbmExploreQueryName>)>,
) {
    let verbose = is_verbose();
    let runtime = create_runtime(benchmarking_context.options().target_partitions.unwrap());

    let benchmark = BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();
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

    for (benchmark_name, query) in queries(benchmark, benchmark_context) {
        if verbose {
            runtime.block_on(print_query_details(
                &store,
                opts(OptimizationLevel::Default, None),
                &query.query_name().to_string(),
                query.text(),
            )).unwrap();
        }

        for &(level_name, level, max_passes, passes) in profiles {
            // Planning only — lazy stream is not consumed, so only planning is measured
            c.bench_function(
                &format!("Planning {level_name} passes={passes}: {benchmark_name}"),
                |b| {
                    b.to_async(&runtime).iter(|| async {
                        let result = store.query_opt(query.text(), opts(level, max_passes)).await;
                        assert!(result.is_ok());
                    });
                },
            );

            // End-to-end — planning + full execution
            c.bench_function(
                &format!("Execution {level_name} passes={passes}: {benchmark_name}"),
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

    let rows = load_criterion_results("target/criterion");
    write_csv(&rows, "bsbm_explore_results.csv");
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<ExploreUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmExploreQueryName,
) -> SparqlRawOperation<BsbmExploreQueryName> {
    benchmark
        .list_raw_operations(benchmark_context)
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}