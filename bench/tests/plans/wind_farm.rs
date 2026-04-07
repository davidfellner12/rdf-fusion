    use crate::plans::{consume_result, run_plan_assertions};
    use anyhow::Context;
    use datafusion::physical_plan::displayable;
    use insta::assert_snapshot;
    use rdf_fusion::execution::sparql::{OptimizationLevel, QueryExplanation, QueryOptions};
    use rdf_fusion_bench::benchmarks::Benchmark;
    use rdf_fusion_bench::benchmarks::windfarm::{
        NumTurbines, WindFarmBenchmark, WindFarmQueryName, get_wind_farm_raw_sparql_operation,
    };
    use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
    use rdf_fusion_bench::operation::SparqlRawOperation;
    use std::path::PathBuf;

    #[tokio::test]
    pub async fn optimized_logical_plan_wind_farm() {
        for_all_explanations(|name, explanation| {
            assert_snapshot!(
                format!("{name} (Optimized)"),
                &explanation.optimized_logical_plan.to_string()
            )
        })
            .await;
    }

    #[tokio::test]
    pub async fn execution_plan_wind_farm() {
        for_all_explanations(|name, explanation| {
            let string = displayable(explanation.execution_plan.as_ref())
                .indent(false)
                .to_string();
            assert_snapshot!(format!("{name} (Execution Plan)"), &string)
        })
            .await;
    }

    #[tokio::test]
    pub async fn optimized_physical_plan_wind_farm() {
        use datafusion::physical_plan::displayable;
        use rdf_fusion::execution::sparql::{create_pyhsical_optimizer_rules, OptimizationLevel};

        for_all_explanations(|name, explanation| {
            let mut plan = explanation.execution_plan.clone();
            let rules = create_pyhsical_optimizer_rules(OptimizationLevel::Default);
            for rule in rules {
                plan = rule.optimize(plan.clone(), &Default::default()).unwrap();
            }
            let plan_string = displayable(plan.as_ref()).indent(false).to_string();
            assert_snapshot!(format!("{name} (Optimized Physical Plan)"), &plan_string);
        })
            .await;
    }

    #[tokio::test]
    pub async fn optimizer_passes_comparison_wind_farm() {
        let benchmarking_context =
            RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);

        let benchmark = WindFarmBenchmark::new(NumTurbines::N4);
        let benchmark_context = benchmarking_context
            .create_benchmark_context(benchmark.name())
            .unwrap();

        let store = benchmark.prepare_store(&benchmark_context).await.unwrap();

        let ignored = [
            WindFarmQueryName::MultiGrouped1,
            WindFarmQueryName::MultiGrouped2,
            WindFarmQueryName::MultiGrouped3,
            WindFarmQueryName::MultiGrouped4,
        ];

        let mut any_difference = false;
        let pairs = [(1usize, 2usize), (1, 3), (2, 3)];

        println!("\n{}", "=".repeat(60));
        println!("BENCHMARK: Wind Farm");
        println!("{}", "=".repeat(60));

        for query_name in WindFarmQueryName::list_queries() {
            if ignored.contains(&query_name) {
                continue;
            }

            let query = get_query_to_execute(&benchmark_context, query_name);

            println!("\n  Query: {query_name}");
            println!("  {}", "-".repeat(40));

            let mut plans: Vec<(usize, String)> = Vec::new();
            for passes in [1, 2, 3] {
                let (result, explanation) = store
                    .explain_query_opt(
                        query.text(),
                        QueryOptions {
                            optimization_level: OptimizationLevel::Default,
                            max_optimizer_passes: Some(passes),
                        },
                    )
                    .await
                    .unwrap();
                consume_result(result).await;
                plans.push((passes, explanation.optimized_logical_plan.to_string()));
            }

            for (a, b) in pairs {
                let plan_a = plans.iter().find(|(p, _)| *p == a).map(|(_, s)| s);
                let plan_b = plans.iter().find(|(p, _)| *p == b).map(|(_, s)| s);

                if plan_a != plan_b {
                    any_difference = true;
                    println!("  [CHANGED] passes={a} vs passes={b} differ");
                } else {
                    println!("  [OK]      passes={a} vs passes={b} identical");
                }
            }
        }

        println!("\n{}", "=".repeat(60));
        if any_difference {
            println!("RESULT Wind Farm: plans CHANGED");
        } else {
            println!("RESULT Wind Farm: plans IDENTICAL");
        }
        println!("{}", "=".repeat(60));
    }

    async fn for_all_explanations(assertion: impl Fn(String, QueryExplanation) -> ()) {
        let benchmarking_context =
            RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);

        let benchmark = WindFarmBenchmark::new(NumTurbines::N4);
        let benchmark_name = benchmark.name();
        let benchmark_context = benchmarking_context
            .create_benchmark_context(benchmark_name)
            .unwrap();

        let store = benchmark.prepare_store(&benchmark_context).await.unwrap();

        let ignored = [
            WindFarmQueryName::MultiGrouped1,
            WindFarmQueryName::MultiGrouped2,
            WindFarmQueryName::MultiGrouped3,
            WindFarmQueryName::MultiGrouped4,
        ];

        for query_name in WindFarmQueryName::list_queries() {
            if ignored.contains(&query_name) {
                continue;
            }

            let benchmark_name = format!("Wind Farm - {query_name}");
            let query = get_query_to_execute(&benchmark_context, query_name);

            let (result, explanation) = store
                .explain_query_opt(query.text(), QueryOptions::default())
                .await
                .unwrap();
            consume_result(result).await;

            run_plan_assertions(|| assertion(benchmark_name, explanation));
        }
    }

    fn get_query_to_execute(
        benchmark_context: &BenchmarkContext,
        query_name: WindFarmQueryName,
    ) -> SparqlRawOperation<WindFarmQueryName> {
        get_wind_farm_raw_sparql_operation(benchmark_context, query_name)
            .context("Could not list raw operations for Wind Farm benchmark. Have you prepared a wind-farm-4 dataset?")
            .unwrap()
    }