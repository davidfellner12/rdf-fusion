use crate::benchmarks::bsbm::NumProducts;
use crate::benchmarks::bsbm::operation::{
    BsbmOperation, BsbmRawOperation, list_raw_operations,
};
use crate::benchmarks::bsbm::report::{BsbmReport, ExploreReportBuilder, QueryDetails};
use crate::benchmarks::bsbm::requirements::{
    download_bsbm_tools, download_pre_generated_queries, generate_dataset_requirement,
};
use crate::benchmarks::bsbm::use_case::BsbmUseCase;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::prepare::PrepRequirement;
use crate::report::BenchmarkReport;
use crate::runs::BenchmarkRun;
use async_trait::async_trait;
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{Query, QueryOptions, QueryResults};
use std::fs;
use std::marker::PhantomData;
use std::path::PathBuf;
use tokio::time::Instant;

/// Holds file paths for the files required for executing a BSBM run.
struct BsbmFilePaths {
    /// A path to the dataset NTriples file.
    dataset: PathBuf,
    /// A path to the csv file that contains the pre-generated queries.
    queries: PathBuf,
}

/// The [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
/// is a widely adopted benchmark built around an e-commerce use case.
///
/// This struct implements the logic for preparing and executing a BSBM benchmark. For that, it
/// requires a concrete [BsbmUseCase] implementation.
pub struct BsbmBenchmark<TUseCase: BsbmUseCase> {
    /// The name of the benchmark.
    name: BenchmarkName,
    /// The number of products.
    num_products: NumProducts,
    /// How many queries to execute at most.
    max_query_count: Option<u64>,
    /// Path file.
    paths: BsbmFilePaths,
    /// The use case
    phantom_data: PhantomData<TUseCase>,
}

impl<TUseCase: BsbmUseCase> BsbmBenchmark<TUseCase> {
    /// Creates a new [BsbmBenchmark] with the given sizes.
    pub fn try_new(
        num_products: NumProducts,
        max_query_count: Option<u64>,
    ) -> anyhow::Result<Self> {
        let dataset_path = PathBuf::from("./dataset.nt".to_string());
        let queries_path = PathBuf::from("./queries.csv".to_string());
        let paths = BsbmFilePaths {
            dataset: dataset_path,
            queries: queries_path,
        };

        Ok(Self {
            name: TUseCase::name().into_benchmark_name(num_products, max_query_count),
            num_products,
            max_query_count,
            paths,
            phantom_data: PhantomData,
        })
    }

    /// The BSBM generator produces a list of queries that are tailored to the generated data. This
    /// method returns a list of these queries that should be executed during this run.
    fn list_operations(
        &self,
        ctx: &BenchmarkContext,
    ) -> anyhow::Result<Vec<BsbmOperation<TUseCase::QueryName>>> {
        println!("Loading queries ...");

        let queries_path = ctx.parent().join_data_dir(&self.paths.queries)?;
        let result = match self.max_query_count {
            None => list_raw_operations::<TUseCase::QueryName>(&queries_path)?
                .map(parse_query)
                .collect(),
            Some(max_query_count) => {
                list_raw_operations::<TUseCase::QueryName>(&queries_path)?
                    .map(parse_query)
                    .take(usize::try_from(max_query_count)?)
                    .collect()
            }
        };

        println!("Queries loaded.");
        Ok(result)
    }

    /// Loads the dataset file into the resulting [Store].
    async fn prepare_store(&self, ctx: &BenchmarkContext<'_>) -> anyhow::Result<Store> {
        println!("Creating in-memory store and loading data ...");

        let dataset_path = ctx.parent().join_data_dir(&self.paths.dataset)?;
        let data = fs::read(&dataset_path)?;
        let memory_store = Store::new();
        memory_store
            .load_from_reader(RdfFormat::NTriples, data.as_slice())
            .await?;
        println!("Store created and data loaded.");
        Ok(memory_store)
    }
}

#[async_trait]
impl<TUseCase: BsbmUseCase + 'static> Benchmark for BsbmBenchmark<TUseCase> {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    #[allow(clippy::expect_used)]
    fn requirements(&self) -> Vec<PrepRequirement> {
        vec![
            download_bsbm_tools(),
            generate_dataset_requirement(self.paths.dataset.clone(), self.num_products),
            download_pre_generated_queries(
                &TUseCase::name().to_string(),
                self.paths.queries.clone(),
                self.num_products,
            ),
        ]
    }

    async fn execute(
        &self,
        bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>> {
        let operations = self.list_operations(bench_context)?;
        let memory_store = self.prepare_store(bench_context).await?;
        let report =
            execute_benchmark::<TUseCase>(bench_context, operations, &memory_store)
                .await?;
        Ok(Box::new(report))
    }
}

/// Parses the SPARQL query by turning an [BsbmRawOperation] to an [BsbmOperation].
fn parse_query<TQueryName>(
    query: BsbmRawOperation<TQueryName>,
) -> BsbmOperation<TQueryName> {
    match query {
        BsbmRawOperation::Query(name, query) => BsbmOperation::Query(
            name,
            Query::parse(&query.replace(" #", ""), None).unwrap(),
        ),
    }
}

async fn execute_benchmark<TUseCase: BsbmUseCase>(
    context: &BenchmarkContext<'_>,
    operations: Vec<BsbmOperation<TUseCase::QueryName>>,
    memory_store: &Store,
) -> anyhow::Result<BsbmReport<TUseCase>> {
    println!("Evaluating queries ...");

    let mut report = ExploreReportBuilder::new();
    let len = operations.len();
    for (idx, operation) in operations.iter().enumerate() {
        if idx % 25 == 0 {
            println!("Progress: {idx}/{len}");
        }

        run_operation(context, &mut report, memory_store, operation).await?;
    }
    let report = report.build();

    println!("Progress: {len}/{len}");
    println!("All queries evaluated.");

    Ok(report)
}

/// Executes a single [BsbmOperation] and stores the results of the profiling in the `report`.
async fn run_operation<TUseCase: BsbmUseCase>(
    context: &BenchmarkContext<'_>,
    report: &mut ExploreReportBuilder<TUseCase>,
    store: &Store,
    operation: &BsbmOperation<TUseCase::QueryName>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let options = QueryOptions::default();
    let (name, explanation) = match &operation {
        BsbmOperation::Query(name, q) => {
            let (result, explanation) =
                store.explain_query_opt(q.clone(), options.clone()).await?;
            match result {
                QueryResults::Boolean(_) => (),
                QueryResults::Solutions(mut s) => {
                    while let Some(s) = s.next().await {
                        s?;
                    }
                }
                QueryResults::Graph(mut g) => {
                    while let Some(t) = g.next().await {
                        t?;
                    }
                }
            }
            (*name, explanation)
        }
    };

    let duration = start.elapsed();
    let run = BenchmarkRun { duration };
    report.add_run(name, run);
    if context.parent().options().verbose_results {
        let details = QueryDetails {
            query: operation.query().to_string(),
            query_type: operation.query_name().to_string(),
            total_time: duration,
            explanation,
        };
        report.add_explanation(details);
    }

    Ok(())
}
