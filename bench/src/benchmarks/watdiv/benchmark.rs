use crate::benchmarks::watdiv::queries::WatDivQueryName;
use crate::benchmarks::watdiv::report::{WatDivReport, WatDivReportBuilder};
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::operation::SparqlRawOperation;
use crate::prepare::PrepRequirement;
use crate::report::BenchmarkReport;
use crate::utils::print_store_stats;
use anyhow::Context;
use async_trait::async_trait;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use std::fs;
use std::path::{Path, PathBuf};
use crate::benchmarks::watdiv::prepare::{compile_watdiv, download_watdiv_tarball, generate_watdiv_data, generate_watdiv_queries};

struct WatDivFilePaths {
    data_file: PathBuf,
    query_folder: PathBuf,
}

pub struct WatDivBenchmark {
    name: BenchmarkName,
    max_query_count: Option<u64>,
}

impl WatDivBenchmark {
    pub fn new(max_query_count: Option<u64>) -> Self {
        let name = BenchmarkName::WatDiv { max_query_count };
        Self { name, max_query_count }
    }

    pub async fn prepare_store(
        &self,
        ctx: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Store> {
        println!("Creating in-memory store and loading WatDiv data ...");
        let files = create_files(ctx)?;
        let store = ctx.parent().create_store();

        let data = fs::read(&files.data_file)
            .context(format!("Could not read WatDiv data: {}", files.data_file.display()))?;
        store.load_from_reader(RdfFormat::NTriples, data.as_slice()).await?;

        print_store_stats(&store).await?;
        println!("WatDiv store ready.");
        Ok(store)
    }
}

#[async_trait]
impl Benchmark for WatDivBenchmark {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    fn requirements(&self, _bench_files_path: &Path) -> Vec<PrepRequirement> {
        vec![
            download_watdiv_tarball(),
            compile_watdiv(),
            generate_watdiv_data(),
            generate_watdiv_queries()
        ]
    }

    async fn execute(
        &self,
        bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>> {
        let store = self.prepare_store(bench_context).await?;
        let report = execute_benchmark(bench_context, &store, self.max_query_count).await?;
        Ok(Box::new(report))
    }
}

async fn execute_benchmark(
    context: &BenchmarkContext<'_>,
    store: &Store,
    max_query_count: Option<u64>,
) -> anyhow::Result<WatDivReport> {
    println!("Evaluating WatDiv queries ...");
    let mut report = WatDivReportBuilder::new();

    let all_queries = WatDivQueryName::list_queries();
    let queries: Vec<_> = match max_query_count {
        Some(n) => all_queries.into_iter().take(n as usize).collect(),
        None => all_queries,
    };

    for query_name in queries {
        println!("Executing query: {query_name}");
        let operation = get_watdiv_raw_sparql_operation(context, query_name)?;
        let (run, _explanation, _num_results) =
            operation.parse().unwrap().run(store).await?;
        report.add_run(query_name, run);
    }

    println!("All WatDiv queries evaluated.");
    Ok(report.build())
}

pub fn get_watdiv_raw_sparql_operation(
    context: &BenchmarkContext<'_>,
    query_name: WatDivQueryName,
) -> anyhow::Result<SparqlRawOperation<WatDivQueryName>> {
    let files = create_files(context)?;
    let query_file = files.query_folder.join(query_name.file_name());
    let query = fs::read_to_string(&query_file).context(format!(
        "Could not read query file: {}",
        query_file.display()
    ))?;
    Ok(SparqlRawOperation::Query(query_name, query))
}

fn create_files(ctx: &BenchmarkContext) -> anyhow::Result<WatDivFilePaths> {
    let data_file = ctx.parent().join_data_dir(Path::new("watdiv.nt"))?;
    let query_folder = ctx.parent().join_data_dir(Path::new("../watdiv_queries/queries"))?;
    Ok(WatDivFilePaths { data_file, query_folder })
}