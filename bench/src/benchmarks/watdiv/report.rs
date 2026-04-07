use crate::report::BenchmarkReport;
use crate::benchmarks::watdiv::WatDivQueryName;
use crate::runs::BenchmarkRun;
use std::collections::HashMap;
use std::path::Path;
use std::fs;

pub struct WatDivReport {
    runs: HashMap<WatDivQueryName, BenchmarkRun>,
}

pub struct WatDivReportBuilder {
    runs: HashMap<WatDivQueryName, BenchmarkRun>,
}

impl WatDivReportBuilder {
    pub fn new() -> Self {
        Self { runs: HashMap::new() }
    }

    pub fn add_run(&mut self, query_name: WatDivQueryName, run: BenchmarkRun) {
        self.runs.insert(query_name, run);
    }

    pub fn build(self) -> WatDivReport {
        WatDivReport { runs: self.runs }
    }
}

impl BenchmarkReport for WatDivReport {
    fn write_results(&self, results_dir: &Path) -> anyhow::Result<()> {
        let mut csv = String::from("query,duration_ms\n");
        for (query_name, run) in &self.runs {
            csv.push_str(&format!(
                "{},{}\n",
                query_name,
                run.duration.as_millis()
            ));
        }
        fs::write(results_dir.join("results.csv"), csv)?;
        Ok(())
    }
}