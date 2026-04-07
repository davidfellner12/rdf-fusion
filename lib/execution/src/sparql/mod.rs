//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.

mod algebra;
pub mod error;
mod eval;
mod explanation;
mod optimizer;
mod rewriting;

pub use crate::sparql::algebra::{Query, QueryDataset, Update};
pub use crate::sparql::explanation::QueryExplanation;
pub use eval::evaluate_query;
pub use optimizer::{create_optimizer_rules, create_pyhsical_optimizer_rules};
pub use rdf_fusion_model::{Variable, VariableNameParseError};
pub use spargebra::SparqlSyntaxError;

/// Defines how many optimizations the query optimizer should apply.
///
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OptimizationLevel {
    /// No optimizations, except rewrites that are necessary for a working query.
    /// Uses DataFusion's default max_passes.
    None,
    /// A balanced default optimization level. Suitable for simple queries or those handling modest
    /// data volumes. Uses 1 optimizer pass.
    #[default]
    Default,
    /// Runs all optimizations. Ideal for complex queries or those processing large datasets.
    /// Uses 3 optimizer passes.
    Full,
}

impl OptimizationLevel {
    /// Returns the number of optimizer passes to use for this level.
    /// Returns `None` for `None` level, letting DataFusion use its own default.
    pub fn max_passes_from_env() -> Option<usize> {
        std::env::var("MAX_PASSES")
            .ok()
            .and_then(|v| v.parse().ok())
    }

    pub fn max_passes(&self) -> Option<usize> {
        match self {
            OptimizationLevel::None => None,
            OptimizationLevel::Default => Some(1),
            OptimizationLevel::Full => Some(3),
        }
    }
}

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// The defined optimization level
    pub optimization_level: OptimizationLevel,
    pub max_optimizer_passes: Option<usize>,
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions;

impl From<QueryOptions> for UpdateOptions {
    #[inline]
    fn from(_query_options: QueryOptions) -> Self {
        Self {}
    }
}
