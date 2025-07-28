use crate::sparql::OptimizationLevel;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Optimizer, OptimizerRule};
use rdf_fusion_api::RdfFusionContextView;
use rdf_fusion_logical::expr::SimplifySparqlExpressionsRule;
use rdf_fusion_logical::extend::ExtendLoweringRule;
use rdf_fusion_logical::join::{SparqlJoinLoweringRule, SparqlJoinReorderingRule};
use rdf_fusion_logical::minus::MinusLoweringRule;
use rdf_fusion_logical::paths::PropertyPathLoweringRule;
use rdf_fusion_logical::patterns::PatternLoweringRule;
use std::sync::Arc;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_lateral_join::DecorrelateLateralJoin;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_nested_union::EliminateNestedUnion;
use datafusion::optimizer::eliminate_one_union::EliminateOneUnion;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;

/// Creates a list of optimizer rules based on the given `optimization_level`.
/// old optimazations used see google docs
pub fn create_optimizer_rules(
    context: RdfFusionContextView,
    optimization_level: OptimizationLevel,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let lowering_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(MinusLoweringRule::new(context.clone())),
        Arc::new(ExtendLoweringRule::new()),
        Arc::new(PropertyPathLoweringRule::new(context.clone())),
        Arc::new(SparqlJoinLoweringRule::new(context.clone())),
        Arc::new(PatternLoweringRule::new(context.clone())),
    ];

    match optimization_level {
        OptimizationLevel::None => {
            let mut rules = Vec::new();
            rules.extend(lowering_rules);
            rules.extend(create_essential_datafusion_optimizers());
            rules
        }
        OptimizationLevel::Default => {
            let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
            rules.push(Arc::new(SparqlJoinReorderingRule::new(
                context.encodings().clone(),
            )));
            rules.extend(lowering_rules);

            // DataFusion Optimizers
            // TODO: Replace with a good subset
            rules.push(Arc::new(SimplifySparqlExpressionsRule::new()));
            rules.extend(create_essential_datafusion_optimizers());
            rules.extend(create_default_optimizers());
            rules
        }
        OptimizationLevel::Full => {
            let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
            rules.push(Arc::new(SparqlJoinReorderingRule::new(
                context.encodings().clone(),
            )));
            rules.extend(lowering_rules);
            rules.extend(Optimizer::default().rules);
            rules.push(Arc::new(SimplifySparqlExpressionsRule::new()));
            rules
        }
    }
}

fn create_essential_datafusion_optimizers() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
    ]
}

fn create_default_optimizers() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![
        Arc::new(EliminateNestedUnion::new()),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(DecorrelateLateralJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        // Must be after PropagateEmptyRelation
        Arc::new(EliminateOneUnion::new()),
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        Arc::new(PushDownLimit::new()),
        Arc::new(PushDownFilter::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        Arc::new(EliminateGroupByConstant::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(OptimizeProjections::new())
    ]
}
