use crate::sparql::OptimizationLevel;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_logical::expr::SimplifySparqlExpressionsRule;
use rdf_fusion_logical::extend::ExtendLoweringRule;
use rdf_fusion_logical::join::SparqlJoinLoweringRule;
use rdf_fusion_logical::minus::MinusLoweringRule;
use rdf_fusion_logical::paths::PropertyPathLoweringRule;
use rdf_fusion_logical::patterns::PatternLoweringRule;
use rdf_fusion_physical::join::NestedLoopJoinProjectionPushDown;
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

            rules.extend(lowering_rules);

            /*rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.encodings().clone(),
                Arc::clone(context.functions()),
            )));*/

            // DataFusion Optimizers
            // TODO: Replace with a good subset
            rules.extend(create_essential_datafusion_optimizers());
            //rules.push(Arc::new(EliminateNestedUnion::new()));
            rules.push(Arc::new(SimplifyExpressions::new()));
            //rules.push(Arc::new(ReplaceDistinctWithAggregate::new()));
            //(rules).push(Arc::new(ExtractEquijoinPredicate::new()));
            //rules.push(Arc::new(DecorrelatePredicateSubquery::new()));
            //rules.push(Arc::new(ScalarSubqueryToJoin::new()));
            //rules.push(Arc::new(DecorrelateLateralJoin::new()));
            rules.push(Arc::new(ExtractEquijoinPredicate::new()));
            //rules.push(Arc::new(EliminateDuplicatedExpr::new()));
            //rules.push(Arc::new(EliminateFilter::new()));
            rules.push(Arc::new(EliminateCrossJoin::new()));
            //rules.push(Arc::new(EliminateLimit::new()));
            //rules.push(Arc::new(PropagateEmptyRelation::new()));

            // Must be after PropagateEmptyRelation
            //Arc::new(EliminateOneUnion::new()),
            //Arc::new(FilterNullJoinKeys::default()),
            //Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            //Arc::new(PushDownLimit::new()),
            //Arc::new(PushDownFilter::new()),
            //Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules


            //rules.push(Arc::new(EliminateGroupByConstant::new()));
            rules.push(Arc::new(CommonSubexprEliminate::new()));
            rules.push(Arc::new(OptimizeProjections::new()));
            /*rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.encodings().clone(),
                Arc::clone(context.functions()),
            )));*/
            rules
        }
        OptimizationLevel::Full => {
            let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();

            rules.extend(lowering_rules);
            rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.encodings().clone(),
                Arc::clone(context.functions()),
            )));

            rules.extend(Optimizer::default().rules);

            rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.encodings().clone(),
                Arc::clone(context.functions()),
            )));
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

/// Creates a list of optimizer rules based on the given `optimization_level`.
pub fn create_pyhsical_optimizer_rules(
    _optimization_level: OptimizationLevel,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    // TODO: build based on optimization level
    let mut rules = PhysicalOptimizer::default().rules;
    rules.push(Arc::new(NestedLoopJoinProjectionPushDown::new()));
    rules
}
