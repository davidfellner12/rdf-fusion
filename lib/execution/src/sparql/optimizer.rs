    use crate::sparql::OptimizationLevel;

    use datafusion::optimizer::{OptimizerRule};
    use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
    use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
    use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
    use datafusion::optimizer::eliminate_join::EliminateJoin;
    use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
    use datafusion::optimizer::optimize_projections::OptimizeProjections;
    use datafusion::optimizer::push_down_filter::PushDownFilter;
    use datafusion::optimizer::push_down_limit::PushDownLimit;
    use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
    use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;

    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
    use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
    use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
    use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
    //use datafusion::physical_optimizer::coalesce_partitions::CoalescePartitions;

    use rdf_fusion_extensions::RdfFusionContextView;

    use rdf_fusion_logical::expr::SimplifySparqlExpressionsRule;
    use rdf_fusion_logical::extend::ExtendLoweringRule;
    use rdf_fusion_logical::join::SparqlJoinLoweringRule;
    use rdf_fusion_logical::minus::MinusLoweringRule;
    use rdf_fusion_logical::paths::PropertyPathLoweringRule;
    use rdf_fusion_logical::patterns::PatternLoweringRule;

    use std::sync::Arc;
    use datafusion::optimizer::decorrelate_lateral_join::DecorrelateLateralJoin;
    use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
    use datafusion::optimizer::eliminate_filter::EliminateFilter;
    use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
    use datafusion::optimizer::eliminate_limit::EliminateLimit;
    use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
    use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
    use datafusion::optimizer::optimize_unions::OptimizeUnions;
    use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
    use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
    use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;

    /// Creates optimizer rules for logical query plan
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
                rules.push(Arc::new(SimplifyExpressions::new()));
                rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                    context.encodings().clone(),
                    Arc::clone(context.functions()),
                )));
                //FOR bsbm_explore, but not for bsbm_business_intelligence
                //rules.push(Arc::new(ReplaceDistinctWithAggregate::new()));

                rules
            }

            OptimizationLevel::Default => {
                let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();

                rules.extend(lowering_rules);
                rules.push(Arc::new(SimplifyExpressions::new()));
                rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                    context.encodings().clone(),
                    Arc::clone(context.functions()),
                )));
                //FOR bsbm_explore, but not for bsbm_business_intelligence
                //rules.push(Arc::new(ReplaceDistinctWithAggregate::new()));

                rules.push(Arc::new(ReplaceDistinctWithAggregate::new()));
                rules.push(Arc::new(DecorrelatePredicateSubquery::new()));
                rules.push(Arc::new(ScalarSubqueryToJoin::new()));

                rules.push(Arc::new(ExtractEquijoinPredicate::new()));
                rules.push(Arc::new(EliminateCrossJoin::new()));
                rules.push(Arc::new(EliminateJoin::new()));

                rules.push(Arc::new(PushDownFilter::new()));
                rules.push(Arc::new(PushDownLimit::new()));

                rules.push(Arc::new(CommonSubexprEliminate::new()));
                rules.push(Arc::new(OptimizeProjections::new()));

                rules
            }

            OptimizationLevel::Full => {
                let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
                rules.extend(lowering_rules);

                rules.push(Arc::new(SimplifyExpressions::new()));
                rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                    context.encodings().clone(),
                    Arc::clone(context.functions()),
                )));

                rules.push(Arc::new(OptimizeUnions::new()));
                rules.push(Arc::new(ReplaceDistinctWithAggregate::new()));
                rules.push(Arc::new(EliminateJoin::new()));
                rules.push(Arc::new(DecorrelatePredicateSubquery::new()));
                rules.push(Arc::new(ScalarSubqueryToJoin::new()));
                rules.push(Arc::new(DecorrelateLateralJoin::new()));
                rules.push(Arc::new(ExtractEquijoinPredicate::new()));
                rules.push(Arc::new(EliminateDuplicatedExpr::new()));
                rules.push(Arc::new(EliminateFilter::new()));
                rules.push(Arc::new(EliminateCrossJoin::new()));
                rules.push(Arc::new(EliminateLimit::new()));
                rules.push(Arc::new(PropagateEmptyRelation::new()));
                rules.push(Arc::new(FilterNullJoinKeys::default()));
                rules.push(Arc::new(EliminateOuterJoin::new()));
                rules.push(Arc::new(PushDownLimit::new()));
                rules.push(Arc::new(PushDownFilter::new()));
                rules.push(Arc::new(SingleDistinctToGroupBy::new()));
                rules.push(Arc::new(EliminateGroupByConstant::new()));
                rules.push(Arc::new(CommonSubexprEliminate::new()));
                rules.push(Arc::new(OptimizeProjections::new()));
                rules
            }
        }
    }

    /// Creates physical optimizer rules
    pub fn create_pyhsical_optimizer_rules(
        optimization_level: OptimizationLevel
    ) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {

        match optimization_level {

            OptimizationLevel::None =>  vec![
                Arc::new(EnforceDistribution::new()),
                Arc::new(SanityCheckPlan::new()),
            ],

            OptimizationLevel::Default => {

                vec![
                    Arc::new(EnforceDistribution::new()),
                    Arc::new(ProjectionPushdown::new()),
                    Arc::new(FilterPushdown::new_post_optimization()),
                    Arc::new(SanityCheckPlan::new()),
                ]
            }

            OptimizationLevel::Full => {
                vec![
                    Arc::new(EnforceDistribution::new()),
                    Arc::new(ProjectionPushdown::new()),
                    Arc::new(FilterPushdown::new_post_optimization()),
                    Arc::new(AggregateStatistics::new()),
                    Arc::new(CombinePartialFinalAggregate::new()),
                    Arc::new(SanityCheckPlan::new()),
                ]
            }
        }
    }