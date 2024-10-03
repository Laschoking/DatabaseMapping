from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.StructuralSimilarityMetrics.DynamicRecordTupleCount import DynamicRecordTupleCount
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.StructuralSimilarityMetrics.NodeDegree import NodeDegree
from src.Config_Files.Setup import run_mappings_on_dbs





if __name__ == "__main__":
    #########################################################
    # Important parameters:
    # Set this higher if all results should be computed several times (since they are non-deterministic)
    RUN_NR = [1, 2, 3]  # 3,4,5
    USE = 'structural-evaluation'
    RES_TABLE = 'StructuralResults'

    # Set Anchor Quantile to 0, so the cartesian product is expanded (all possible combinations)
    q_0 = QuantileAnchorTerms(0)

    # Set Expansion Strategies
    stat_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=False)
    dyn_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=True)
    expansions = [stat_cross_product, dyn_cross_product]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric str_weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    metric_weights = [0.8, 0.9, 1]
    metrics = [JaccardIndex(metric_weight=w) for w in metric_weights]
    metrics += [DynamicRecordTupleCount(metric_weight=w) for w in metric_weights]
    metrics += [NodeDegree(metric_weight=w) for w in metric_weights]

    run_mappings_on_dbs(USE, RES_TABLE, expansions, metrics,nr_runs=RUN_NR)

