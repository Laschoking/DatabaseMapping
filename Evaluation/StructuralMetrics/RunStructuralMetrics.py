from src.Classes.QuantileAnchorElements import QuantileAnchorElements
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.StructuralSimilarityMetrics.FactPairSimilarity import FactPairSimilarity
from src.StructuralSimilarityMetrics.FactSimilarity import FactSimilarity
from src.StructuralSimilarityMetrics.DegreeSimilarity import DegreeSimilarity
from src.Config_Files.Setup import run_mappings_on_dbs





if __name__ == "__main__":
    #########################################################
    # Important parameters:
    # Set this higher if all results should be computed several times (since they are non-deelementinistic)
    RUN_NR = [1, 2, 3]  # 3,4,5
    USE = 'structural-evaluation'
    RES_TABLE = 'StructuralResults'

    # Set Anchor Quantile to 0, so the cartesian product is expanded (all possible combinations)
    q_0 = QuantileAnchorElements(0)

    # Set Expansion Strategies
    stat_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=False)
    dyn_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=True)
    expansions = [stat_cross_product, dyn_cross_product]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric str_weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    imp_alphas = [0, 0.1, 0.2]
    metrics = [FactSimilarity(imp_alpha=w) for w in imp_alphas]
    metrics += [FactPairSimilarity(imp_alpha=w) for w in imp_alphas]
    metrics += [DegreeSimilarity(imp_alpha=w) for w in imp_alphas]

    run_mappings_on_dbs(USE, RES_TABLE, expansions, metrics,nr_runs=RUN_NR)

