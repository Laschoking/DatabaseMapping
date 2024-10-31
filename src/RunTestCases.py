from src.Classes.QuantileAnchorElements import QuantileAnchorElements
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.StructuralSimilarityMetrics.FactPairSimilarity import FactPairSimilarity
from src.Classes.SimilarityMetric import MixedSimilarityMetric
from src.LexicalSimilarityMetrics.Dice import Dice
from src.Config_Files.Setup import run_mappings_on_dbs



if __name__ == "__main__":

    #########################################################
    # Important parameters:
    RUN_NR = [1]
    USE = 'latex'

    # Set Expansion Strategies
    expansions = [IterativeAnchorExpansion(anchor_quantile=QuantileAnchorElements(0.8),DYNAMIC=True)]
    #expansions += [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=False) for q in quantiles]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric str_weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    # Use classical weight for both metrics
    fp_sim = FactPairSimilarity(imp_alpha=0.2)

    run_mappings_on_dbs(USE, 'Testcases', expansions, [fp_sim],nr_runs=RUN_NR)
