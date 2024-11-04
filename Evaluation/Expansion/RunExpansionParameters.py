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
    USE = 'expansion-same-lib%'
    #RES_TABLE = 'ExpansionResults_MIX_No_Gamma'

    # Setup 3 Anchor values: (this will expand the 10/5/2% of elements with the highest degree)
    quantiles = [QuantileAnchorElements(0.95)] #[QuantileAnchorElements(0.9), QuantileAnchorElements(0.95), QuantileAnchorElements(0.98)]

    # Set Expansion Strategies
    expansions = [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=True) for q in quantiles]
    #expansions += [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=False) for q in quantiles]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric str_weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics

    alpha_weights = [0,0.1,0.2]
    ratios = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

    # Use classical weight for both metrics
    fp_sim = FactPairSimilarity(imp_alpha=0.2)
    dice = Dice(n=2,imp_alpha=0.1)
    metrics = [MixedSimilarityMetric(struct_ratio=s, lex_metric=dice, imp_alpha=0, struct_metric=fp_sim) for s in ratios]
    run_mappings_on_dbs(USE, 'ExpansionResults', expansions, metrics,nr_runs=RUN_NR)

    # Use no importance weight for both metrics
    fp_sim = FactPairSimilarity(imp_alpha=0)  # best value 0.2
    dice = Dice(n=2, imp_alpha=0)  # best value 0.1
    metrics = [MixedSimilarityMetric(struct_ratio=s, lex_metric=dice, imp_alpha=0, struct_metric=fp_sim) for s in
               ratios]
    run_mappings_on_dbs(USE, 'ExpansionResults_MIX_No_Gamma', expansions, metrics, nr_runs=RUN_NR)

