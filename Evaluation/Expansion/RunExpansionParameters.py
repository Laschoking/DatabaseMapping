from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.StructuralSimilarityMetrics.FactSimilarity import FactSimilarity
from src.StructuralSimilarityMetrics.FactPairSimilarity import FactPairSimilarity
from src.Classes.SimilarityMetric import MixedSimilarityMetric
from src.LexicalSimilarityMetrics.Dice import Dice
from src.Config_Files.Setup import run_mappings_on_dbs



if __name__ == "__main__":

    #########################################################
    # Important parameters:
    RUN_NR = [1,2,3]
    USE = 'expansion%'
    RES_TABLE = 'ExpansionResults_MIX_Weight1'

    # Setup 3 Anchor values: (this will expand the 10/5/2% of constants with the highest degree)
    quantiles = [QuantileAnchorTerms(0.95)] #[QuantileAnchorTerms(0.9), QuantileAnchorTerms(0.95), QuantileAnchorTerms(0.98)]

    # Set Expansion Strategies
    expansions = [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=True) for q in quantiles]
    #expansions += [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=False) for q in quantiles]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric str_weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics

    lex_weights = [0.8, 0.9, 1]
    fp_sim = FactPairSimilarity(imp_alpha=0) # best value 0.2
    fact_sim = FactSimilarity(imp_alpha=0) # best value 0.1
    dice = Dice(n=2,imp_alpha=0) # best value 0.1
    # [fact_sim,edge] +
    #metrics = [Dice(n=2, imp_alpha=w) for w in lex_weights]

    ratios = [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]
    metrics = [MixedSimilarityMetric(struct_metric=fact_sim,lex_metric=dice, struct_ratio=s,imp_alpha=0) for s in ratios]
    metrics += [MixedSimilarityMetric(struct_metric=fp_sim,lex_metric=dice, struct_ratio=s,imp_alpha=0) for s in ratios]


    run_mappings_on_dbs(USE, RES_TABLE, expansions, metrics,nr_runs=RUN_NR)


# TODO run without any imp_alpha