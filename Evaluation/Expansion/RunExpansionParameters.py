import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.Classes.SimilarityMetric import MixedSimilarityMetric
from src.LexicalSimilarityMetrics.Dice import Dice
from src.Config_Files.Setup import run_mappings_on_dbs



if __name__ == "__main__":

    #########################################################
    # Important parameters:
    RUN_NR = [1]
    USE = 'expansion-same-lib'
    RES_TABLE = 'ExpansionResults_New'

    # Setup 3 Anchor values: (this will expand the 10/5/2% of constants with the highest degree)
    q = QuantileAnchorTerms(0.95)

    # Set Expansion Strategies
    expansions = [IterativeAnchorExpansion(anchor_quantile=q,DYNAMIC=True)]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    weight = 0.8
    ratios = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]

    jaccard = JaccardIndex(metric_weight=weight)
    dice = Dice(n=2, metric_weight=weight)
    metrics = [MixedSimilarityMetric(struct_metric=jaccard,lex_metric=dice,
                                     str_ratio=s,metric_weight=1) for s in ratios]


    run_mappings_on_dbs(USE, RES_TABLE, expansions, metrics,nr_runs=RUN_NR)

