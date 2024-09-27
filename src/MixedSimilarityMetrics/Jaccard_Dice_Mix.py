from src.Classes.SimilarityMetric import SimilarityMetric
from src.LexicalSimilarityMetrics import Dice
from src.StructuralSimilarityMetrics import JaccardIndex


class JaccardDiceMix(SimilarityMetric):
    def __init__(self, str_ratio, metric_weight):
        super().__init__("Dynamic Jaccard", metric_weight)
        self.jaccard = JaccardIndex.JaccardIndex(metric_weight=metric_weight)
        self.dice = Dice.Dice(n=2, metric_weight=metric_weight)
        self.str_ratio = str_ratio

    def compute_similarity(self, term1, term2, sub_rec_tuples):
        str_sim = self.str_ratio * self.jaccard.compute_structural_similarity(term1, term2, sub_rec_tuples)
        lex_sim = (1 - self.str_ratio) * self.dice.compute_lexical_similarity(term1, term2)
        return str_sim + lex_sim
