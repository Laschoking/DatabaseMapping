from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.jaro_winkler import JaroWinkler as JW


class JaroWinkler(LexicalSimilarityMetric):
    def __init__(self,metric_weight=1):
        super().__init__("JaroWinkler",metric_weight)
        self.jarowinkler = JW()

    def compute_lexical_similarity(self, term_name1, term_name2):
        term_name1 = term_name1.lower()
        term_name2 = term_name2.lower()
        score = self.jarowinkler.similarity(term_name1, term_name2)
        return score

