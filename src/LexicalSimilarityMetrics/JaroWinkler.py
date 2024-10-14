from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.jaro_winkler import JaroWinkler as JW


class JaroWinkler(LexicalSimilarityMetric):
    def __init__(self,imp_alpha=0):
        super().__init__("JaroWinkler",imp_alpha)
        self.jarowinkler = JW()

    def compute_lexical_similarity(self, element_name1, element_name2):
        element_name1 = element_name1.lower()
        element_name2 = element_name2.lower()
        score = self.jarowinkler.similarity(element_name1, element_name2)
        return score

