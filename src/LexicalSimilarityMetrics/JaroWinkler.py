from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.jaro_winkler import JaroWinkler as JW


class JaroWinkler(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("JaroWinkler")

    def compute_lexical_similarity(self, term_name1, term_name2):
        jarowinkler = JW()
        score = jarowinkler.similarity(term_name1, term_name2)
        return score