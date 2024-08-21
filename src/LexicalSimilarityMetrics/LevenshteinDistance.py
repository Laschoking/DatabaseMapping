import difflib
from src.Classes.SimilarityMetric import SimilarityMetric
import Levenshtein as LS


class LevenshteinDistance(SimilarityMetric):
    def __init__(self):
        super().__init__("Levenshtein Distance")

    def compute_similarity(self, term1, term2, sub_rec_tuples):
        term_name1 = term1.name
        term_name2 = term2.name
        if not sub_rec_tuples:
            return 0
        score = LS.distance(term_name1, term_name2)
        return score

    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        else:
            return old_sim

