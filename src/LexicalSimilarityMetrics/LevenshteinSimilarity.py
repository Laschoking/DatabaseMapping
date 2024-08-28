import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.normalized_levenshtein import NormalizedLevenshtein


class LevenshteinSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("Levenshtein Similarity")

    def compute_lexical_similarity(self, term_name1, term_name2):
        norm_lev = NormalizedLevenshtein()
        score = 1 - norm_lev.distance(term_name1, term_name2)
        return score