import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
import re

class LevenshteinSimilarity(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("Levenshtein")
        self.norm_lev = NormalizedLevenshtein()

    def compute_lexical_similarity(self, term_name1, term_name2):
        score = 1 - self.norm_lev.distance(term_name1, term_name2)
        return score
