import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
import re

class LevenshteinSimilarity(LexicalSimilarityMetric):
    def __init__(self,imp_alpha=0):
        super().__init__("Levenshtein",imp_alpha)
        self.norm_lev = NormalizedLevenshtein()

    def compute_lexical_similarity(self, element_name1, element_name2):
        score = 1 - self.norm_lev.distance(element_name1, element_name2)
        return score
