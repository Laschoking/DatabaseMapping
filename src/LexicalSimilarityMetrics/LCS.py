import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.longest_common_subsequence import LongestCommonSubsequence
import re


class LCS(LexicalSimilarityMetric):
    def __init__(self,metric_weight=1):
        super().__init__("LCS",metric_weight)
        self.lcs = LongestCommonSubsequence()

    def compute_lexical_similarity(self, term_name1, term_name2):
        score = self.lcs.length(term_name1, term_name2) / max(len(term_name1), len(term_name2))
        return score

