import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.longest_common_subsequence import LongestCommonSubsequence
import re


class LCS(LexicalSimilarityMetric):
    def __init__(self,imp_alpha=0):
        super().__init__("LCS",imp_alpha)
        self.lcs = LongestCommonSubsequence()

    def compute_lexical_similarity(self, element_name1, element_name2):
        score = self.lcs.length(element_name1, element_name2) / max(len(element_name1), len(element_name2))
        return score

