
from nltk.util import ngrams
from collections import Counter
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
from strsimpy.qgram import QGram

from strsimpy.normalized_levenshtein import NormalizedLevenshtein

class Dice(LexicalSimilarityMetric):
    def __init__(self,n):
        super().__init__(f"Dice")
        self.n = n

    def compute_lexical_similarity(self, term_name1, term_name2):
        # Get N-grams
        ngrams1 = self.get_ngrams(term_name1, self.n)
        ngrams2 = self.get_ngrams(term_name2, self.n)

        # Find the common n-grams
        common_ngrams = ngrams1 & ngrams2

        # Calculate the similarity score
        if ngrams1 or ngrams2:
            similarity = 2 * len(common_ngrams) / (len(ngrams1) + len(ngrams2))
        else:
            similarity = 0

        return similarity


    def get_ngrams(self,text, n):
        # Generate n-grams
        n_grams = ngrams(text, n)
        return set(n_grams)
