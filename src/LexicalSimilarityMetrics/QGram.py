
from nltk.util import ngrams
from collections import Counter
from src.Classes.SimilarityMetric import LexicalSimilarityMetric


class QGram(LexicalSimilarityMetric):
    def __init__(self,n):
        super().__init__(f"QGram")
        self.n = n

    def compute_lexical_similarity(self, term_name1, term_name2):
        # Get N-grams
        ngrams1 = self.get_ngrams(term_name1, self.n)
        ngrams2 = self.get_ngrams(term_name2, self.n)

        # Count occurrences of each n-gram
        counter1 = Counter(ngrams1)
        counter2 = Counter(ngrams2)

        # Find the common n-grams
        common_ngrams = counter1 & counter2  # Intersection: min(counter1[key], counter2[key])

        # Calculate the similarity score
        total_ngrams = sum(common_ngrams.values())
        total_unique_ngrams = max(sum(counter1.values()),sum(counter2.values()))

        similarity = total_ngrams / total_unique_ngrams if total_unique_ngrams != 0 else 0

        return similarity


    def get_ngrams(self,text, n):
        # Generate n-grams
        n_grams = ngrams(text, n)
        return [''.join(grams) for grams in n_grams]
