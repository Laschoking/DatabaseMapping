from src.Classes.SimilarityMetric import LexicalSimilarityMetric


class Equality(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("Equality")


    def compute_lexical_similarity(self, term_name1, term_name2):
        if term_name1 == term_name2:
            return 1
        else:
            return 0

