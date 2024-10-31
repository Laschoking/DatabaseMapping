from src.Classes.SimilarityMetric import LexicalSimilarityMetric


class Equality(LexicalSimilarityMetric):
    def __init__(self,imp_alpha=0):
        super().__init__("Equality",imp_alpha)


    def compute_lexical_similarity(self, element_name1, element_name2):
        if element_name1 == element_name2:
            return 1
        else:
            return 0

