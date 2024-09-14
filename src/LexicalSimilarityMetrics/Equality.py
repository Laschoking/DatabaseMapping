from src.Classes.SimilarityMetric import LexicalSimilarityMetric


class Equality(LexicalSimilarityMetric):
    def __init__(self,metric_weight=1):
        super().__init__("Equality",metric_weight)


    def compute_lexical_similarity(self, term_name1, term_name2):
        if term_name1 == term_name2:
            return 1
        else:
            return 0

