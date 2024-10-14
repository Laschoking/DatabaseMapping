from src.Classes.SimilarityMetric import StructuralSimilarityMetric

class DegreeSimilarity(StructuralSimilarityMetric):
    def __init__(self,imp_alpha):
        super().__init__("Node Degree",imp_alpha)

    def compute_structural_similarity(self, element1, element2, sub_fact_pairs):
        return min(element1.degree,element2.degree) / max(element1.degree,element2.degree)

