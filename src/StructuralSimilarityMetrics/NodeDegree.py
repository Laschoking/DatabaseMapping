from src.Classes.SimilarityMetric import StructuralSimilarityMetric

class NodeDegree(StructuralSimilarityMetric):
    def __init__(self):
        super().__init__("Node Degree")

    def compute_structural_similarity(self, term1, term2, sub_rec_tuples):
        return min(term1.degree,term2.degree) ** 2 / max(term1.degree,term2.degree)

