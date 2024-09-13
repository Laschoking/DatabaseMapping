from src.Classes.SimilarityMetric import StructuralSimilarityMetric

class NodeDegree(StructuralSimilarityMetric):
    def __init__(self,metric_weight):
        super().__init__("Node Degree",metric_weight)

    def compute_structural_similarity(self, term1, term2, sub_rec_tuples):
        return min(term1.degree,term2.degree) / max(term1.degree,term2.degree)

