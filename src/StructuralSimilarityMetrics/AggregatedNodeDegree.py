from src.Classes.SimilarityMetric import SimilarityMetric

class AggregatedNodeDegree(SimilarityMetric):
    def __init__(self):
        super().__init__("Aggregated Node Degree")

    def compute_structural_similarity(self, term1, term2, sub_rec_tuples):
        min(term1.degree,term2.degree) ** 2 / max(term1.degree,term2.degree)


