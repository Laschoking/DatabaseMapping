from src.Classes.SimilarityMetric import SimilarityMetric

class AggregatedNodeDegree(SimilarityMetric):
    def __init__(self):
        super().__init__("Aggregated Node Degree")

    def compute_structural_similarity(self, element1, element2, sub_fact_pairs):
        min(element1.degree,element2.degree) ** 2 / max(element1.degree,element2.degree)


