from src.Classes.SimilarityMetric import SimilarityMetric

class NodeDegree(SimilarityMetric):
    def __init__(self):
        super().__init__("Node Degree")

    def compute_similarity(self, term1, term2, sub_rec_tuples):
        return min(term1.degree,term2.degree) ** 2 / max(term1.degree,term2.degree)

    def recompute_similarity(self,old_sim, term1, term2, sub_rec_tuples):
        return self.compute_similarity(term1, term2, sub_rec_tuples)



