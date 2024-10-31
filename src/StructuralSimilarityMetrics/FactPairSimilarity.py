from collections import Counter
from src.Classes.SimilarityMetric import StructuralSimilarityMetric


class FactPairSimilarity(StructuralSimilarityMetric):
    def __init__(self,imp_alpha):
        super().__init__("FactPair-Sim",imp_alpha)

    def compute_structural_similarity(self, element1, element2, sub_fact_pairs):
        edge_count = 0
        rec_node_deg = Counter()
        temp_fact_pairs = set()
        # that would iterate through each fact_pair twice
        for fact, fact_pairs in sub_fact_pairs.items():
            rec_node_deg[fact] = len(fact_pairs)
            temp_fact_pairs |= fact_pairs

        for fact_pair in temp_fact_pairs:
            rec1_deg = rec_node_deg[fact_pair.fact1]
            rec2_deg = rec_node_deg[fact_pair.fact2]
            edge_count += 1 / max(rec1_deg, rec2_deg)

        total_occurrences = element1.degree + element2.degree
        # str_weight matches higher to prefer important nodes
        return 2 * edge_count / total_occurrences


