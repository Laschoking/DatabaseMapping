from collections import Counter
from src.Classes.SimilarityMetric import SimilarityMetric


class DynamicRecordTupleCount(SimilarityMetric):
    def __init__(self):
        super().__init__("Edge Count")

    def compute_similarity(self, term1, term2, sub_rec_tuples):
        # no mutual matchings possible
        if not sub_rec_tuples:
            return 0
        edge_count = 0
        rec_node_deg = Counter()
        temp_record_tuples = set()
        # that would iterate through each record_tuple twice
        for record, rec_tuples in sub_rec_tuples.items():
            rec_node_deg[record] = len(rec_tuples)
            temp_record_tuples |= rec_tuples

        for record_tuple in temp_record_tuples:
            rec1_deg = rec_node_deg[record_tuple.record1]
            rec2_deg = rec_node_deg[record_tuple.record2]
            edge_count += 1 / max(rec1_deg, rec2_deg)

        total_occurrences = term1.degree + term2.degree
        # weight matches higher to prefer important nodes
        return edge_count ** 2 / total_occurrences

    def recompute_similarity(self,old_sim, term1, term2, sub_rec_tuples):
        return self.compute_similarity(term1, term2, sub_rec_tuples)



