from src.Classes.SimilarityMetric import StructuralSimilarityMetric


class JaccardIndex(StructuralSimilarityMetric):
    def __init__(self,metric_weight):
        super().__init__("Dynamic Jaccard",metric_weight)

    def compute_structural_similarity(self, term1, term2, sub_rec_tuples):
        # no mutual matchings possible
        poss_matches = 0
        file_rec_id1 = dict()
        file_rec_id2 = dict()
        # this gives all combinations per file_name
        for record in sub_rec_tuples.keys():
            if record.db == "facts-db1":
                file_rec_id1.setdefault(record.file_name, list()).append(record)
            else:
                file_rec_id2.setdefault(record.file_name, list()).append(record)
        # TODO This is currenctly not optimal, see Unit_Test_Max_Cardinality_1
        # to approximate the correct result
        # example (1 <-> 1, 1 <-> 2, 1 <-> 3, 2 <-> 4, 3 <-> 4, 4 <-> 4) means we have even number of records at each side but only two matches
        for file_name, rec_ids1 in file_rec_id1.items():
            rec_ids2 = file_rec_id2[file_name]
            poss_matches += min(len(rec_ids1), len(rec_ids2))

        total_occurrences = term1.degree + term2.degree
        # weight matches higher to prefer important nodes
        return poss_matches / total_occurrences


#poss_matches * poss_matches / total_occurrences = pm / total_occ