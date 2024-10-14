from src.Classes.SimilarityMetric import StructuralSimilarityMetric


class FactSimilarity(StructuralSimilarityMetric):
    def __init__(self,imp_alpha):
        super().__init__("Fact Similarity",imp_alpha)

    def compute_structural_similarity(self, element1, element2, sub_fact_pairs):
        """ This implements the computation of the common occurrences """
        """ For each common occurrence, the linked records are collected for c1,c2, and the minimum is collected """
        poss_matches = 0
        file_rec_id1 = dict()
        file_rec_id2 = dict()
        # this gives all combinations per file
        for record in sub_fact_pairs.keys():
            if record.db == "facts-db1":
                file_rec_id1.setdefault(record.file, list()).append(record)
            else:
                file_rec_id2.setdefault(record.file, list()).append(record)
        # to approximate the correct result
        # example (1 <-> 1, 1 <-> 2, 1 <-> 3, 2 <-> 4, 3 <-> 4, 4 <-> 4) means we have even number of records at each side but only two matches
        for file_name, rec_ids1 in file_rec_id1.items():
            rec_ids2 = file_rec_id2[file_name]
            poss_matches += min(len(rec_ids1), len(rec_ids2))

        total_occurrences = element1.degree + element2.degree
        return poss_matches / total_occurrences


