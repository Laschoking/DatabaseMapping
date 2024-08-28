class SimilarityMetric:
    def __init__(self,name):
        self.name = name

    def compute_similarity(self,term1,term2,sub_rec_tuples):
        pass

    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        pass

class StructuralSimilarityMetric(SimilarityMetric):
    def __init__(self,name):
        super().__init__(name)


    def compute_similarity(self,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        return self.compute_structural_similarity(term1, term2, sub_rec_tuples)

    def compute_structural_similarity(self,term1, term2, sub_rec_tuples):
        pass

    def recompute_similarity(self, old_sim, term1, term2, sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        return self.compute_similarity(term1, term2, sub_rec_tuples)



class LexicalSimilarityMetric(SimilarityMetric):
    def __init__(self,name):
        super().__init__(name)

    def compute_similarity(self,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        term_name1 = term1.name
        term_name2 = term2.name
        return self.compute_lexical_similarity(term_name1, term_name2)

    def compute_lexical_similarity(self,term_name1, term_name2):
        pass

    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        else:
            return old_sim



