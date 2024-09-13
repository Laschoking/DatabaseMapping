import re
import math

class SimilarityMetric:
    def __init__(self,name,metric_weight):
        self.name = name
        self.metric_weight = metric_weight

        # Both value are updated by the mapping function
        self.max_deg1 = 0
        self.max_deg2 = 0

    def compute_similarity(self,term1,term2,sub_rec_tuples):
        pass

    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        pass

    def weight_importance(self, term1, term2, sim):
        #return 1
        #return min(term1.degree,term2.degree)
        #return sim - 1 / (term1.degree + term2.degree)
        max_degree = self.max_deg1 + self.max_deg2
        if not max_degree:
            raise ValueError(f"total node degree should not be 0 {self.name,term1.name,term2.name}")
        norm_importance = (term1.degree + term2.degree) / max_degree
        return self.metric_weight * sim + (1 - self.metric_weight) * norm_importance



class StructuralSimilarityMetric(SimilarityMetric):
    def __init__(self,name,metric_weight):
        super().__init__(name,metric_weight)


    def compute_similarity(self,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        sim = self.compute_structural_similarity(term1, term2, sub_rec_tuples)
        return self.weight_importance(term1, term2, sim)

    def compute_structural_similarity(self,term1, term2, sub_rec_tuples):
        pass

    def recompute_similarity(self, old_sim, term1, term2, sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        return self.compute_similarity(term1, term2, sub_rec_tuples)


class LexicalSimilarityMetric(SimilarityMetric):
    def __init__(self,name,metric_weight):
        super().__init__(name,metric_weight)

    def compute_similarity(self,term1,term2,sub_rec_tuples):
        ALPHA = 0.95
        if not sub_rec_tuples:
            return 0

        low_name1 = term1.name.lower()
        low_name2 = term2.name.lower()
        low_name1,nrs1 = self.split_term(low_name1)
        low_name2,nrs2 = self.split_term(low_name2)

        lex_sim = self.compute_lexical_similarity(low_name1, low_name2)
        nr_sim = self.number_similarity(nrs1,nrs2)
        mixed_sim = ALPHA * lex_sim + (1 - ALPHA) * nr_sim
        return self.weight_importance(term1, term2, mixed_sim)


    def compute_lexical_similarity(self,term_name1, term_name2):
        pass

    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        else:
            return old_sim

    def split_term(self,low_name):
        # Filter a string for numbers, and return filtered string and numbers
        nrs = re.findall(r'\d+', low_name)
        low_name = re.sub(r'\d+', "", low_name)
        return low_name, nrs

    def number_similarity(self,nrs1,nrs2):
        l_nr1,l_nr2 = len(nrs1),len(nrs2)
        # Return full value if no term had numbers inside
        if l_nr1 == 0 and l_nr2 == 0:
            return 1
        # Return lowest value, if one term has numbers, and the other one does not
        if (l_nr1 == 0) != (l_nr2 == 0):
            return 0

        sim = 0
        i = 0
        while i < min(l_nr1,l_nr2):

            n1 = int(nrs1[i])
            n2 = int(nrs2[i])
            max_nr = max(n1,n2)
            if n1 == n2:
                sim += 1
            elif max_nr:
                sim += 1 - abs(n1 - n2) / max_nr
                # Testen für mehrfach Zahlen?
                # TODO handle negative cases
            i += 1
        return sim / min(l_nr1,l_nr2)




