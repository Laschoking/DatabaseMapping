import re
import math

class SimilarityMetric:
    def __init__(self,name,imp_alpha,struct_ratio):
        self.name = name
        self.imp_alpha = imp_alpha
        self.struct_ratio = struct_ratio
        # Both value are updated by the mapping_func function
        self.max_deg1 = 0
        self.max_deg2 = 0

    def compute_similarity(self,element1,element2,sub_fact_pairs):
        pass

    def recompute_similarity(self,old_sim,element1,element2,sub_fact_pairs):
        pass

    def weight_importance(self, element1, element2, sim):
        max_degree = self.max_deg1 + self.max_deg2
        if not max_degree:
            raise ValueError(f"total node degree should not be 0 {self.name,element1.name,element2.name}")
        importance_weight = (element1.degree + element2.degree) / max_degree
        return (1 - self.imp_alpha) * sim + self.imp_alpha * importance_weight


    def set_max_deg1(self,max_deg1):
        self.max_deg1 = max_deg1

    def set_max_deg2(self,max_deg2):
        self.max_deg2 = max_deg2


class StructuralSimilarityMetric(SimilarityMetric):
    def __init__(self,name,imp_alpha):
        super().__init__(name,imp_alpha,struct_ratio=1)


    def compute_similarity(self,element1,element2,sub_fact_pairs):
        if not sub_fact_pairs:
            return 0
        sim = self.compute_structural_similarity(element1, element2, sub_fact_pairs)
        return self.weight_importance(element1, element2, sim)

    def compute_structural_similarity(self,element1, element2, sub_fact_pairs):
        pass

    def recompute_similarity(self, old_sim, element1, element2, sub_fact_pairs):
        if not sub_fact_pairs:
            return 0
        return self.compute_similarity(element1, element2, sub_fact_pairs)


class LexicalSimilarityMetric(SimilarityMetric):
    def __init__(self,name,imp_alpha):
        super().__init__(name,imp_alpha,struct_ratio=0)

    def compute_similarity(self,element1,element2,sub_fact_pairs):
        nr_weight = 0.95
        if not sub_fact_pairs:
            return 0

        low_name1 = element1.name.lower()
        low_name2 = element2.name.lower()
        low_name1,nrs1 = self.split_element(low_name1)
        low_name2,nrs2 = self.split_element(low_name2)

        lex_sim = self.compute_lexical_similarity(low_name1, low_name2)
        nr_sim = self.number_similarity(nrs1,nrs2)
        mixed_sim = nr_weight * lex_sim + (1 - nr_weight) * nr_sim
        return self.weight_importance(element1, element2, mixed_sim)


    def compute_lexical_similarity(self,element_name1, element_name2):
        pass

    def recompute_similarity(self,old_sim,element1,element2,sub_fact_pairs):
        if not sub_fact_pairs:
            return 0
        else:
            return old_sim

    def split_element(self,low_name):
        # Filter a string for numbers, and return filtered string and numbers
        nrs = re.findall(r'\d+', low_name)
        low_name = re.sub(r'\d+', "", low_name)
        return low_name, nrs

    def number_similarity(self,nrs1,nrs2):
        l_nr1,l_nr2 = len(nrs1),len(nrs2)
        # Return full value if no element had numbers inside
        if l_nr1 == 0 and l_nr2 == 0:
            return 1
        # Return lowest value, if one element has numbers, and the other one does not
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

            i += 1
        return sim / min(l_nr1,l_nr2)




class MixedSimilarityMetric(SimilarityMetric):
    def __init__(self,struct_metric, lex_metric,struct_ratio,imp_alpha):
        self.struct_metric = struct_metric
        self.lex_metric = lex_metric
        self.name = f"{struct_metric.name}_{struct_ratio}_{lex_metric.name}"

        super().__init__(self.name,imp_alpha,struct_ratio=struct_ratio)


    def compute_similarity(self, element1, element2, sub_fact_pairs):

        # propagate  (which would trigger, application of importance str_weight)
        str_sim = self.struct_ratio * self.struct_metric.compute_similarity(element1, element2, sub_fact_pairs)
        lex_sim = (1 - self.struct_ratio) * self.lex_metric.compute_similarity(element1, element2,sub_fact_pairs)
        mixed_sim = str_sim + lex_sim
        if mixed_sim is None:
            raise ValueError(f"mixed similarity is NONE ")
        return mixed_sim

    # Overwrites other behaviour
    def set_max_deg1(self, max_deg1):
        self.max_deg1 = max_deg1
        self.struct_metric.set_max_deg1(self.max_deg1)
        self.lex_metric.set_max_deg1(self.max_deg1)

    def set_max_deg2(self, max_deg2):
        self.max_deg2 = max_deg2
        self.struct_metric.set_max_deg2(self.max_deg2)
        self.lex_metric.set_max_deg2(self.max_deg2)

    def recompute_similarity(self,old_sim,element1,element2,sub_fact_pairs):
        str_sim = self.struct_ratio * self.struct_metric.recompute_similarity(old_sim,element1, element2, sub_fact_pairs)
        lex_sim = (1 - self.struct_ratio) * self.lex_metric.recompute_similarity(old_sim,element1, element2,sub_fact_pairs)
        mixed_sim = str_sim + lex_sim
        return mixed_sim

