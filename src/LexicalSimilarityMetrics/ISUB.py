import difflib
from src.Classes.SimilarityMetric import SimilarityMetric


class IsubStringMatcher(SimilarityMetric):
    def __init__(self):
        super().__init__("ISUB StringMatcher")

    def compute_similarity(self, term1, term2, sub_rec_tuples):
        term_name1 = term1.name
        term_name2 = term2.name
        if not sub_rec_tuples:
            return 0

        # if both terms are integers String-Matching will have problems -> return closeness of both ints then
        if term1.type == "int" and term2.type == "int":
            max_int = max(int(term_name1), int(term_name2))
            if max_int > 0:
                return 1 - abs(int(term_name1) - int(term_name2)) / max_int
            else:
                return 1
        # if both were int we return already. now we know, that 1 is string
        elif (term1.type == "int" and term2.type != "int") or (term1.type != "int" and term2.type == "int"):
            return -1

        elif term_name1 is None or term_name2 is None or term_name1 == '' or term_name2 == '':
            return -1


        # count common substrings
        count_lcs = 0
        sm = difflib.SequenceMatcher(None, term_name1, term_name2)
        matching_blocks = sm.get_matching_blocks()
        # convert to list to reduce string sice
        l_st1 = len(term_name1)
        l_st2 = len(term_name2)
        term_name1 = list(term_name1)
        term_name2 = list(term_name2)

        # no need to recompute index overlap when we have blocks already
        winkler_st1_ind, winkler_st2_ind, winkler_size = matching_blocks[0]
        for st1_ind, st2_ind, size in matching_blocks:
            if size == 0: continue  # reached last statement or too small match
            count_lcs += size
            term_name1[st1_ind:st1_ind + size] = [None] * size
            term_name2[st2_ind:st2_ind + size] = [None] * size

        comm = 2 * count_lcs / (l_st1 + l_st2)

        # reduce strings of lcs matching
        diff1 = len([c for c in term_name1 if c]) / l_st1
        diff2 = len([c for c in term_name2 if c]) / l_st2

        # count differenc of left-overs
        product = diff1 * diff2
        l_st_sum = diff1 + diff2
        p = 0.6
        if l_st_sum - product == 0:
            diff = 0
        else:
            diff = product / (p + (1 - p) * (l_st_sum - product))

        if winkler_st1_ind == 0 and winkler_st2_ind == 0 and winkler_size > 0:
            impr_winkler = min(winkler_size, 4) * 0.1 * (1 - comm)
        else:
            impr_winkler = 0

        score = comm - diff + impr_winkler
        return score
    def recompute_similarity(self,old_sim,term1,term2,sub_rec_tuples):
        if not sub_rec_tuples:
            return 0
        else:
            return old_sim

