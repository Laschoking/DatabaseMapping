import difflib
from src.Classes.SimilarityMetric import LexicalSimilarityMetric
# this follows the LogMap implementation at https://github.com/ernestojimenezruiz/logmap-matcher/blob/a35c18eec8d027a76974b90aa63bca2b982db876/src/main/java/uk/ac/ox/krr/logmap2/mappings/I_Sub.java#L157

class IsubStringMatcher(LexicalSimilarityMetric):
    def __init__(self):
        super().__init__("ISUB")

    def compute_lexical_similarity(self, term_name1, term_name2):
        term_name1 = term_name1.lower()
        term_name2 = term_name2.lower()

        # if both terms are integers String-Matching will have problems -> return closeness of both ints then

        # count common substrings
        # note: normally a subsequence does not have to be contiguous (as in the Longest Common Subsequence)
        # The imported SequenceMatcher guarantees this even though the name might suggest otherwise
        # "The idea is to find the longest contiguous matching subsequence" in https://docs.python.org/3/library/difflib.html#difflib.SequenceMatcher
        count_lcs = 0
        sm = difflib.SequenceMatcher(None, term_name1, term_name2)
        matching_blocks = sm.get_matching_blocks()
        # convert to list to reduce string sice
        l_st1 = len(term_name1)
        l_st2 = len(term_name2)
        if not l_st1 and not l_st2:
            return 1
        elif bool(l_st1) != bool(l_st2):
            return 0

        term_list1 = list(term_name1)
        term_list2 = list(term_name2)

        # no need to recompute index overlap when we have blocks already
        winkler_st1_ind, winkler_st2_ind, winkler_size = matching_blocks[0]
        for st1_ind, st2_ind, size in matching_blocks:
            if size == 0:
                continue  # reached last statement or too small match
            count_lcs += size
            term_list1[st1_ind:st1_ind + size] = [None] * size
            term_list2[st2_ind:st2_ind + size] = [None] * size

        comm = 2 * count_lcs / (l_st1 + l_st2)

        # reduce strings of lcs matching
        diff1 = len([c for c in term_list1 if c]) / l_st1
        diff2 = len([c for c in term_list2 if c]) / l_st2

        # count difference of left-overs
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


