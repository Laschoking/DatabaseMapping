from src.SimilarityMetric.Jaccard_Min import *
from src.SimilarityMetric.ISUB_SequenceMatcher import *


def jaccard_isub_mix(term1, term2, occ_overlap):
    p = 0.5
    jaccard_sim = jaccard_min(term1, term2, occ_overlap)
    isub_sim = isub_sequence_matcher(term1, term2, occ_overlap)
    return p * jaccard_sim + (1 - p) * isub_sim

# def structural_similarity_jaccard(term1,term2,occ_overlap,all_tuple_sim):