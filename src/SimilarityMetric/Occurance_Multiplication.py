def occurrence_multiplication(term_name1, term_name2, term1, term2, occ_overlap):
    c_sum = 0
    for common in occ_overlap:
        c_sum += term1.occurrence_c[common] * term2.occurrence_c[common]
    return c_sum