def term_equality(term1, term2, common_occ):
    if term1.name == term2.name:
        return 1
    else:
        return 0