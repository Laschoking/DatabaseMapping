import Python.Libraries.ExpansionStrategies.Iterative_Anchor_Expansion
def dynamic_jaccard_index(term_obj1, term_obj2, rec_ids):
    # no mutual matchings possible
    if not rec_ids:
        return 0
    poss_matches = 0
    total_occurrences = 0
    # this gives all combinations per file_name
    for file_record_obj in rec_ids.values():
        rids1, rids2 = file_record_obj.rids1,file_record_obj.rids2
        poss_matches += min(len(rids1.keys()),len(rids2.keys()))

    total_occurrences += sum(len(x) for x in term_obj1.occurrences.values())
    total_occurrences += sum(len(x) for x in term_obj2.occurrences.values())
    # weight matches higher to prefer important nodes
    return poss_matches ** 2/total_occurrences
