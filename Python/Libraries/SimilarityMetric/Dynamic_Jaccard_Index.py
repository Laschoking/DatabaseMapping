import Python.Libraries.ExpansionStrategies.Iterative_Anchor_Expansion
def dynamic_jaccard_index(term_obj1, term_obj2, rids1, rids2):
    # no mutual matchings possible
    if not rids1 or not rids2:
        return 0
    poss_matches = 0
    total_occurrences = 0
    for file_name,rid1 in rids1.items():
        rid2 = rids2[file_name]
        poss_matches += min(len(rid1),len(rid2))

    total_occurrences += sum(len(x) for x in term_obj1.occurrences.data())
    total_occurrences += sum(len(x) for x in term_obj2.occurrences.data())
    # weight matches higher to prefer important nodes
    return poss_matches ** 2/total_occurrences,0
