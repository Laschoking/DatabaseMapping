import Python.Libraries.ExpansionStrategies.Iterative_Anchor_Expansion
def dynamic_jaccard_index(term_obj1, term_obj2, sub_rids1,sub_rids2):
    # no mutual matchings possible
    if not sub_rids1 or not sub_rids2:
        return 0
    poss_matches = 0
    total_occurrences = 0
    file_rids1 = dict()
    file_rids2 = dict()
    # this gives all combinations per file_name
    for sub_rid1 in sub_rids1:
        file_rids1.setdefault(sub_rid1.file_name,list()).append(sub_rid1.rid)
    for sub_rid2 in sub_rids2:
        file_rids2.setdefault(sub_rid2.file_name,list()).append(sub_rid2.rid)
    # TODO This is currenctly not optimal. Probably a bipartite graph matching algorithm has to be deployed
    # to approximate the correct result
    # example (1 <-> 1, 1 <-> 2, 1 <-> 3, 2 <-> 4, 3 <-> 4, 4 <-> 4) means we have even number of records at each side but only two matches
    for file_name,recs1 in file_rids1.items():
        recs2 = file_rids2[file_name]
        poss_matches += min(len(recs1),len(recs2))

    total_occurrences += sum(len(x) for x in term_obj1.occurrences.values())
    total_occurrences += sum(len(x) for x in term_obj2.occurrences.values())
    # weight matches higher to prefer important nodes
    return poss_matches ** 2/total_occurrences
