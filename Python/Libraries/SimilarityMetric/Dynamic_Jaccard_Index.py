import Python.Libraries.ExpansionStrategies.Iterative_Anchor_Expansion
def dynamic_jaccard_index(term_obj1, term_obj2, sub_rids):
    # no mutual matchings possible
    if not sub_rids:
        return 0
    poss_matches = 0
    total_occurrences = 0
    file_rids1 = dict()
    file_rids2 = dict()
    # this gives all combinations per file_name
    for rec_obj in sub_rids.keys():
        if rec_obj.db == "facts-db1":
            file_rids1.setdefault(rec_obj.file_name,list()).append(rec_obj.rid)
        else:
            file_rids2.setdefault(rec_obj.file_name, list()).append(rec_obj.rid)
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
