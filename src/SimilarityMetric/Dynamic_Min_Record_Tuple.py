from collections import Counter


# import src.ExpansionStrategies.Iterative_Anchor_Expansion
def dynamic_min_rec_tuples(term1, term2, sub_rids):
    # no mutual matchings possible
    if not sub_rids:
        return 0
    edge_count = 0
    rec_node_deg = Counter()
    temp_record_tuples = set()
    # that would iterate through each record_tuple twice
    for record, rec_tuples in sub_rids.items():
        rec_node_deg[record] = len(rec_tuples)
        temp_record_tuples |= rec_tuples

    for record_tuple in temp_record_tuples:
        rec1_deg = rec_node_deg[record_tuple.record1]
        rec2_deg = rec_node_deg[record_tuple.record2]
        edge_count += 1 / max(rec1_deg, rec2_deg)

    total_occurrences = term1.degree + term2.degree
    # weight matches higher to prefer important nodes
    return edge_count ** 2 / total_occurrences


''' 
1 / max(deg_1,deg_2) oder 1 / min(deg_1,deg_2)
A - B 1/3
A - C 1/3
A - F 1/3
G - F 1/3
H - F 1/3
1 - 2
1 - 3
1 - 4
2 - 5
3 - 5
4 - 5
    
    -> 2 
A,F deg: 3 ,B,C,D,E,G,H deg :1
A - B : 1
C - D : 1
-> 2
A - B 1/2 
(1,1)
(1,2)
(2,1)
(        
A - C 1/2
E - B 1/2
E - C 1/2
-> 2


'''