
import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedDict,SortedList,SortedSet
from src.Config_Files.Debug_Flags import DEBUG_TERMS, debug_set,HUB_RECOMPUTE, PLOT_STATISTICS
from src.Classes.ExpansionStrategy import  ExpansionStrategy
import src.Classes.Terms
import itertools

class Crossproduct(ExpansionStrategy):
    def __init__(self,anchor_quantile,sim_outlier,DYNAMIC):
        super().__init__("Iterative",anchor_quantile,sim_outlier,DYNAMIC)


    def accept_expand_mappings(self,mapping, terms_db1, terms_db2, blocked_terms,
                                   similarity_metric):
        prio_dict = SortedList()

        # those lists hold all terms, that are still mappable
        free_term_names1 = SortedList(terms_db1.keys())
        free_term_names2 = SortedList(terms_db2.keys())

        # those Dicts are a mirror version of prio_dict. for each term t, the tuple objects are saved, where t is involved
        # holds {term_name : [(tuple1,sim1),(tuple2,sim2) ...]}
        terms_db1_pq_mirror = SortedDict()
        terms_db2_pq_mirror = SortedDict()
        mapping_dict = []

        tuples_loc_sim = SortedDict()
        processed_mapping_tuples = set()

        # counts len, after mapping pop, del obsolete tuples & adding new tuples from neighbourhoods
        watch_prio_len = []
        watch_exp_sim = []
        watch_mapped_sim = []
        uncertain_mapping_tuples = 0

        # block certain terms, that cannot be changed without computing wrong results
        for blocked_term in blocked_terms:
            if blocked_term in terms_db1:
                # map term to itself
                mapping_dict.append((blocked_term, blocked_term))
                free_term_names1.discard(blocked_term)
                # if in terms_db2 then delete occurrence there
                if blocked_term in free_term_names2:
                    free_term_names2.discard(blocked_term)
                else:
                    # for counting, how many terms are mapped to synthetic values (that do not exist in DB2)
                    mapping.syn_counter += 1
        terms_db1 = terms_db1.values()
        terms_db2 = terms_db2.values()
        all_poss_mapping = find_crossproduct_mappings(terms_db1, terms_db2)
        # tuples_loc_sim, terms_db1_pq_mirror, terms_db2_pq_mirror, prio_dict, processed_mapping_tuples, watch_exp_sim, similarity_metric
        add_mappings_to_pq(all_poss_mapping, tuples_loc_sim, terms_db1_pq_mirror, terms_db2_pq_mirror, prio_dict,
                           processed_mapping_tuples, watch_exp_sim, similarity_metric)

        count_hub_recomp = 0

        while 1:
            if prio_dict:  # pop last item = with the highest similarity
                sim, tuples = prio_dict.peekitem(index=-1)
                # print(prio_dict)

                # data could be empty because of deletion of obsolete term-tuples
                if not tuples:
                    prio_dict.popitem(-1)
                    continue

                # removes first data-item
                term_name_tuple = tuples.pop()
                term_name1, term_name2 = term_name_tuple
                term1, term2 = terms_db1[term_name1], terms_db2[term_name2]

                # last tuple in similarity bin -> delete empty bin

                if term_name1 in free_term_names1 and term_name2 in free_term_names2:

                    sim, common_occ = tuples_loc_sim[term_name_tuple]

                    # add new mapping
                    mapping_dict.append((term_name1, term_name2))
                    # print(term_name1 + " : " + term_name2)

                    # make terms "blocked"
                    free_term_names1.discard(term_name1)
                    free_term_names2.discard(term_name2)

                    # remove tuple from mirror so that we have no key error
                    terms_db1_pq_mirror[term_name1].remove((term_name_tuple, sim))
                    terms_db2_pq_mirror[term_name2].remove((term_name_tuple, sim))

                    # delete all tuples from priority queue, that contain term1 or term2

                    uncertain_mapping_flag = delete_from_prio_dict(terms_db1_pq_mirror[term_name1], prio_dict, sim)
                    uncertain_mapping_flag += delete_from_prio_dict(terms_db2_pq_mirror[term_name2], prio_dict, sim)
                    if uncertain_mapping_flag:
                        uncertain_mapping_tuples += 1
                    # remove term entry from mirror
                    del terms_db1_pq_mirror[term_name1]
                    del terms_db2_pq_mirror[term_name2]

                    watch_mapped_sim.append(sim)

                watch_prio_len.append(sum(len(val) for val in prio_dict.values()))
            else:
                for term_name1 in free_term_names1:
                    new_term = "new_var_" + str(mapping.syn_counter)
                    # print("add new var: " + new_term + " for " + term)
                    mapping_dict.append((term_name1, new_term))
                    mapping.syn_counter += 1
                break
        mapping.final_mapping = pd.DataFrame.from_records(mapping_dict, columns=None)

        # TODO Plot node distribution
        '''fig, ax = plt.subplots(4,1)
        fig.suptitle("iterativeExpansion + " + similarity_metric.__name__)
        ax[0].scatter(range(len(watch_prio_len)),watch_prio_len,s=1, label='Queue Size')
        ax[0].set_title("Queue Size")
        ax[1].scatter(range(len(watch_exp_sim)),np.array(watch_exp_sim),s=1,label='Expanded Similarities')
        ax[1].set_title("Computed Similarities")
        ax[2].scatter(range(len(watch_mapped_sim)), np.array(watch_mapped_sim), s=0.5, label='Mapped Similarities')
        ax[2].set_title("Mapped Similarities")
        ax[3].hist(watch_mapped_sim,100,label='Accepted MappingContainer Distribution')
        ax[3].set_title("Distribution of Similarities")
        fig.tight_layout()
        plt.show()
        '''
        return uncertain_mapping_tuples, count_hub_recomp, len(tuples_loc_sim.keys())


def delete_from_prio_dict(tuples, prio_dict, mapped_sim):
    uncertain_mapping_flag = False
    for tuple_names, sim in tuples:
        if sim not in prio_dict:
            ValueError("sim- key not in priority dict:" + str(sim))
        elif tuple_names not in prio_dict[sim]:
            continue
            # for the moment we just ignore, that the tuple was removed from the other side some iterations before
            # f.e. (t_total1,t3) was chosen before as mapping so (t_total1,t2) was removed in last iteration
            # now we pick (t4,t2) and would want to remove (t_total1,t2) again b
            # print("skipped value, bc it was removed from other side: " + str(tuple_names))
        else:
            prio_dict[sim].remove(tuple_names)
            # this is for logging, how often a mapping was done, where one of the terms had other tuples with the same
            # similarity
            if not uncertain_mapping_flag and sim >= mapped_sim:
                uncertain_mapping_flag = True
    return uncertain_mapping_flag


def find_crossproduct_mappings(hubs1, hubs2):
    return set(itertools.product(hubs1, hubs2))


# poss_mappings is a set of tuple
def add_mappings_to_pq(new_mapping_tuples, tuples_loc_sim, terms_db1_pq_mirror, terms_db2_pq_mirror, prio_dict,
                       processed_mapping_tuples, watch_exp_sim, similarity_metric):
    for term1, term2 in new_mapping_tuples:
        term_name_tuple = term1.name, term2.name

        # this check is currently not necessary but later, when adding struc-sim we need it
        if term_name_tuple not in tuples_loc_sim:
            join = occurrence_overlap(term1, term2)
            common_occ, term1_record_ids, term2_record_ids = join
            sim = similarity_metric(term1, term2, common_occ)

            tuples_loc_sim[term_name_tuple] = (sim, common_occ)

            # add tuple to priority_queue
            if sim > 0:
                if sim not in prio_dict:
                    prio_dict[sim] = SortedList([term_name_tuple])
                else:
                    prio_dict[sim].add(term_name_tuple)
                # print(sim,term_name_tuple)

                processed_mapping_tuples.add(term_name_tuple)

                # add term & tuple to prio_dict-mirror-1
                if term1.name in terms_db1_pq_mirror:
                    terms_db1_pq_mirror[term1.name].append((term_name_tuple, sim))
                else:
                    terms_db1_pq_mirror[term1.name] = [(term_name_tuple, sim)]

                # add term & tuple to prio_dict-mirror-2
                if term2.name in terms_db2_pq_mirror:
                    terms_db2_pq_mirror[term2.name].append((term_name_tuple, sim))
                else:
                    terms_db2_pq_mirror[term2.name] = [(term_name_tuple, sim)]
                watch_exp_sim.append(sim)


def occurrence_overlap(term1, term2):
    # intersection saves the key (file,col_nr): #common which is the minimum of occurrences for this key
    intersection = term1.occurrence_c & term2.occurrence_c
    term1_record_ids = []
    term2_record_ids = []
    # maybe it would be smarter to calculate this only after mapping has been accepted
    # on the other hand: when including the neighbour sim we need this info here
    # overlap consists of file, col_nr
    for overlap in intersection:
        term1_record_ids.append(term1.occurrences[overlap])
        term2_record_ids.append(term2.occurrences[overlap])
    return intersection, term1_record_ids, term2_record_ids


def find_hubs_std(free_term_names, terms_occ):
    degrees = []
    nodes = []
    for term in free_term_names:
        degrees.append(len(terms_occ[term]))
        nodes.append(term)
    mean = np.mean(degrees)
    std_dev = np.std(degrees)
    threshold = mean + std_dev
    # print("mean: "  + str(mean))
    # print("std_dev: " + str(std_dev))
    hubs = [nodes[i] for i in range(len(degrees)) if degrees[i] > threshold]
    # print("anzahl der hubs: " + str(len(hubs)))
    # print("anzahl der Terme: " + str(len(nodes)))
    return hubs


def find_hubs_quantile(free_term_names, terms):
    nodes = [terms[term_name].degree for term_name in free_term_names]
    print("node degree mean: " + str(round(np.mean(nodes), 2)) + " standard deviation: " + str(round(np.std(nodes), 2)))
    quantile = np.quantile(nodes, q=0.98)
    # returns termobjects
    return set(terms[free_term_names[i]] for i in range(len(free_term_names)) if nodes[i] >= quantile)