from collections import deque
import itertools
import numpy as np
from Python.Libraries.Classes import  *
import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedList, SortedDict
import itertools
import Python.Config_Files.Setup as setup


# blocked terms only if DL-computation
def iterative_anchor_expansion(mapping_obj, db1, terms1, db2, terms2, blocked_terms, similarity_metric):
    prio_dict = SortedDict()
    active_rid_combinations = dict()
    active_rids1 = dict()
    active_rids2 = dict()
    # those lists hold all terms, that are still mappable
    for term in terms1.keys():
        if type(term) is not str:
            print(term)
    free_term_names1 = SortedList(terms1.keys())
    free_term_names2 = SortedList(terms2.keys())

    # those Dicts are a mirror version of prio_dict. for each term t, the tuple objects are saved, where t is involved
    # holds {term_name : [(tuple1,sim),(tuple2,sim2) ...]}
    terms1_pq_mirror = SortedDict()
    terms2_pq_mirror = SortedDict()
    mapping_dict = []

    tuples_loc_sim = SortedDict()
    processed_mapping_tuples = set()

    # block certain terms, that cannot be changed without computing wrong results
    for blocked_term in blocked_terms:
        if blocked_term in terms1:
            # map term to itself
            mapping_dict.append((blocked_term, blocked_term))
            free_term_names1.discard(blocked_term)
            # if in terms2 then delete occurrence there
            if blocked_term in free_term_names2:
                free_term_names2.discard(blocked_term)
            else:
                # for counting, how many terms are mapped to synthetic values (that do not exist in db2)
                mapping_obj.new_term_counter += 1
    # counts len, after mapping pop, del obsolete tuples & adding new tuples from neighbourhoods
    watch_prio_len = []
    watch_exp_sim = []
    accepted_sim = []
    uncertain_mapping_tuples = 0
    local_approval = setup.hub_recompute

    count_hub_recomp = 0
    new_hubs_flag = True
    last_sim = 0

    while 1:
        if prio_dict and not new_hubs_flag:  # pop last item = with the highest similarity
            sim, tuples = prio_dict.peekitem(index=-1)
            # if setup.debug: print(prio_dict)

            # data could be empty because of deletion of obsolete term-tuples
            if not tuples:
                prio_dict.popitem(-1)
                continue

            # removes first data-item ( tuples appended later i.e. by hub recomputation are at the end)
            term_tuple = tuples.pop(0)
            term_name1, term_name2 = term_tuple.term_obj1.name,term_tuple.term_obj1.name

            # last tuple in similarity bin -> delete empty bin

            if term_name1 not in free_term_names1 or term_name2 not in free_term_names2:
                ValueError("Term should not be vacant anymore: " + term_name1 + " " + term_name2)
            # if value is too bad - find new Hubs

            if setup.hub_recompute and accepted_sim and local_approval:
                q1 = np.percentile(accepted_sim, 25)
                q3 = np.percentile(accepted_sim, 75)
                IQR = q3 - q1
                low_outlier = q1 - 1.5 * IQR
                if sim < low_outlier:
                    # trigger new hub detection
                    new_hubs_flag = True
                    # insert sim & tuple back to dictionary
                    prio_dict[sim].append(term_tuple)
                    print("denied: " + str(sim))
                    # mark as false so at least 1 new mapping has to be added before we can trigger recomputation again
                    local_approval = False
                    continue
                    
            # add new mapping
            mapping_dict.append((term_name1, term_name2))
            sim, rids1, rids2 = term_tuple.get_similarity()

            if setup.debug: print(term_name1 + " -> " + term_name2)

            # make terms "blocked"
            free_term_names1.discard(term_name1)
            free_term_names2.discard(term_name2)

            # remove tuple from mirror so that we have no key error
            terms1_pq_mirror[term_name1].remove((sim, term_tuple))
            terms2_pq_mirror[term_name2].remove((sim, term_tuple))

            # delete all tuples from priority queue, that contain term_obj1 or term_obj2
            # TODO it is possible that the deletion of a tuple from prio-dict leads to some unexpected behaviour in active_record combinations / their subscribers
            uncertain_mapping_flag = delete_from_prio_dict(terms1_pq_mirror[term_name1], prio_dict, sim)
            uncertain_mapping_flag += delete_from_prio_dict(terms2_pq_mirror[term_name2], prio_dict, sim)
            l = sum(len(val) for val in prio_dict.values())
            if setup.debug:
                print("reduced length: " + str(l))
            watch_prio_len.append(l)

            if uncertain_mapping_flag:
                uncertain_mapping_tuples += 1
            # remove term entry from mirror
            del terms1_pq_mirror[term_name1]
            del terms2_pq_mirror[term_name2]

            accepted_sim.append(sim)

            kill_rid_combs = dict()
            keep_rid_combs = dict()
            # can be used later in the expansion
            add_rid_combs = dict()

            # dynamic record matching
            for file_name,(rid1,rid1_combs) in rids1.items():
                if rid1 in active_rids1[file_name]:
                    # active_rids holds references to all combinations of records with rid1 as first augment
                    curr_rid_combs = active_rids1[file_name][rid1]
                    kill_rid_combs.setdefault(file_name,set()).update(curr_rid_combs - rid1_combs)
                    keep_rid_combs.setdefault(file_name,set()).update(curr_rid_combs & rid1_combs)
                else:
                    add_rid_combs.setdefault(file_name,set()).update(rid1_combs)

            # i think we only can kill  existing combs bc. the other stuff was done by rid1 already
            # TODO do this for rid2 too

            # update record-combinations that were verified by current mapping
            for file_name, rid_tuple in keep_rid_combs.items():
                filled_cell =
                # by accepting the current mapping the record-tuple is matched & finished
                if rid_tuple.mark_filled_cell(filled_cell):
                    del active_rids1[file_name][rid_tuple.rid1]
                    del active_rids2[file_name][rid_tuple.rid2]
                    del rid_tuple

            # delete record-combinations that were discarded because of the selected mapping from active combinations
            for file_name, rid_tuple in kill_rid_combs:
                active_rids1[file_name][rid_tuple.rid1].delete(rid_tuple)
                active_rids2[file_name][rid_tuple.rid2].delete(rid_tuple)
                subscribers = rid_tuple.subscribers
                # those are term tuples (potential mappings) that would have satisfied rid_tuple
                # now that rid_tuple is invalid they have to be updated & their confidence value adepted
                for term_tuple in subscribers:

                    sim, rids1, rids2 = tuples_loc_sim[term_tuple]
                    rids1[rid_tuple.rid1].delete(rid_tuple)
                    rids2[rid_tuple.rid2].delete(rid_tuple)

                    # remove the record-id from the mapping, if the record-combination was the onliest in the set
                    if not rids1[rid_tuple.rid1]: del rids1[rid_tuple.rid1]
                    if not rids2[rid_tuple.rid2]: del rids2[rid_tuple.rid2]
                    # check if the mapping is now obsolete because it does not fulfil any record-combinations anymore
                    if not rids1 or not rids2:
                        delete_from_prio_dict([term_tuple],prio_dict,sim)

                    # otherwise: update confidence value & possibly change position of mapping in the prio queue
                    new_sim,new_sim2 = similarity_metric(term_obj1, term_obj2, rids1, rids2)
                    update_tuple_prio_dict(term_tuple,prio_dict,sim,new_sim)

            # add new record-combinations that will be the base of the following discovery step
            for file_name,rid_tuple in add_rid_combs.items():
                # active_rids1 = {file_name : { rid1 : [rid_tuple1, rid_tuple2, .. ], rid2 : []}}
                active_rids1.setdefault(file_name,dict()).setdefault(rid_tuple.rid1,list()).append(rid_tuple)
                active_rids2.setdefault(file_name,dict()).setdefault(rid_tuple.rid2,list()).append(rid_tuple)


            # expansion strategy:
            # current state: consider only terms, that occur in same colum of merged records
            new_mapping_tuples = set()
            for file_name,rid_tuple in add_rid_combs.items():


                df1 = db1.files[file_name]
                df2 = db2.files[file_name]
                cols = unpack_multi_columns(map_term_col)
                # if setup.debug: print(file_name,col)

                # the mapped tuple (term_obj1, term_obj2) has the same key =  "file_name" & position "col"
                # this could have been multiple times (db1_row_ids & db2_row_ids) for the same key
                # i.e. term_obj1 "a" appears in several rows at the same spot  1:[a,b,c], 2:[a,d,f], so db1_row_ids hold all record-ids [1,2]
                # -> iterate through all columns and retrieve possible mapping pairs (aka. neighbours of term1 & term2)
                for col_ind in set(range(len(df1.columns))) - set(cols):
                    # iterate through all records of db1: "filename", where term1 was at place "col"

                    # retrieve Term-objects that are neigbours of previously mapped terms
                    new_term_names1 = set(df1.at[rec_ind, col_ind] for rec_ind in db1_row_ids if
                                          df1.at[rec_ind, col_ind] in free_term_names1)
                    new_term_names2 = set(df2.at[rec_ind, col_ind] for rec_ind in db2_row_ids if
                                          df2.at[rec_ind, col_ind] in free_term_names2)

                    # insert crossproduct of poss. new mappings into set
                    new_mapping_tuples |= find_crossproduct_mappings(new_term_names1, new_term_names2)

            # remove pairs, that are in prio_dict
            if setup.update_terms:
                update_tuples = processed_mapping_tuples & new_mapping_tuples
                update_existing_mappings(update_tuples, prio_dict, tuples_loc_sim, terms1_pq_mirror, terms2_pq_mirror)

            new_mapping_tuples -= processed_mapping_tuples

            add_mappings_to_pq(terms1, terms2, new_mapping_tuples, terms1_pq_mirror, terms2_pq_mirror,
                               prio_dict,processed_mapping_tuples, watch_exp_sim, similarity_metric,active_rid_combinations)

            if not prio_dict:
                new_hubs_flag = True
            # allow new hub-recomputation
            if not local_approval:
                local_approval = True
                print("now accepted: " + str(sim))

            l = sum(len(val) for val in prio_dict.values())
            if setup.debug:
                print("new length: " + str(l))
            watch_prio_len.append(l)

        # add new hubs, if prio_dict is empty
        elif len(free_term_names1) > 0 and len(free_term_names2) > 0 and new_hubs_flag:
            new_hubs_flag = False  # idea is to only find new hubs if in last iteration at least 1 mapping was added
            count_hub_recomp += 1

            # detect new hubs (term-objects) based on all free-terms for each Database
            hub_objs1 = find_hubs_quantile(free_term_names1, terms1)
            hub_objs2 = find_hubs_quantile(free_term_names2, terms2)

            new_mapping_tuples = find_crossproduct_mappings(hub_objs1, hub_objs2)
            add_mappings_to_pq(new_mapping_tuples, tuples_loc_sim, terms1_pq_mirror, terms2_pq_mirror,
                               prio_dict,processed_mapping_tuples, watch_exp_sim, similarity_metric,active_rid_combinations)

            l = sum(len(val) for val in prio_dict.values())
            if setup.debug:
                print("new length hubs: " + str(l))
            watch_prio_len.append(l)

        else:
            # Exit Strategy
            # map the remaining terms to dummies
            for term_name1 in free_term_names1:
                new_term = "new_var_" + str(mapping_obj.new_term_counter)
                # print("add new var: " + new_term + " for " + term)
                mapping_dict.append((term_name1, new_term))
                mapping_obj.new_term_counter += 1
            if len(mapping_dict) != len(terms1):
                s1 = set([x for (x, y) in mapping_dict])
                s2 = set(terms1.keys())
                print(s1 - s2)
                print(s2 - s1)
                raise ValueError(
                    "not same nr of mappings than terms: " + str(len(mapping_dict)) + " " + str(len(terms1)))

            break
    mapping_obj.mapping = pd.DataFrame.from_records(mapping_dict, columns=None)
    fig, ax = plt.subplots(4, 1)
    fig.suptitle("iterativeExpansion + " + similarity_metric.__name__)
    ax[0].scatter(range(len(watch_prio_len)), watch_prio_len, s=1, label='Queue Size')
    ax[0].set_title("Queue Size")
    ax[1].scatter(range(len(watch_exp_sim)), np.array(watch_exp_sim), s=1, label='Expanded Similarities')
    ax[1].set_title("Computed Similarities")
    ax[2].scatter(range(len(accepted_sim)), np.array(accepted_sim), s=0.5, label='Accepted Similarities')
    ax[2].set_title("Mapped Similarities")
    ax[3].hist(accepted_sim, 100, label='Accepted Mapping Distribution')
    ax[3].set_title("Distribution of Similarities")
    fig.tight_layout()
    plt.show()

    return uncertain_mapping_tuples, count_hub_recomp, len(tuples_loc_sim.keys())


# stop sim berechnung wenn maximum gefunden wurde?

def update_tuple_prio_dict(term_tuple,prio_dict,sim,new_sim):
    return



def delete_from_prio_dict(tuples, prio_dict, mapped_sim):
    uncertain_mapping_flag = False
    for sim, tuple_names in tuples:
        if sim not in prio_dict:
            ValueError("sim- key not in priority dict:" + str(sim))
        elif tuple_names not in prio_dict[sim]:
            continue
        # for the moment we just ignore, that the tuple was removed from the other side some iterations before
        # f.e. (t1,t3) was chosen before as mapping so (t1,t2) was removed in last iteration
        # now we pick (t4,t2) and would want to remove (t1,t2) again b
        # print("skipped value, bc it was removed from other side: " + str(tuple_names))
        else:
            prio_dict[sim].remove(tuple_names)
            # this is for logging, how often a mapping was done, where one of the terms had other tuples with the same
            # similarity
            if not uncertain_mapping_flag and sim >= mapped_sim:
                uncertain_mapping_flag = True
    return uncertain_mapping_flag


def find_crossproduct_mappings(hub_objs1, hub_objs2):
    return itertools.product(hub_objs1, hub_objs2)


# poss_mappings is a set of tuple
def add_mappings_to_pq(new_mapping_tuples, terms1_pq_mirror, terms2_pq_mirror,
                       prio_dict,processed_mapping_tuples, watch_exp_sim, similarity_metric,active_rid_combinations):
    for term_obj1, term_obj2 in new_mapping_tuples:
        term_tuple = TermTuple(term_obj1, term_obj2,similarity_metric)
        
        # active rid_combinations may reduce the overlap, because of our knowledge about the state of the mapping
        term_tuple.occurrence_overlap(active_rid_combinations)

        sim, rids1, rids2 = term_tuple.compute_similarity()
        # this check is currently not necessary but later, when adding struc-sim we need it

        # add tuple to priority_queue
        if sim > 0:
            prio_dict.setdefault(sim, []).append(term_tuple)

            processed_mapping_tuples.add(term_tuple)

            # add term & tuple to prio_dict-mirror-1
            terms1_pq_mirror.setdefault(term_tuple.term_obj1.name,list()).append((sim, term_tuple))

            # add term & tuple to prio_dict-mirror-2
            terms2_pq_mirror.setdefault(term_tuple.term_obj2.name,list()).append((sim, term_tuple))

            watch_exp_sim.append(sim)






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
    # if setup.debug: print(
    #    "node degree mean: " + str(round(np.mean(nodes), 2)) + " standard deviation: " + str(round(np.std(nodes), 2)))
    quantile = np.quantile(nodes, q=0.95)
    return set(terms[free_term_names[iter]] for iter in range(len(free_term_names)) if nodes[iter] >= quantile)


def unpack_multi_columns(cols):
    # returns a list of ints
    return list(map(int, cols.split("-")))