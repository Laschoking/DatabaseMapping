from collections import deque
import itertools
import numpy as np
import Python.Libraries.Classes as classes
from Python.Libraries.Classes import *
import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedList, SortedDict
import itertools
import Python.Config_Files.Setup as setup


# blocked terms only if DL-computation
def recompute_hubs(accepted_sim):
    q1 = np.percentile(accepted_sim, 25)
    q3 = np.percentile(accepted_sim, 75)
    IQR = q3 - q1
    low_outlier = q1 - 1.5 * IQR
    return low_outlier


def initialize_records(db):
    records = dict()
    for file_name,df in db.files.items():
        col_len = df.shape[1]
        for rid in range(len(df)):
            records[rid,file_name] = Record(rid, col_len,file_name)
    return records





def iterative_anchor_expansion(mapping_obj, db1, terms1, db2, terms2, blocked_terms, similarity_metric):
    prio_dict = SortedDict()

    records1 = initialize_records(db1)
    records2 = initialize_records(db2)
    expanded_record_tuples = dict()

    # those lists hold all terms, that are still mappable
    for term in terms1.keys():
        if type(term) is not str:
            print(term)
    free_term_names1 = SortedList(terms1.keys())
    free_term_names2 = SortedList(terms2.keys())

    mapping_dict = []

    tuples_loc_sim = SortedDict()
    processed_mapping_tuples = set()

    # block certain terms, that cannot be changed without computing wrong results
    '''    
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
    ''' # counts len, after mapping pop, del obsolete tuples & adding new tuples from neighbourhoods
    watch_prio_len = []
    watch_exp_sim = []
    accepted_sim = []
    uncertain_mapping_tuples = 0
    local_approval = setup.hub_recompute

    count_hub_recomp = 0
    new_hubs_flag = True

    while 1:
        if prio_dict and not new_hubs_flag:  # pop last item = with the highest similarity
            sim, tuples = prio_dict.peekitem(index=-1)
            # if setup.debug: print(prio_dict)

            # data could be empty because of deletion of obsolete term-tuples
            if not tuples:
                prio_dict.popitem(-1)
                continue

            # removes first data-item ( tuples appended later i.e. by hub recomputation are at the end)
            mapped_tuple = tuples.pop(0)
            term_name1, term_name2 = mapped_tuple.term_obj1.name,mapped_tuple.term_obj2.name
            sim = mapped_tuple.get_similarity()
            sub_rids1,sub_rids2,sub_rids = mapped_tuple.get_records()

            # last tuple in similarity bin -> delete empty bin

            if term_name1 not in free_term_names1 or term_name2 not in free_term_names2:
                ValueError("Term should not be vacant anymore: " + term_name1 + " " + term_name2)

            # if value is too bad - find new Hubs
            if setup.hub_recompute and accepted_sim and local_approval:
                low_outlier = recompute_hubs(accepted_sim)
                if sim < low_outlier:
                    # trigger new hub detection
                    new_hubs_flag = True
                    # insert sim & tuple back to dictionary
                    prio_dict[sim].append(mapped_tuple)
                    print("denied: " + str(sim))
                    # mark as false so at least 1 new mapping has to be added before we can trigger recomputation again
                    local_approval = False
                    continue
                    
            # add new mapping
            mapping_dict.append((term_name1, term_name2))

            #if setup.debug:print(f"mapped tuple: {mapped_tuple}")

            if setup.debug:
                print("-----------------------------")
                print(term_name1 + " -> " + term_name2 )#+ " " +  str(mapped_tuple))

            # make terms "blocked"
            free_term_names1.discard(term_name1)
            free_term_names2.discard(term_name2)

            # find all term-tuplesthat are not possible after the current mapping (i.e accept: A -> A , can never match B-> A )
            # remove them from prio_dict & unattatch them
            remove_term_tuples = set()
            remove_term_tuples |= mapped_tuple.term_obj1.attached_tuples
            remove_term_tuples |= mapped_tuple.term_obj2.attached_tuples
            remove_term_tuples.remove(mapped_tuple)

            delete_from_prio_dict(remove_term_tuples, prio_dict)
            for del_term_tuple in remove_term_tuples.copy():
                #print(f"delete ({del_term_tuple.term_obj1.name},{del_term_tuple.term_obj2.name})")
                del_term_tuple.unlink_from_term_parents()
                del_term_tuple.unlink_from_all_rid_tuples()

            l = sum(len(val) for val in prio_dict.values())
            watch_prio_len.append(l)
            accepted_sim.append(sim)

            # can be used later in the expansion
            expansion_rid_tuples = set() # filename {ridtuples}
            active_rid_tuples = set()
            outdated_rid_tuples = set()
            altered_term_tuples = set()
            # all tuples that are not active will be expanded & made active
            for rec_obj,mapped_rec_tuples in itertools.chain(sub_rids1.items(),sub_rids2.items()):
                # rec_obj active means that the connected mapped_rec_tuples are also active
                if rec_obj.is_active():
                    active_rid_tuples |= mapped_rec_tuples
                    outdated_rid_tuples |= rec_obj.active_records_tuples - mapped_rec_tuples

                # record-tuple was not active -> expand it in the discovery phase
                else:
                    rec_obj.set_active()
                    expansion_rid_tuples.update(mapped_rec_tuples)
            # this is a little redundant as we do it twice for each record-tuple
            for mapped_rec_tuple in sub_rids:
                # the record-tuple has the mapped_tuple as a subscriber, and
                mapped_rec_tuple.mark_filled_cols(mapped_tuple)

            # make rid-tuples that are now invalid inactive & find Term Tuples that need to be updated
            for outdated_rid_tuple in outdated_rid_tuples.copy():
                altered_tuples = outdated_rid_tuple.make_inactive()
                altered_term_tuples.update(altered_tuples)

            # update confidence value & possibly change position of mapping in the prio queue
            # this will delete the Term Tuple if it now has a similarity of 0 (hence we dont need to delete Term Tuples midway)
            update_tuples_prio_dict(altered_term_tuples, prio_dict)

            # expansion strategy:
            # current state: consider only terms, that occur in same colum of merged records
            new_mapping_tuples = set()
            for rec_tuple in expansion_rid_tuples:
                # TODO ggf. alles in Record Object reinladen, & auf dataframes verzichten
                file_name = rec_tuple.rec_obj1.file_name
                df1 = db1.files[file_name]
                df2 = db2.files[file_name]
                new_cols = rec_tuple.rec_obj1.vacant_cols # has the same result as rec_obj2.vacant_cols, because both are updated at the same time
                for col in new_cols:
                    term_name1 = df1.iat[rec_tuple.rec_obj1.rid,col]
                    term_name2 = df2.iat[rec_tuple.rec_obj2.rid, col]
                    new_mapping_tuples.add((terms1[term_name1],terms2[term_name2]))
                    #if setup.debug: print(f"term1: {term_name1} , term2:  {term_name2}")
            new_mapping_tuples -= processed_mapping_tuples

            add_mappings_to_pq(new_mapping_tuples,prio_dict,processed_mapping_tuples,
                               watch_exp_sim, similarity_metric,records1,records2,expanded_record_tuples)

            if not prio_dict:
                new_hubs_flag = True

            # allow new hub-recomputation
            if not local_approval:
                local_approval = True
                print("now accepted: " + str(sim))

            l = sum(len(val) for val in prio_dict.values())
            watch_prio_len.append(l)

            mapped_tuple.unlink_from_term_parents()
            mapped_tuple.unlink_from_all_rid_tuples()

        # add new hubs, if prio_dict is empty
        elif len(free_term_names1) > 0 and len(free_term_names2) > 0 and new_hubs_flag:
            new_hubs_flag = False  # idea is to only find new hubs if in last iteration at least 1 mapping was added
            count_hub_recomp += 1

            # detect new hubs (term-objects) based on all free-terms for each Database
            hub_objs1 = find_hubs_quantile(free_term_names1, terms1)
            hub_objs2 = find_hubs_quantile(free_term_names2, terms2)

            new_mapping_tuples = find_crossproduct_mappings(hub_objs1, hub_objs2)
            add_mappings_to_pq(new_mapping_tuples,
                               prio_dict,processed_mapping_tuples, watch_exp_sim, similarity_metric,records1,records2,expanded_record_tuples)

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
    plot_statistics(similarity_metric.__name__,watch_prio_len,watch_exp_sim,accepted_sim)

    return uncertain_mapping_tuples, count_hub_recomp, len(tuples_loc_sim.keys())


# stop sim berechnung wenn maximum gefunden wurde?

def update_tuples_prio_dict(sub_term_tuples,prio_dict):
    for sub_term_tuple in sub_term_tuples:
        old_sim = sub_term_tuple.get_similarity()
        new_sim = sub_term_tuple.compute_similarity()
        # similarity stayed the same
        if setup.debug: print(f"recompute sim : ({sub_term_tuple.term_obj1.name},{sub_term_tuple.term_obj2.name}) old sim: {old_sim}, new sim: {new_sim}")

        if old_sim == new_sim:
            return
        prio_dict[old_sim].remove(sub_term_tuple)
        if new_sim == 0:
            # the tuple does not fulfil any potential record tuple anymore so its useless
            sub_term_tuple.unlink_from_term_parents()
            sub_term_tuple.unlink_from_all_rid_tuples()
            #if setup.debug: print(f" deleted Term Tuple {sub_term_tuple.term_obj1.name},{sub_term_tuple.term_obj2.name} with Similarity = 0")
        else:
            prio_dict.setdefault(new_sim,list()).append(sub_term_tuple)




def delete_from_prio_dict(remove_term_tuples, prio_dict):
    for mapped_tuple in remove_term_tuples:
        sim = mapped_tuple.get_similarity()

        if sim not in prio_dict:
            ValueError("sim- key not in priority dict:" + str(sim))
        else:
            prio_dict[sim].remove(mapped_tuple)

def find_crossproduct_mappings(hub_objs1, hub_objs2):
    return itertools.product(hub_objs1, hub_objs2)


# poss_mappings is a set of tuple
def add_mappings_to_pq(new_mapping_tuples,
                       prio_dict,processed_mapping_tuples, watch_exp_sim, similarity_metric,records1,records2,expanded_record_tuples):

    for term_obj1, term_obj2 in new_mapping_tuples:
        new_tuple = classes.TermTuple(term_obj1, term_obj2,similarity_metric)
        
        # active rid_combinations may reduce the overlap, because of our knowledge about the state of the mapping
        new_tuple.occurrence_overlap(records1,records2,expanded_record_tuples)

        sim = new_tuple.compute_similarity()
        # this check is currently not necessary but later, when adding struc-sim we need it
        # add tuple to priority_queue
        if sim > 0:
            if setup.debug: print(f"expanded tuple: {new_tuple.term_obj1.name},{new_tuple.term_obj2.name}, sim: {sim}")
            prio_dict.setdefault(sim, list()).append(new_tuple)

            processed_mapping_tuples.add((term_obj1,term_obj2))

            watch_exp_sim.append(sim)


def find_hubs_quantile(free_term_names, terms):
    nodes = [terms[term_name].degree for term_name in free_term_names]
    quantile = np.quantile(nodes, q=0.95)
    return set(terms[free_term_names[iter]] for iter in range(len(free_term_names)) if nodes[iter] >= quantile)

def plot_statistics(metric_name,watch_prio_len,watch_exp_sim,accepted_sim ):
    fig, ax = plt.subplots(4, 1)
    fig.suptitle("iterativeExpansion + " + metric_name)
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
    pass