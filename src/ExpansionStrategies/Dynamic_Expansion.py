import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedDict
import src.Config_Files.Setup as setup

import src.Classes.Terms


# blocked terms only if DL-computation
def recompute_hubs(accepted_sim):
    q1 = np.percentile(accepted_sim, 25)
    q3 = np.percentile(accepted_sim, 75)
    IQR = q3 - q1
    low_outlier = q1 - 1.5 * IQR
    return low_outlier








def iterative_anchor_expansion(mapping_obj, records1, terms1, records2, terms2, blocked_terms, similarity_metric):
    prio_dict = SortedDict()

    expanded_record_tuples = dict()
    hub_mapping_tuples = dict() # remember all term tuples created by hubs
    processed_term_tuples = set()
    # those lists hold all terms, that are still mappable
    c_free_terms1 = len(terms1.keys())
    c_free_terms2 = len(terms2.keys())

    mapping_dict = []

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

    new_hubs_flag = True

    while 1:
        if prio_dict and not new_hubs_flag:  # pop last item = with the highest similarity
            sim, tuples = prio_dict.peekitem(index=-1)

            # data could be empty because of deletion of obsolete term-tuples
            if not tuples:
                prio_dict.popitem(-1)
                continue

            # removes first data-item ( tuples appended later i.e. by hub recomputation are at the end)
            mapped_tuple = tuples.pop(0)

            # if value is too bad - find new Hubs
            if setup.hub_recompute and accepted_sim and local_approval:
                low_outlier = recompute_hubs(accepted_sim)
                if mapped_sim < low_outlier:
                    # trigger new hub detection
                    new_hubs_flag = True
                    # insert mapped_sim & tuple back to dictionary
                    prio_dict[mapped_sim].append(mapped_tuple)
                    if setup.debug: print("denied: " + str(mapped_sim))
                    # mark as false so at least 1 new mapping has to be added before we can trigger recomputation again
                    local_approval = False
                    continue

            # confidence value was accepted:

            mapped_sim = mapped_tuple.get_similarity()
            #if setup.debug or mapped_tuple in setup.debug_set:
            print("-----------------------------")
            print(f"{mapped_tuple.term_obj1.name}  -> {mapped_tuple.term_obj2.name} with sim: {mapped_sim}")

            delete_term_tuples, altered_term_tuples = mapped_tuple.accept_this_mapping() # returns mappings that are now invalid & or need to be updated
            sub_rids = mapped_tuple.get_clean_record_tuples()

            # reduce free term counter
            c_free_terms1 -= 1
            c_free_terms2 -= 1

            # add new mapping
            mapping_dict.append((mapped_tuple.term_obj1.name, mapped_tuple.term_obj2.name))



            # remove tuples from prio_dict, that are now not possible anymore
            delete_from_prio_dict(delete_term_tuples, prio_dict)

            l = sum(len(val) for val in prio_dict.values())
            watch_prio_len.append(l)
            accepted_sim.append(mapped_sim)

            # will be used later in the expansion
            expansion_rid_tuples = set()
            # denotes record-tuples that are now invalid, bc. the mapping does not support them
            outdated_rid_tuples = set()
        # TODO an accepted mapping can also invalidate record-tuple that are active but not in process!
            # all tuples that are not active will be expanded & made active
            for rec_obj,mapped_rec_tuples in sub_rids.items():
                # rec_obj active means that the connected mapped_rec_tuples are also active
                if rec_obj.is_in_process():
                    outdated_rid_tuples |= rec_obj.get_active_record_tuples() - mapped_rec_tuples
                # record-tuple was not active -> expand it in the discovery phase

                # in case the tuple was a hub, it could remove not-in-process record-tuples, that other hubs-tuples were relying on
                # TODO i think this can also happen for non hubs see above
                elif mapped_tuple in hub_mapping_tuples.copy().values():
                    rec_obj.in_process = True
                    outdated_rid_tuples |= rec_obj.get_all_record_tuples() - mapped_rec_tuples
                    expansion_rid_tuples.update(mapped_rec_tuples - outdated_rid_tuples)
                else:
                    rec_obj.in_process = True
                    expansion_rid_tuples.update(mapped_rec_tuples)

            if mapped_tuple in hub_mapping_tuples.copy():
                del hub_mapping_tuples[mapped_tuple.term_obj1, mapped_tuple.term_obj2]

            # make rid-tuples that are now invalid inactive & find Term Tuples that need to be updated
            for outdated_rid_tuple in outdated_rid_tuples.copy():
                altered_tuples = outdated_rid_tuple.make_inactive()
                altered_term_tuples |= altered_tuples

            # there are cases, where a term-tuple is deleted (from  A -> A follows that (A,B) should be deleted. At the same time
            # A -> A also restricts the record-tuples, which can again affect  (A,B). since it is already deleted, we take all deleted out
            altered_term_tuples -= delete_term_tuples
            # update confidence value & possibly change position of mapping in the prio queue
            # this will delete the Term Tuple if it now has a similarity of 0 (hence we dont need to delete Term Tuples midway)
            update_tuples_prio_dict(altered_term_tuples, prio_dict)

            # expansion strategy: consider only terms, that occur in same colum of merged records
            new_mapping_tuples = set()
            for rec_tuple in expansion_rid_tuples:
                if setup.debug or rec_tuple in setup.debug_set:
                    print(f"expand record tuple: {rec_tuple.rec_obj1.file_name}({rec_tuple.rec_obj1.rid},{rec_tuple.rec_obj2.rid})")
                new_cols = rec_tuple.rec_obj1.vacant_cols # has the same result as rec_obj2.vacant_cols, because both are updated at the same time
                for col in new_cols:
                    term_obj1 = rec_tuple.rec_obj1.terms[col]
                    term_obj2 = rec_tuple.rec_obj2.terms[col]
                    new_mapping_tuples.add((term_obj1,term_obj2))

            # we need this as Unit_Test_Double_Expansion shows (it can otherwise happen, that a term tuple is expanded several times)
            new_mapping_tuples -= processed_term_tuples
            processed_term_tuples |= new_mapping_tuples

            # after accepting the first hub-term-tuple, we will expand it
            # however, it could be that we thus expand a term-tuple that is already in prio-dict, bc. it is also a hub-tuple
            # we need however to trigger that the remaining hubtuples are recomputated, bc. the first hub-tuple could have invalidated some record-tuples
            expand_hub_tuples = new_mapping_tuples.intersection(hub_mapping_tuples.keys())
            #update_tuples_prio_dict([hub_mapping_tuples[t1,t2] for (t1,t2) in expand_hub_tuples],prio_dict)
            new_mapping_tuples -= expand_hub_tuples
            add_mappings_to_pq(new_mapping_tuples,prio_dict,
                               watch_exp_sim, similarity_metric,expanded_record_tuples)

            # if prio dict is empty after expansion, we need to search new hubs
            if not prio_dict:
                new_hubs_flag = True

            # allow new hub-recomputation
            if not local_approval:
                local_approval = True
                #print("now accepted: " + str(mapped_sim))

            l = sum(len(val) for val in prio_dict.values())
            watch_prio_len.append(l)
            # TODO: do we have to do sth. with mapped_tuple even?
            #mapped_tuple.
            #mapped_tuple.unlink_from_all_rid_tuples()

        # add new hubs, if prio_dict is empty & free terms exist in DB1 and DB2
        elif c_free_terms1 > 0 and c_free_terms2 > 0 and new_hubs_flag:
            new_hubs_flag = False  # idea is to only find new hubs if in last iteration at least 1 mapping was added
            mapping_obj.c_hub_recomp += 1

            # detect new hubs (term-objects) based on all free-terms for each Database
            hub_objs1 = find_hubs_quantile(terms1)
            hub_objs2 = find_hubs_quantile(terms2)

            new_mapping_tuples = combine_hub_terms_pairwise(hub_objs1, hub_objs2) # return iterable
            add_mappings_to_pq(new_mapping_tuples,
                               prio_dict, watch_exp_sim, similarity_metric,expanded_record_tuples,hub_mapping_tuples)

            l = sum(len(val) for val in prio_dict.values())
            #if setup.debug:
            #    print("new length hubs: " + str(l))
            watch_prio_len.append(l)


        # Exit Strategy
        # map the remaining terms to dummy strings
        else:
            for term_obj in terms1.values():
                if term_obj.is_active():
                    new_term = "new_var_" + str(mapping_obj.new_term_counter)
                    # print("add new var: " + new_term + " for " + term)
                    mapping_dict.append((term_obj.name, new_term))
                    mapping_obj.new_term_counter += 1
                    if setup.debug or term_obj in setup.debug_set:
                        print(f"added synthetic term ({term_obj.name},{new_term})")
            if len(mapping_dict) != len(terms1):
                #print(mapping_dict)
                print(len(mapping_dict))
                print(len(terms1))
                '''s1 = set([x for (x, y) in mapping_dict])
                s2 = set([y for (x, y) in mapping_dict])

                print(s1 ^ set(terms1.keys()))
                print(s2 ^ set(terms2.keys()))
                raise ValueError(
                    "not same nr of mappings than terms: " + str(len(mapping_dict)) + " " + str(len(terms1)))
                '''

            break
    mapping_obj.mapping = pd.DataFrame.from_records(mapping_dict, columns=None)
    #plot_statistics(similarity_metric.__name__,watch_prio_len,watch_exp_sim,accepted_sim)

    return #uncertain_mapping_tuples, 0


def update_tuples_prio_dict(sub_term_tuples,prio_dict):
    for sub_term_tuple in sub_term_tuples:
        old_sim = sub_term_tuple.get_similarity()
        new_sim = sub_term_tuple.recompute_similarity()
        # similarity stayed the same
        if setup.debug or sub_term_tuple.term_obj1 in setup.debug_set or sub_term_tuple.term_obj2 in setup.debug_set:
            print(f"recompute sim : ({sub_term_tuple.term_obj1.name},{sub_term_tuple.term_obj2.name}) old sim: {old_sim}, new sim: {new_sim}")

        # this tuple stays the same, so no need to update the priority_dict
        if old_sim == new_sim:
            continue
        prio_dict[old_sim].remove(sub_term_tuple)
        if new_sim == 0:
            # the tuple does not fulfil any potential record tuple anymore so its useless
            sub_term_tuple.gen_active = False
            if setup.debug or sub_term_tuple.term_obj1 in setup.debug_set or sub_term_tuple.term_obj2 in setup.debug_set:
                print(f" deleted Term Tuple {sub_term_tuple.term_obj1.name},{sub_term_tuple.term_obj2.name} with Similarity = 0")
        else:
            prio_dict.setdefault(new_sim,list()).append(sub_term_tuple)


def delete_from_prio_dict(remove_term_tuples, prio_dict):
    for mapped_tuple in remove_term_tuples:
        sim = mapped_tuple.get_similarity()
        if setup.debug or mapped_tuple.term_obj1 in setup.debug_set or mapped_tuple.term_obj2 in setup.debug_set:
            print(f"remove ({mapped_tuple.term_obj1.name},{mapped_tuple.term_obj2.name}) with sim {sim} from prio-dict")
        if sim not in prio_dict:
            raise ValueError("sim- key not in priority dict:" + str(sim))
        else:
            prio_dict[sim].remove(mapped_tuple)




# poss_mappings is a set of tuple
def add_mappings_to_pq(new_mapping_tuples,
                       prio_dict, watch_exp_sim, similarity_metric,expanded_record_tuples,hub_mapping_tuples=None):

    for term_obj1, term_obj2 in new_mapping_tuples:
        if not term_obj1.is_active() :
            print(f" term1 was mapped already: {term_obj1.name} to {term_obj2.name}")
            continue

        if not term_obj2.is_active():
            print(f"term2 was mapped already: {term_obj1.name} to {term_obj2.name}")
            continue

        new_tuple = src.Classes.Terms.TermTuple(term_obj1, term_obj2, expanded_record_tuples, similarity_metric)
        
        # active rid_combinations may reduce the overlap, because of our knowledge about the state of the mapping
        #new_tuple.calc_initial_record_tuples()

        sim = new_tuple.compute_similarity()
        # this check is currently not necessary but later, when adding struc-sim we need it
        # add tuple to priority_queue
        if sim > 0:
            if setup.debug or term_obj1 in setup.debug_set or term_obj2 in setup.debug_set:
                print(f"expanded tuple: {new_tuple.term_obj1.name},{new_tuple.term_obj2.name}, sim: {sim}")
            prio_dict.setdefault(sim, list()).append(new_tuple)
            watch_exp_sim.append(sim)

            if hub_mapping_tuples is not None:
                hub_mapping_tuples[term_obj1, term_obj2] = new_tuple
        else:
            new_tuple.gen_active = False


def find_hubs_quantile(terms):
    nodes = []
    pot_hubs = []
    for term_obj in terms.values():
        if term_obj.is_active():
            nodes.append(term_obj.degree)
            pot_hubs.append(term_obj)
    quantile = np.quantile(nodes, q=0.95)
    return set(pot_hubs[i] for i in range(len(nodes)) if nodes[i] >= quantile)

def combine_hub_terms_pairwise(hub_objs1, hub_objs2):
    return set((hub_obj1, hub_obj2) for hub_obj1 in hub_objs1 for hub_obj2 in hub_objs2)

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