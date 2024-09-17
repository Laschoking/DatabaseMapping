import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedDict,SortedList,SortedSet
from src.Config_Files.Debug_Flags import DEBUG_TERMS, DEBUG_RECORDS, debug_set,HUB_RECOMPUTE, PLOT_STATISTICS
from src.Classes.ExpansionStrategy import  ExpansionStrategy
import src.Classes.Terms

class IterativeAnchorExpansion(ExpansionStrategy):
    def __init__(self,anchor_quantile,DYNAMIC):
        super().__init__("Iterative",anchor_quantile,DYNAMIC)


    def accept_expand_mappings(self,mapping, terms_db1, terms_db2, blocked_terms,similarity_metric):
        self.anchor_quantile.reset_quantile() # Reset quantile since it is a shared object with other MappingContainers
        prio_dict = SortedList()

        expanded_record_tuples = dict()
        exp_anchor_mappings = set()
        processed_mappings = set()

        # Count how many terms in DB1 and DB2 are still vacant
        c_free_terms_db1 = len(terms_db1.keys())
        c_free_terms_db2 = len(terms_db2.keys())

        mapping_dict = []

        # Logging variables
        w_prio_len, w_exp_sim, mapped_sims = [], [], []

        # Flag to signalise the detection of new anchor mappings
        new_anchor_mappings = True

        # If Datalog Rules are executed on the merged database, match the following terms to themselves
        '''for blocked_term in blocked_terms:
            if blocked_term in terms_db1.keys():
                term = terms_db1[blocked_term]
                term.deactivate_term_and_all_tt()
                # map term to itself
                mapping_dict.append((blocked_term, blocked_term))
                c_free_terms_db1 -= 1
                # if in terms_db2 then delete occurrence there
                if blocked_term in terms_db2.keys():
                    terms_db2[blocked_term].deactivate_term_and_all_tt()
                    c_free_terms_db2 -= 1
                else:
                    # for counting, how many terms are mapped to synthetic values (that do not exist in db2)
                    mapping.syn_counter += 1
        '''

        while 1:
            if prio_dict and not new_anchor_mappings:
                # The prio_dict is sorted in ascending order, so the last value has the highest similarity
                accepted_mapping = prio_dict.pop(index=-1)

                # Update & expand its neighbourhood
                accepted_sim = accepted_mapping.get_similarity()
                if DEBUG_TERMS or accepted_mapping in debug_set:
                    print("-----------------------------")
                    print(f"{accepted_mapping.term1.name}  -> {accepted_mapping.term2.name} with sim: {accepted_sim}")

                sub_rec_tuples = accepted_mapping.get_clean_record_tuples()

                related_mappings, altered_mappings,finished_record_tuples = accepted_mapping.accept_this_mapping()

                # Save complete record tuples for efficient matching of databases
                for file_name, rec_tuples in finished_record_tuples.items():
                    mapping.final_rec_tuples.setdefault(file_name,set()).update(rec_tuples)

                # Reduce free term counter
                c_free_terms_db1 -= 1
                c_free_terms_db2 -= 1

                # Save selected mapping
                mapping_dict.append((accepted_mapping.term1.name, accepted_mapping.term2.name,accepted_sim))
                if (accepted_mapping.term1,accepted_mapping.term2) in exp_anchor_mappings:
                    mapping.c_accepted_anchor_mappings += 1

                # Remove the mappings that are now not possible anymore from prio_dict
                c_uncertain_mapping = self.delete_from_prio_dict(related_mappings, accepted_mapping, prio_dict)
                mapping.c_uncertain_mappings += c_uncertain_mapping

                w_prio_len.append(len(prio_dict))
                mapped_sims.append(accepted_sim)

                expansion_rid_tuples = set() # this is to make sure a coherent ordered of expansion
                outdated_rid_tuples = set()

                # Iterate through the subscribed record tuples by record (from DB1 or DB2), and connected record_tuples
                for record, mapped_rec_tuples in sub_rec_tuples.items():

                    # Find Record_tuples that have record inside but are not fulfilled by the accepted mapping
                    outdated_rid_tuples |= record.get_all_record_tuples() - mapped_rec_tuples

                    # If the record is not in_process, no terms within have been already mapped (except the current)
                    # Therefor the connected record_tuples should be expanded
                    if not record.is_in_process():
                        record.in_process = True
                        if DEBUG_TERMS:
                            print(f"make record in process: {record.file_name, record.rid}")

                        expansion_rid_tuples.update(mapped_rec_tuples - outdated_rid_tuples)


                # Deactivate invalid record_tuples and save mappings were subscribed to the record_tuples
                for outdated_rid_tuple in outdated_rid_tuples.copy():
                    altered_tuples = outdated_rid_tuple.make_inactive()
                    if self.DYNAMIC:
                        altered_mappings |= altered_tuples

                # Remove mappings that will be deleted anyway from the mappings that need an update
                altered_mappings -= related_mappings

                # Update the similarity score and possibly change the position of the updated mappings in the prio_dict
                self.update_tuples_prio_dict(altered_mappings, prio_dict)

                # Reveal the two records of each expanded record_tuple and
                # add term-tuples in the same column as potential new mappings

                new_mappings = set()
                for rec_tuple in expansion_rid_tuples:
                    if DEBUG_TERMS or rec_tuple in debug_set:
                        print(
                            f"expand record tuple: {rec_tuple.record1.file_name}({rec_tuple.record1.rid},{rec_tuple.record2.rid})")
                    new_cols = rec_tuple.record1.vacant_cols  # has the same result as record2.vacant_cols, because both are updated at the same time
                    for col in new_cols:
                        term1 = rec_tuple.record1.terms[col]
                        term2 = rec_tuple.record2.terms[col]
                        new_mappings.add((term1, term2))

                # Make sure to only expand mappings, that have not been processed before
                new_mappings -= processed_mappings

                # Create new mappings from the mappings and insert them into prio_dict
                processed_mappings |= self.add_mappings_to_pq(new_mappings, prio_dict,
                                   w_exp_sim, similarity_metric, expanded_record_tuples)


                # Trigger new anchor mappings if the prio_dict is empty after expansion
                if not prio_dict:
                    new_anchor_mappings = True

                w_prio_len.append(len(prio_dict))


            # Find anchor mappings if some terms are still vacant
            elif c_free_terms_db1 > 0 and c_free_terms_db2 > 0 and new_anchor_mappings:
                if DEBUG_TERMS or DEBUG_RECORDS:
                    print(f"Find new hubs: {c_free_terms_db1}")
                new_anchor_mappings = False  # idea is to only find new hubs if in last iteration at least 1 mapping was added
                mapping.c_hub_recomp += 1


                # Find anchor terms for DB1 and DB2
                anchor_terms1 = self.anchor_quantile.calc_anchor_terms(terms_db1)
                anchor_terms2 = self.anchor_quantile.calc_anchor_terms(terms_db2)

                mapping.anchor_nodes[0] |= anchor_terms1
                mapping.anchor_nodes[1] |= anchor_terms2

                # Combine anchor terms pairwise and insert them into the prio_dict
                new_mappings = set((term1, term2) for term1 in anchor_terms1 for term2 in anchor_terms2)
                exp_mappings = self.add_mappings_to_pq(new_mappings,prio_dict, w_exp_sim, similarity_metric, expanded_record_tuples)
                exp_anchor_mappings |= exp_mappings
                processed_mappings |= exp_mappings

                # Increase the quantile and trigger new anchor mappings if no anchor mappings with sim > 0 were found
                if not exp_mappings and self.anchor_quantile.q > 0:
                    self.anchor_quantile.double_quantile()
                    new_anchor_mappings = True

                w_prio_len.append(len(prio_dict))


            # Exit Strategy
            else:
                # Map remaining terms to dummy strings
                for term in terms_db1.values():
                    if term.is_active():
                        new_term = "new_var_" + str(mapping.syn_counter)
                        mapping_dict.append((term.name, new_term,0.01))
                        mapping.syn_counter += 1
                        if DEBUG_TERMS or term in debug_set:
                            print(f"added synthetic term ({term.name},{new_term})")

                # Make quality check if each element in DB1 received mapping
                if len(mapping_dict) != len(terms_db1):
                    print(len(mapping_dict))
                    print(len(terms_db1))
                    s1 = set([x for (x, y, z) in mapping_dict])
                    s2 = set([y for (x, y, z) in mapping_dict])

                    print(s1 ^ set(terms_db1.keys()))
                    print(s2 ^ set(terms_db2.keys()))
                    raise ValueError(
                        "not same nr of mappings than terms: " + str(len(mapping_dict)) + " " + str(len(terms_db1)))
                break

        # Load all mappings into the dataframe at once
        mapping.final_mapping = pd.DataFrame.from_records(mapping_dict, columns=None)
        if PLOT_STATISTICS:
            self.plot_statistics(similarity_metric.name,w_prio_len,w_exp_sim,mapped_sims)

        return len(processed_mappings)


