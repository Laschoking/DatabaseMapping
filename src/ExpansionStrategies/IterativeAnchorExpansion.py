import matplotlib.pyplot as plt
import pandas as pd
from sortedcontainers import SortedDict,SortedList,SortedSet
from src.Config_Files.Debug_Flags import DEBUG_TERMS, DEBUG_RECORDS, debug_set, PLOT_STATISTICS
from src.Classes.ExpansionStrategy import  ExpansionStrategy
import src.Classes.DomainElements

class IterativeAnchorExpansion(ExpansionStrategy):
    def __init__(self,anchor_quantile,DYNAMIC,sim_th=0.0):
        super().__init__("Local",anchor_quantile,DYNAMIC,sim_th)


    def accept_expand_mappings(self, mapping_func, elements_db1, elements_db2, blocked_elements, similarity_metric):
        self.anchor_quantile.reset_quantile() # Reset quantile since it is a shared object with other MappingContainers
        Q = SortedList()

        expanded_fact_pairs = dict()
        exp_anchor_mappings = set()
        processed_mappings = set()

        # Count how many elements in DB1 and DB2 are still vacant
        c_free_elements_db1 = len(elements_db1.keys())
        c_free_elements_db2 = len(elements_db2.keys())

        mapping_dict = []

        # Logging variables
        w_prio_len, w_exp_sim, mapped_sims = [], [], []

        # Flag to signalise the detection of new anchor mappings
        new_anchor_mappings = True

        while 1:
            if Q and not new_anchor_mappings:
                if len(mapped_sims) % 100 == 0:
                    print(f"mapped {len(mapped_sims)} element pairs, size of Q: {len(Q)}")

                # The prio_dict is sorted in ascending order, so the last value has the highest similarity
                accepted_mapping = Q.pop(index=-1)

                # Update & expand its neighbourhood
                accepted_sim = accepted_mapping.get_similarity()
                if DEBUG_TERMS or accepted_mapping in debug_set:
                    print("-----------------------------")
                    print(f"{accepted_mapping.element1.name}  -> {accepted_mapping.element2.name} with sim: {accepted_sim}")

                sub_fact_pairs = accepted_mapping.get_clean_fact_pairs()

                # Accept_this_mapping also handles the deactivation of facts, that cannot get matched anymore
                related_mappings, altered_mappings,finished_fact_pairs = accepted_mapping.accept_this_mapping(DYNAMIC=self.DYNAMIC)

                # Save complete fact tuples for efficient matching of databases
                for file_name, fact_pairs in finished_fact_pairs.items():
                    mapping_func.final_fact_pairs.setdefault(file_name, set()).update(fact_pairs)

                # Reduce free element counter
                c_free_elements_db1 -= 1
                c_free_elements_db2 -= 1

                # Save selected mapping_func
                mapping_dict.append((accepted_mapping.element1.name, accepted_mapping.element2.name,accepted_sim))
                if (accepted_mapping.element1,accepted_mapping.element2) in exp_anchor_mappings:
                    mapping_func.c_accepted_anchor_mappings += 1

                # Remove the mappings that are now not possible anymore from prio_dict
                c_uncertain_mapping = self.delete_from_prio_dict(related_mappings, accepted_mapping, Q)
                mapping_func.c_uncertain_mappings += c_uncertain_mapping

                w_prio_len.append(len(Q))
                mapped_sims.append(accepted_sim)

                expansion_fact_pairs = set()
                outdated_fact_pairs = set()

                # Iterate through the subscribed fact tuples by fact (from DB1 or DB2), and connected fact_pairs
                for fact, mapped_fact_pairs in sub_fact_pairs.items():

                    # Find fact_pairs that have fact inside but are not fulfilled by the accepted mapping_func
                    outdated_fact_pairs |= fact.get_all_fact_pairs() - mapped_fact_pairs

                    # If the fact is not in_process, no elements within have been already mapped (except the current)
                    # Therefor the connected fact_pairs should be expanded
                    if not fact.is_in_process():
                        fact.in_process = True
                        if DEBUG_TERMS:
                            print(f"make fact in process: {fact.file, fact.index}")

                        expansion_fact_pairs.update(mapped_fact_pairs - outdated_fact_pairs)

                # Deactivate invalid fact_pairs and save all mappings that subscribed to the fact_pairs
                for outdated_fact_pair in outdated_fact_pairs.copy():
                    altered_tuples = outdated_fact_pair.make_inactive()
                    if self.DYNAMIC:
                        altered_mappings |= altered_tuples

                # Remove mappings that will be deleted anyway from the mappings that need an update
                altered_mappings -= related_mappings

                # Update the similarity score and possibly change the position of the updated mappings in the prio_dict
                self.update_tuples_prio_dict(altered_mappings, Q)

                # Reveal the two facts of each expanded fact_pair and
                # add element-tuples in the same column as potential new mappings

                new_mappings = set()
                for fact_pair in expansion_fact_pairs:
                    if DEBUG_TERMS or fact_pair in debug_set:
                        print(
                            f"expand fact tuple: {fact_pair.fact1.file}({fact_pair.fact1.index},{fact_pair.fact2.index})")
                    new_cols = fact_pair.fact1.vacant_cols  # has the same result as fact2.vacant_cols, because both are updated at the same time
                    for col in new_cols:
                        element1 = fact_pair.fact1.elements[col]
                        element2 = fact_pair.fact2.elements[col]
                        new_mappings.add((element1, element2))

                # couldnt it be that we have f1(a,b,c) & f2(a,b,b), b -> b is getting expanded even though there are no fact pairs?
                # Make sure to only expand mappings, that have not been processed before
                # This could happen for anchor pairs, that were expanded, and then get expanded again, bc. the facts are activated now
                new_mappings -= processed_mappings

                # Create new mappings from the mappings and insert them into prio_dict
                processed_mappings |= self.add_mappings_to_pq(new_mappings, Q,
                                   w_exp_sim, similarity_metric, expanded_fact_pairs)


                # Trigger new anchor mappings if the prio_dict is empty after expansion
                if not Q:
                    new_anchor_mappings = True

                w_prio_len.append(len(Q))

            # Find anchor mappings if some elements are still vacant
            elif c_free_elements_db1 > 0 and c_free_elements_db2 > 0 and new_anchor_mappings:
                if DEBUG_TERMS or DEBUG_RECORDS:
                    print(f"Find new hubs: {c_free_elements_db1}")
                #print(f"started anchor computation, len: {w_prio_len}")

                new_anchor_mappings = False  # idea is to only find new hubs if in last iteration at least 1 mapping_func was added
                mapping_func.c_hub_recomp += 1

                # Find anchor elements for DB1 and DB2
                anchor_elements1 = self.anchor_quantile.calc_anchor_elements(elements_db1)
                anchor_elements2 = self.anchor_quantile.calc_anchor_elements(elements_db2)
                print(f"anchor length: {len(anchor_elements1),len(anchor_elements2)}")

                mapping_func.anchor_nodes[0] |= anchor_elements1
                mapping_func.anchor_nodes[1] |= anchor_elements2

                # Combine anchor elements pairwise and insert them into the prio_dict
                new_mappings = set((element1, element2) for element1 in anchor_elements1 for element2 in anchor_elements2)
                exp_mappings = self.add_mappings_to_pq(new_mappings,Q, w_exp_sim, similarity_metric, expanded_fact_pairs)
                exp_anchor_mappings |= exp_mappings
                processed_mappings |= exp_mappings

                # Increase the quantile and trigger new anchor mappings if no anchor mappings with sim > 0 were found
                if not exp_mappings and self.anchor_quantile.q > 0:
                    self.anchor_quantile.double_quantile()
                    new_anchor_mappings = True

                w_prio_len.append(len(Q))
                #print(f"finished anchor computation, len: {w_prio_len}")


            # Exit Strategy
            else:
                # Map remaining elements to dummy strings
                for element in elements_db1.values():
                    if element.is_active():
                        new_element = "new_var_" + str(mapping_func.syn_counter)
                        mapping_dict.append((element.name, new_element,0.01))
                        mapping_func.syn_counter += 1
                        if DEBUG_TERMS or element in debug_set:
                            print(f"added synthetic element ({element.name},{new_element})")

                # Make quality check if each element in DB1 received mapping_func
                if len(mapping_dict) != len(elements_db1):
                    print(len(mapping_dict))
                    print(len(elements_db1))
                    s1 = set([x for (x, y, z) in mapping_dict])
                    s2 = set([y for (x, y, z) in mapping_dict])

                    print(s1 ^ set(elements_db1.keys()))
                    print(s2 ^ set(elements_db2.keys()))
                    raise ValueError(
                        "not same nr of mappings than elements: " + str(len(mapping_dict)) + " " + str(len(elements_db1)))
                break

        # Load all mappings into the dataframe at once
        mapping_func.final_mapping = pd.DataFrame.from_records(mapping_dict, columns=['element1', 'element2', 'sim'])
        if PLOT_STATISTICS:
            self.plot_statistics(similarity_metric.name,w_prio_len,w_exp_sim,mapped_sims)

        return len(processed_mappings)


