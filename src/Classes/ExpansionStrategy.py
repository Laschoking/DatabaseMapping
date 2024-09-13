import matplotlib.pyplot as plt
import numpy as np
from src.Config_Files.Debug_Flags import DEBUG_TERMS,DEBUG_RECORDS, debug_set,HUB_RECOMPUTE, PLOT_STATISTICS,DYNAMIC_EXPANSION
from src.Classes.Terms import Mapping


class ExpansionStrategy:
    def __init__(self,name,anchor_quantile,sim_outlier,DYNAMIC):
        self.name = name
        self.anchor_quantile = anchor_quantile
        self.sim_outlier = sim_outlier
        self.DYNAMIC = DYNAMIC


    def accept_expand_mappings(self,mapping, terms_db1, terms_db2, blocked_terms,
                                   similarity_metric):
        pass

    def update_tuples_prio_dict(self,sub_mappings, prio_dict):
        update_mappings = []
        for sub_mapping in sub_mappings:
            # remove the tuple before the value is recomputed otherwise the SortedList is getting inconsistent
            if sub_mapping not in prio_dict:
                print(prio_dict)
            prio_dict.remove(sub_mapping)

            old_sim = sub_mapping.get_similarity()
            new_sim = sub_mapping.recompute_similarity()
            # similarity stayed the same
            if DEBUG_TERMS or sub_mapping.term1 in debug_set or sub_mapping.term2 in debug_set:
                print(
                    f"recompute sim : ({sub_mapping.term1.name},{sub_mapping.term2.name}) old sim: {old_sim}, new sim: {new_sim}")



            # Leave mappings with not-changing similarity in prio_dict
            if old_sim == new_sim:
                update_mappings.append(sub_mapping)
                continue

            # Remove mappings that are now obsolete
            if new_sim == 0:
                sub_mapping.gen_active = False

                if DEBUG_TERMS or sub_mapping.term1 in debug_set or sub_mapping.term2 in debug_set:
                    print(
                        f" deleted Term Tuple {sub_mapping.term1.name},{sub_mapping.term2.name} with Similarity = 0")

            # Insert mapping in prio_dict with updated similarity score
            else:
                update_mappings.append(sub_mapping)

        if update_mappings:
            prio_dict.update(update_mappings)

    def delete_from_prio_dict(self,remove_mappings, accepted_mapping, prio_dict):
        # This logs, if the accepted mapping was insecure because a related mapping had the same similarity score
        uncertain_mapping = 0
        for mapped_tuple in remove_mappings:
            if mapped_tuple not in prio_dict:
                print(prio_dict)
            prio_dict.remove(mapped_tuple)
            sim = mapped_tuple.get_similarity()
            if (accepted_mapping.eq_values
                (mapped_tuple)):
                uncertain_mapping = 1
            if DEBUG_TERMS or mapped_tuple.term1 in debug_set or mapped_tuple.term2 in debug_set:
                print(f"remove ({mapped_tuple.term1.name},{mapped_tuple.term2.name}) with sim {sim} from prio-dict")

        return uncertain_mapping

    def add_mappings_to_pq(self,new_mapping_tuples,
                           prio_dict, w_exp_sim, similarity_metric, expanded_record_tuples):
        exp_mappings = set()
        for term1, term2 in new_mapping_tuples:
            if not term1.is_active():
                print(f"term1 was mapped already: {term1.name} to {term2.name}")
                continue

            if not term2.is_active():
                print(f"term2 was mapped already: {term1.name} to {term2.name}")
                continue

            new_mapping = Mapping(term1, term2, expanded_record_tuples, similarity_metric)

            sim = new_mapping.compute_similarity()
            exp_mappings.add((term1, term2))
            if sim > -0.5:
                # Insert mapping into priority_queue
                if DEBUG_TERMS or term1 in debug_set or term2 in debug_set:
                    print(f"expanded tuple: {new_mapping.term1.name},{new_mapping.term2.name}, sim: {sim}")
                prio_dict.add(new_mapping)
                w_exp_sim.append(sim)


            else:
                new_mapping.gen_active = False
        return exp_mappings

    def plot_statistics(self,metric_name, w_prio_len, w_exp_sim, accepted_sim):
        fig, ax = plt.subplots(4, 1)
        fig.suptitle("iterativeExpansion + " + metric_name)
        ax[0].scatter(range(len(w_prio_len)), w_prio_len, s=1, label='Queue Size')
        ax[0].set_title("Queue Size")
        ax[1].scatter(range(len(w_exp_sim)), np.array(w_exp_sim), s=1, label='Expanded Similarities')
        ax[1].set_title("Computed Similarities")
        ax[2].scatter(range(len(accepted_sim)), np.array(accepted_sim), s=0.5, label='Accepted Similarities')
        ax[2].set_title("Mapped Similarities")
        ax[3].hist(accepted_sim, 100, label='Accepted MappingContainer Distribution')
        ax[3].set_title("Distribution of Similarities")
        fig.tight_layout()
        plt.show()

