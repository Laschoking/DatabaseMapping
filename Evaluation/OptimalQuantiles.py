import datetime
from sortedcontainers import SortedList
import time

from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.Classes.SimOutlier import SimOutlier,QuantileOutlier
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.StructuralSimilarityMetrics.DynamicRecordTupleCount import DynamicRecordTupleCount
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.StructuralSimilarityMetrics.NodeDegree import NodeDegree
from src.LexicalSimilarityMetrics.ISUB import IsubStringMatcher
from src.LexicalSimilarityMetrics.LevenshteinSimilarity import LevenshteinSimilarity
from src.LexicalSimilarityMetrics.JaroWinkler import JaroWinkler
from src.Libraries import ShellLib

if __name__ == "__main__":

    GEN_FACTS = False  # if true, run doop again for new fact-gen, otherwise just copy from doop/out
    COMP_MAPPING = True

    # Set Anchor Quantile
    q_0 = QuantileAnchorTerms(0)

    # Set SimOutlier
    sim_outlier = QuantileOutlier()

    # Set Expansion Strategy
    static_crossproduct = IterativeAnchorExpansion(q_0, sim_outlier, DYNAMIC=False)

    # Set up Similarity Metrics
    jaccard_index = JaccardIndex()

    dbs = [Doop_Simple_Pointer,Doop_Gocd_Websocket_Notifier_v1_v4]
    for db_config in dbs:
        data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)


        # load facts into data-object
        data.db1_original_facts.read_db_relations()
        data.db2_original_facts.read_db_relations()


        db1_facts = data.db1_original_facts
        db2_facts = data.db2_original_facts

        data.add_mapping(MappingContainer(data.paths, static_crossproduct, jaccard_index))

        eval_tab = PrettyTable()
        eval_tab.field_names = ["Method", "data set", "unique rows DB1", "unique rows DB2", "Common Rows",
                                "overlap in %"]
        eval_tab.add_row(
            ["No mapping", "original facts"] + compute_overlap_dbs(data.db1_original_facts, data.db2_original_facts,
                                                                   print_flag=False))

        time_tab = PrettyTable()
        time_tab.field_names = ["MappingContainer", "#blocked Mappings", "# 1:1 Mappings", "#synthetic Terms", "# hub comp.",
                                "uncertain mappings", "# comp. tuples", "comp. tuples in %", "run-time"]

        # iterate through all selected mapping functions
        for mapping in data.mappings:
            print("--------------------------")
            print(mapping.name)
            mapping.init_records_terms_db1(data.db1_original_facts)
            mapping.init_records_terms_db2(data.db2_original_facts)
            c_max_tuples = len(mapping.terms_db1) * len(mapping.terms_db2)

            # calculate similarity_matrix & compute maximal mapping from db1_facts to db2_facts
            if COMP_MAPPING:
                t0 = time.time()
                mapping.compute_mapping(db1_facts,db2_facts, 0)
                t1 = time.time()
                mapping.db1_renamed_facts.log_db_relations()
                mapping_rt = round(t1 - t0, 4)
            else:
                mapping.read_mapping()
                mapping_rt = 0.0
            t1 = []
            for term in mapping.terms_db1.values():
                t1.append((term.degree,term.name))
            t1.sort(key=lambda tup: tup[0], reverse=True)  #sort by degree

            t2 = []
            for term in mapping.terms_db2.values():
                t2.append((term.degree,term.name))
            t2.sort(key=lambda tup: tup[0], reverse=True)  #sort by degree
            # TODO other values could have same value still so order is not 100%

            best_mappings = mapping.final_mapping.nlargest(5, 2) # find best similarities
            for index,(term1,term2,sim) in best_mappings.iterrows():
                deg1 = mapping.terms_db1[term1].degree
                ind1 = t1.index((deg1,term1))
                deg2 = mapping.terms_db2[term2].degree
                ind2 = t2.index((deg2,term2))
                print(f"{ind1,ind2}")
                print(f"corresponds to {1 - ind1/ len(t1)} for q1")
                print(f"corresponds to {1 - ind2 / len(t2)} for q2")
            #print(best_mappings)


            nr_1_1_mappings = len(mapping.final_mapping)
            # execute best mapping and create merged database: merge(map(db1_facts), db2_facts) -> merge_db2
            mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)

            res = count_overlap_merge_db(mapping.db_merged_facts)
            if mapping == data.mappings[-1]:
                eval_tab.add_row([mapping.name, "merged facts"] + res, divider=True)
            else:
                eval_tab.add_row([mapping.name, "merged facts"] + res, divider=False)


            time_tab.add_row(
                [mapping.name, 0, nr_1_1_mappings, mapping.syn_counter, mapping.c_hub_recomp,
                 mapping.c_uncertain_mappings, mapping.c_mappings,
                 str(round(mapping.c_mappings * 100 / c_max_tuples, 2)) + "%", mapping_rt])
            if COMP_MAPPING:
                print(f"nr of terms: {len(mapping.terms_db1),len(mapping.terms_db2)}")
                print(f"expanded anchor nodes: {len(mapping.anchor_nodes[0]),len(mapping.anchor_nodes[1])}")
                print(f"accepted mappings: {mapping.c_accepted_anchor_mappings}")


        # Evaluation function to analyse if the mapping reduces storage
        #print(time_tab)

        #print(eval_tab)

        # data.log_terms()