import datetime
import time
import git
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.Classes.SimOutlier import SimOutlier, QuantileOutlier
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.StructuralSimilarityMetrics.DynamicRecordTupleCount import DynamicRecordTupleCount
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.StructuralSimilarityMetrics.NodeDegree import NodeDegree

from src.Libraries import ShellLib

import pandas as pd
import sqlite3

# TODO adjust mapping_id

if __name__ == "__main__":
    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"structural-evaluation\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    #mapping_df = pd.DataFrame(columns=["mapping_id", "expansion", "anchor_quantile", "metric", "importance_weight",
    #"commit_SHA"])
    #mapping_df.set_index("mapping_id", inplace=True)

    mapping_df = sql_con.get_table('MappingSetup')
    mapping_df.set_index("mapping_id", inplace=True)

    res_df = pd.DataFrame(
        columns=["mapping_id", "db_config_id", "unique_records_db1", "unique_records_db2", "common_records",
                 "overlap_perc", "synthetic_terms", "hub_computations",
                 "uncertain_mappings", "computed_mappings", "max_tuples", "runtime"])

    # Detect current commit for logging
    repo = git.Repo(search_parent_directories=True)
    commit = repo.head.object.hexsha

    for db_identifier, db_pair in db_config_df.iterrows():
        print(f"file: {db_identifier}")
        if db_identifier != "Simple_Pointer_v1_v1_copy":
            continue

        db_config = DbConfig(*db_pair)
        data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)
        # data.add_mapping(MappingContainer(data.paths, exp_strategy, node_metric)))

        # compute & evaluate equality base line
        db1_facts = data.db1_original_facts
        db2_facts = data.db2_original_facts

        # load facts into data-object
        db1_facts.read_db_relations()
        db2_facts.read_db_relations()

        # add mappings to data
        # Set Anchor Quantile
        q_0 = QuantileAnchorTerms(0)

        # Set SimOutlier
        sim_outlier = QuantileOutlier()

        # Set Expansion Strategy
        stat_cross_product = IterativeAnchorExpansion(q_0, sim_outlier, DYNAMIC=False)
        dyn_cross_product = IterativeAnchorExpansion(q_0, sim_outlier, DYNAMIC=True)

        # Set up Similarity Metrics
        jaccard_index = JaccardIndex(metric_weight=0.2)
        dynamic_edge_count = DynamicRecordTupleCount(metric_weight=0.2)
        node_degree = NodeDegree(metric_weight=0.2)

        # Add combinations as new Mapping Container
        data.add_mapping(MappingContainer(data.paths, stat_cross_product, node_degree))
        # data.add_mapping(MappingContainer(data.paths, stat_cross_product, jaccard_index))
        # data.add_mapping(MappingContainer(data.paths, stat_cross_product, dynamic_edge_count))

        # data.add_mapping(MappingContainer(data.paths, dyn_cross_product, node_degree))
        # data.add_mapping(MappingContainer(data.paths, dyn_cross_product, jaccard_index))
        # data.add_mapping(MappingContainer(data.paths, dyn_cross_product, dynamic_edge_count))

        # iterate through all selected mapping functions
        for mapping in data.mappings:
            print("--------------------------")
            print(mapping.name)
            mapping.init_records_terms_db1(data.db1_original_facts)
            mapping.init_records_terms_db2(data.db2_original_facts)
            c_max_tuples = len(mapping.terms_db1) * len(mapping.terms_db2)

            # calculate similarity_matrix & compute maximal mapping from db1_facts to db2_facts
            t0 = time.time()
            mapping.compute_mapping(db1_facts, db2_facts, [])
            t1 = time.time()
            mapping.db1_renamed_facts.log_db_relations()
            mapping_rt = round(t1 - t0, 4)

            nr_1_1_mappings = len(mapping.final_mapping)
            # execute best mapping and create merged database: merge(map(db1_facts), db2_facts) -> merge_db2
            mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)

            mapping.log_mapping()
            mapping.db_merged_facts.log_db_relations()

            mapping_setup = mapping.get_finger_print()
            mapping_setup |= {"commit_SHA" : commit,'mapping_id' : 1}
            #new_mapping = pd.DataFrame({k: [v] for k, v in mapping_setup.items()})
            new_mapping = pd.Series(mapping_setup)
            # If mapping_setup is not in the DB already, mark it for insertion (with next Id number)
            if mapping_df.eq(new_mapping).all(axis=1).any():
                print("xx")

            new_mapping.set_index("mapping_id", inplace=True)

            mapping_df = pd.concat([mapping_df,new_mapping],ignore_index=False)


            # Insert mapping_setup into df, if it does not exist yet

            # Insert quality results into the evaluation table
            qual_res = count_overlap_merge_db(mapping.db_merged_facts)
            qual_res |= mapping.get_result_finger_print()
            qual_res |= {"mapping_id" : 1, "db_config_id" : db_identifier, "max_tuples" : c_max_tuples, "runtime": mapping_rt}

            new_rec = pd.DataFrame({k: [v] for k, v in qual_res.items()})
            #new_rec.set_index("mapping_id", inplace=True)

            res_df = pd.concat([res_df, new_rec], ignore_index=False)

            l_blocked_terms = 0


            print(f"expanded anchor nodes: {len(mapping.anchor_nodes[0]), len(mapping.anchor_nodes[1])}")
            print(f"accepted mappings: {mapping.c_accepted_anchor_mappings}")
            wrong_mappings = []
            # Output the wrongly mapped pairs:
            for index, rec in mapping.final_mapping.iterrows():
                if rec.iat[0] != rec.iat[1]:
                    wrong_mappings.append(list(rec[:-1]))

            # print(wrong_mappings)

        # Evaluation function to analyse if the mapping reduces storage
        sql_con.insert_records("MappingSetup", mapping_df,write_index=True)
        sql_con.insert_records("StructuralResults", res_df,write_index=False)

