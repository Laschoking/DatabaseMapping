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
import pandas as pd


if __name__ == "__main__":
    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"structural-evaluation\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')
    new_mappings_df = pd.DataFrame()
    existing_mappings_df = sql_con.get_table('MappingSetup')
    #existing_mappings_df.set_index("mapping_id", inplace=True)

    result_df = pd.DataFrame(columns=["mapping_id", "db_config_id", "unique_records_db1", "unique_records_db2",
                                      "common_records","overlap_perc", "synthetic_terms", "hub_computations",
                                      "uncertain_mappings", "computed_mappings", "max_tuples", "runtime"])


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

            new_mapping = pd.Series(mapping.get_finger_print())

            # If mapping_setup is in the DB already, use the existing Mapping_Identifier
            matches = existing_mappings_df[['expansion','anchor_quantile','importance_weight','dynamic','metric']].eq(new_mapping).all(axis=1)
            if matches.any():
                # The index which has the match is exactly the mapping_id we are looking for
                mapping_id = matches.idxmax()
            else:
                # Add new entry for mapping_df
                mapping_id = len(existing_mappings_df)
                new_mapping['mapping_id'] = mapping_id
                if not existing_mappings_df.empty:
                    new_mappings_df = pd.concat([existing_mappings_df, new_mapping.to_frame().T], ignore_index=True)
                else:
                    # Copy first series as DataFrame
                    new_mappings_df = pd.DataFrame(new_mapping).T



            # Save quality results for the insertion into the StructuralEvaluation table
            str_res = count_overlap_merge_db(mapping.db_merged_facts)
            str_res |= mapping.get_result_finger_print()
            str_res |= {"mapping_id" : mapping_id, "db_config_id" : db_identifier, "max_tuples" : c_max_tuples, "runtime": mapping_rt}

            new_result = pd.Series(str_res)
            if not result_df.empty:
                result_df = pd.concat([result_df, new_result.to_frame().T], ignore_index=False)
            else:
                result_df = pd.DataFrame(new_result).T

            print(f"expanded anchor nodes: {len(mapping.anchor_nodes[0]), len(mapping.anchor_nodes[1])}")
            print(f"accepted mappings: {mapping.c_accepted_anchor_mappings}")
            wrong_mappings = []
            # Output the wrongly mapped pairs:
            for index, rec in mapping.final_mapping.iterrows():
                if rec.iat[0] != rec.iat[1]:
                    wrong_mappings.append(list(rec[:-1]))

            # print(wrong_mappings)
            break

        # Evaluation function to analyse if the mapping reduces storage
        print(result_df.dtypes)
        if not new_mappings_df.empty:
            sql_con.insert_records("MappingSetup", new_mappings_df, write_index=False)
        sql_con.insert_records("StructuralResults", result_df.astype(str), write_index=False)

