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
import itertools
import gc
import pandas as pd


# TODO insert data row by row and catch errors better

def get_mapping_id(new_mapping, existing_mappings_df) -> (int, bool):
    # If mapping_setup is in the DB already, use the existing Mapping_Identifier
    matches = existing_mappings_df[
        ['expansion', 'anchor_quantile', 'importance_weight', 'dynamic', 'metric']].eq(new_mapping).all(axis=1)
    if matches.any():
        # The index which has the match is exactly the mapping_id we are looking for
        curr_mapping_id = existing_mappings_df.loc[matches.idxmax(), 'mapping_id']
        return curr_mapping_id, True

    else:
        # Add new entry for mapping_df
        curr_mapping_id = len(existing_mappings_df)
        return curr_mapping_id, False


def skip_current_computation(mapping_id,db_config_id, df) -> bool:
    result_key_df = df[['mapping_id', 'db_config_id']]
    current_keys = pd.Series({'mapping_id': mapping_id, 'db_config_id': db_config_id})
    matches = result_key_df.eq(current_keys)
    if matches.all(axis=1).any():
        return True
    else:
        False





if __name__ == "__main__":

    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"structural-evaluation\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping dataframes
    new_mappings_df = pd.DataFrame()
    existing_mappings_df = sql_con.get_table('MappingSetup')

    # Setup result dataframes
    new_result_df = pd.DataFrame(columns=["mapping_id", "db_config_id", "unique_records_db1", "unique_records_db2",
                                      "common_records", "overlap_perc", "synthetic_terms", "hub_computations",
                                      "uncertain_mappings", "computed_mappings", "max_tuples", "runtime"])
    existing_result_df = sql_con.get_table('StructuralResults')

    #########################################################
    # Important parameters:

    # Set Anchor Quantile to 0, so the cartesian product is expanded (all possible combinations)
    q_0 = QuantileAnchorTerms(0)

    # Set Expansion Strategies (since
    stat_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=False)
    dyn_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=True)
    expansions = [stat_cross_product,dyn_cross_product]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    jaccard_index = JaccardIndex(metric_weight=1)
    dynamic_edge_count = DynamicRecordTupleCount(metric_weight=1)
    node_degree = NodeDegree(metric_weight=1)
    metrics = [node_degree, jaccard_index,dynamic_edge_count] # 3 combinations

    ############################################################

    # Iterate through all relevant data base pairs that are used for the structural-evaluation
    for curr_db_config_id, db_pair in db_config_df.iterrows():
        #if curr_db_config_id != "Simple_Pointer_v1_v1_copy":
        #   continue
        print(f"file: {curr_db_config_id}")
        db_config = DbConfig(*db_pair)
        data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)

        # Load facts into the data structure
        db1_facts = data.db1_original_facts.read_db_relations()
        db2_facts = data.db2_original_facts.read_db_relations()

        # Add combinations as new Mapping Container
        data.add_mappings([MappingContainer(data.paths, expansion, metric)
                           for expansion, metric in itertools.product(expansions, metrics)
                           ])


        # iterate through all selected mapping setups
        for mapping in data.mappings:

            # Put results of mapping run into dataframe for insertion into the database
            new_mapping = pd.Series(mapping.get_finger_print())

            curr_mapping_id,MAPPING_EXISTS = get_mapping_id(new_mapping, existing_mappings_df)
            new_mapping['mapping_id'] = curr_mapping_id

            # Check if the computation can be skipped, because the keys (mapping_id, db_config_id) are already in database
            if MAPPING_EXISTS:
                # Make series empty, so it will not be inserted to db later
                new_mapping = new_mapping[0:0]
                RES_EXISTS = skip_current_computation(mapping_id=curr_mapping_id,db_config_id=curr_db_config_id,df=existing_result_df)
                if RES_EXISTS:
                    continue
            else:
                # Otherwise, add the new mapping setup to local df and database
                existing_mappings_df = sql_con.add_series_to_df(series=new_mapping,df=existing_mappings_df)


            print("--------------------------")
            print(mapping.name)

            mapping.init_records_terms_db1(data.db1_original_facts)
            mapping.init_records_terms_db2(data.db2_original_facts)

            t0 = time.time()

            # Find the best mapping for the selected parameters (expansion, metric, metric_weight)
            # Apply the mapping to populate db1_renamed_facts
            mapping.compute_mapping(db1_facts, db2_facts, [])

            t1 = time.time()
            mapping_rt = round(t1 - t0, 4)


            # Merge db1_renamed_facts and db2_facts into db_merged_facts
            mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)

            # Log computed mapping and renamed DB1 and merged DB
            mapping.log_mapping()
            mapping.db1_renamed_facts.log_db_relations()
            mapping.db_merged_facts.log_db_relations()



            # Save quality results for the insertion into the StructuralEvaluation table
            c_max_tuples = len(mapping.terms_db1) * len(mapping.terms_db2)
            str_res = count_overlap_merge_db(mapping.db_merged_facts)
            str_res |= mapping.get_result_finger_print()
            str_res |= {"mapping_id": curr_mapping_id, "db_config_id": curr_db_config_id, "max_tuples": c_max_tuples,
                        "runtime": mapping_rt}

            new_result = pd.Series(str_res)


            # Evaluation function to analyse if the mapping reduces storage
            if not new_mapping.empty:
                sql_con.insert_series("MappingSetup", series=new_mapping, write_index=False)

            # The casting of new_result_df to str is necessary because sqlite sometimes inserts BLOB for other data types
            if not new_result.empty:
                sql_con.insert_df("StructuralResults", series=new_result.astype(str), write_index=False)

            break
        gc.collect()

