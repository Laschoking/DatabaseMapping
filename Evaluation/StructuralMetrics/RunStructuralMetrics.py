import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.StructuralSimilarityMetrics.DynamicRecordTupleCount import DynamicRecordTupleCount
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.StructuralSimilarityMetrics.NodeDegree import NodeDegree
import itertools
import gc
import pandas as pd



if __name__ == "__main__":

    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"structural-evaluation\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping dataframes
    existing_mappings_df = sql_con.get_table('MappingSetup')

    existing_result_df = sql_con.get_table('StructuralResults_New2')

    #########################################################
    # Important parameters:
    # Set this higher if all results should be computed several times (since they are non-deterministic)
    RUN_NR = [1,2,3] # 3,4,5
    # TODO: 1 more run

    # Set Anchor Quantile to 0, so the cartesian product is expanded (all possible combinations)
    q_0 = QuantileAnchorTerms(0)

    # Set Expansion Strategies (since
    stat_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=False)
    dyn_cross_product = IterativeAnchorExpansion(anchor_quantile=q_0, DYNAMIC=True)
    expansions = [stat_cross_product,dyn_cross_product]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    metric_weights = [0.8,0.9,1]
    metrics = [JaccardIndex(metric_weight=w) for w in metric_weights]
    metrics += [DynamicRecordTupleCount(metric_weight=w) for w in metric_weights]
    metrics += [NodeDegree(metric_weight=w) for w in metric_weights]


    ############################################################

    # Iterate through all relevant database pairs that are used for the structural-evaluation
    for curr_db_config_id, db_pair in db_config_df.iterrows():
        print(f"file: {curr_db_config_id}")
        db_config = DbConfig(*db_pair[['use','type','file_name','db1','db2']])
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
            new_mapping['str_ratio'] = 1


            # Check if the computation can be skipped, because the keys (mapping_id, db_config_id) are already in database
            if MAPPING_EXISTS:
                # Make series empty, so it will not be inserted to db later
                new_mapping = new_mapping[0:0]
                left_runs = skip_current_computation(mapping_id=curr_mapping_id, db_config_id=curr_db_config_id,
                                                     df=existing_result_df, run_nr=RUN_NR)
                if not left_runs:
                    continue
            else:
                # Otherwise, add the new mapping setup to local res_df and database
                left_runs = RUN_NR
                existing_mappings_df = add_series_to_df(series=new_mapping, df=existing_mappings_df)

            print("--------------------------")
            print(mapping.name)
            for run in left_runs:
                # To be save, that no data problems arise, we create a new mapping object for each iteration
                print(f"Run number: {run}")
                temp_mapping_obj = MappingContainer(data.paths,mapping.expansion_strategy,mapping.similarity_metric,
                                                    mapping_id=curr_mapping_id,run_nr=run)

                temp_mapping_obj.init_records_terms_db1(data.db1_original_facts)
                temp_mapping_obj.init_records_terms_db2(data.db2_original_facts)
                c_max_tuples = len(temp_mapping_obj.terms_db1) * len(temp_mapping_obj.terms_db2)


                t0 = time.time()

                # Find the best mapping for the selected parameters (expansion, metric, metric_weight)
                # Apply the mapping to populate db1_renamed_facts
                temp_mapping_obj.compute_mapping(db1_facts, db2_facts, [])

                t1 = time.time()
                mapping_rt = round(t1 - t0, 4)

                # Merge db1_renamed_facts and db2_facts into db_merged_facts
                temp_mapping_obj.merge_dbs(temp_mapping_obj.db1_renamed_facts, db2_facts, temp_mapping_obj.db_merged_facts)


                # Save quality results for the insertion into the StructuralEvaluation table
                str_res = count_overlap_merge_db(temp_mapping_obj.db_merged_facts)
                str_res |= temp_mapping_obj.get_result_finger_print()
                str_res |= {"mapping_id": curr_mapping_id, "db_config_id": curr_db_config_id, "max_tuples": c_max_tuples,
                            "runtime": mapping_rt,'run_nr' : run}

                new_result = pd.Series(str_res)

                # Log computed mapping and renamed DB1 and merged DB
                # We only log the last run
                temp_mapping_obj.log_mapping()
                temp_mapping_obj.db1_renamed_facts.log_db_relations(mapping_id=curr_mapping_id, run_nr=run)
                temp_mapping_obj.db_merged_facts.log_db_relations(mapping_id=curr_mapping_id, run_nr=run)

                # The casting of new_result_df to str is necessary because sqlite sometimes inserts BLOB for other data types
                if not new_result.empty:
                    sql_con.insert_series("StructuralResults_New2", series=new_result.astype(str), write_index=False)

                gc.collect()

            # Evaluation function to analyse if the mapping reduces storage
            if not new_mapping.empty:
                sql_con.insert_series("MappingSetup", series=new_mapping, write_index=False)




