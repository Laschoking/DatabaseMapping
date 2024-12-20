import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import OriginalFactsContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Config_Files.Analysis_Configs import *
from src.Libraries.EvaluateMappings import *
import itertools
import gc
import pandas as pd

def run_mappings_on_dbs(db_config_use,res_table,expansions,metrics,nr_runs):

    # Retrieve relevant facts from Database
    query = f"SELECT * FROM  DbConfig WHERE Use LIKE \"{db_config_use}%\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping_func dataframes
    existing_mappings_df = sql_con.get_table('MappingSetup')

    existing_result_df = sql_con.get_table(res_table)

    # Iterate through all relevant database pairs that are used for the evaluation
    for curr_db_config_id, db_pair in db_config_df.iterrows():
        db_config = DbConfig(*db_pair[['use', 'type', 'file_name', 'db1', 'db2']])
        data = OriginalFactsContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)


        # Load facts into the facts structure
        db1_facts = data.db1_original_facts.read_db_relations()
        db2_facts = data.db2_original_facts.read_db_relations()

        for expansion, metric in itertools.product(expansions, metrics):
            new_mapping = pd.Series({"expansion": expansion.name, "dynamic": str(expansion.DYNAMIC),
                           "anchor_quantile": expansion.anchor_quantile.initial_q, "metric": metric.name,
                           "importance_parameter": metric.imp_alpha, "struct_ratio": metric.struct_ratio, 'sim_th' : expansion.sim_th})

            curr_mapping_id, MAPPING_EXISTS = get_mapping_id(new_mapping, existing_mappings_df)
            new_mapping['mapping_id'] = curr_mapping_id

            # Check if the computation can be skipped, because the keys (mapping_id, db_config_id) are already in database
            if MAPPING_EXISTS:
                # Make series empty, so it will not be inserted to db later
                new_mapping = new_mapping[0:0]
                left_runs = skip_current_computation(mapping_id=curr_mapping_id, db_config_id=curr_db_config_id,
                                                     df=existing_result_df, run_nr=nr_runs)
                if not left_runs:
                    continue
            else:
                # Otherwise, add the new mapping_func setup to local res_df_w_alpha and database
                left_runs = nr_runs
                existing_mappings_df = add_series_to_df(series=new_mapping, df=existing_mappings_df)

            print("--------------------------")
            print(f"file: {curr_db_config_id}")
            print(f" mapping_id: {curr_mapping_id}")

            for run_nr in left_runs:
                # To be safe, that no facts problems arise, we create a new mapping_func object for each run
                print(f"Run number: {run_nr}")

                # Add combinations as new Mapping Container
                mapping = MappingContainer(data.paths, expansion, metric,mapping_id=curr_mapping_id,run_nr=run_nr)
                #facts.add_mapping(mapping_func)
                mapping.init_facts_elements_db1(data.db1_original_facts)
                mapping.init_facts_elements_db2(data.db2_original_facts)
                c_max_tuples = len(mapping.elements_db1) * len(mapping.elements_db2)


                t0 = time.time()

                # Find the best mapping_func for the selected parameters (expansion, metric, imp_alpha)
                # Apply the mapping_func to populate db1_renamed_facts
                mapping.compute_mapping(db1_facts, db2_facts, [])

                t1 = time.time()
                mapping_rt = round(t1 - t0, 4)

                # Merge db1_renamed_facts and db2_facts into db_merged_facts
                mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)


                # Save quality results for the insertion into the StructuralEvaluation table
                str_res = count_overlap_merge_db(mapping.db_merged_facts)
                str_res |= mapping.get_result_finger_print()
                str_res |= {"mapping_id": curr_mapping_id, "db_config_id": curr_db_config_id, "max_tuples": c_max_tuples,
                            "runtime": mapping_rt,'run_nr' : run_nr}

                new_result = pd.Series(str_res)

                # Log computed mapping_func and renamed DB1 and merged DB
                mapping.log_mapping()
                mapping.db1_renamed_facts.log_db_relations()
                mapping.db_merged_facts.log_db_relations()

                # The casting of new_result_df to str is necessary because sqlite sometimes inserts BLOB for other facts types
                if not new_result.empty:
                    sql_con.insert_series(res_table, series=new_result.astype(str))


                # Evaluation function to analyse if the mapping_func reduces storage
                if not new_mapping.empty:
                    sql_con.insert_series("MappingSetup", series=new_mapping)

                del mapping
                gc.collect()

