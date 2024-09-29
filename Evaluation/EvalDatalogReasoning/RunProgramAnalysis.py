import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.Libraries import ShellLib
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.LexicalSimilarityMetrics.Dice import Dice
import itertools
import gc
import pandas as pd
import src.Libraries.PandasUtility as pd_util
from src.Libraries.EvaluateMappings import verify_merge_results,count_overlap_merge_db

def run_separate_program_analyses(data, dl_programs, nemo_rt_df):
    for dl_program in dl_programs:
        # Run the program analysis separated from each other: pa(db1), pa(db2) to generate base-stats
        nemo_rt_db1 = ShellLib.chase_nemo(dl_program.sep_dl, data.db1_original_facts.path,
                                          data.db1_original_results.path)
        pd_util.add_series_to_df(series=nemo_rt_db1, df=nemo_rt_df)

        nemo_rt_db2 = ShellLib.chase_nemo(dl_program.sep_dl, data.db2_original_facts.path,
                                          data.db2_original_results.path)

        pd_util.add_series_to_df(series=nemo_rt_db2, df=nemo_rt_df)

        data.db1_original_results.read_db_relations()
        data.db2_original_results.read_db_relations()
    return nemo_rt_df



def run_merged_program_analyses(mapping, dl_programs, nemo_rt_df):
    for dl_program in dl_programs:
        # run Nemo-Rules on merged facts (merge_db2 )
        nemo_runtime = ShellLib.chase_nemo(dl_program.merge_dl, mapping.db_merged_facts.path,
                                           mapping.db_merged_results.path)

        # Read PA-results
        mapping.db_merged_results.read_db_relations()

        # Apply mapping to merged-result (from db2_facts)
        # mapping.map_df(mapping.db_merged_results, mapping.db1_unravelled_results)
        mapping.unravel_merge_dbs()
        mapping.db1_unravelled_results.log_db_relations()
        mapping.db2_unravelled_results.log_db_relations()

    return nemo_runtime

if __name__ == "__main__":

    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"expansion-same-lib\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping dataframes
    mapping_df = sql_con.get_table('MappingSetup')
    existing_result_df = sql_con.get_table('ExpansionResults')


    # Get mapping information
    existing_result_df = pd.merge(existing_result_df, mapping_df,on='mapping_id')

    #########################################################
    # Important parameters:
    dl_programs = [Doop_CFG,Doop_PointerAnalysis]
    ############################################################


    # Log Results:
    nemo_rt_df = pd.DataFrame(columns=['Analysis','db_config_id','total_rt','loading_rt','reasoning_rt','output_rt'])

    # Iterate through all relevant database pairs that are used for the datalog-evaluation

    for curr_db_config_id, db_pair in db_config_df.iterrows():
        # Setup data bases
        print(f"file: {curr_db_config_id}")
        db_config = DbConfig(*db_pair[['use','type','file_name','db1','db2']])
        data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)

        # Load facts into the data structure
        db1_facts = data.db1_original_facts.read_db_relations()
        db2_facts = data.db2_original_facts.read_db_relations()

        run_separate_program_analyses(data, dl_programs, nemo_rt_df)

        # Retrieve all mapping configuration that were used in the ReasoningDB
        curr_mappings = existing_result_df[existing_result_df['db_config_id'] == curr_db_config_id]

        expansions = {()}
        for index,mapping_ser in curr_mappings.iterrows():
            expansion = IterativeAnchorExpansion(anchor_quantile=mapping_ser['anchor_quantile'],
                                                 DYNAMIC=bool(mapping_ser['dynamic']))
            if mapping_ser['metric'] == 'Dice':
                metric = Dice(metric_weight=mapping_ser['importance_weight'])
            elif mapping_ser['metric'] == 'Jaccard':
                metric = JaccardIndex(metric_weight=mapping_ser['importance_weight'])
            else:
                raise ValueError(f"metric not known: {mapping_ser['metric']}")

            mapping = MappingContainer(paths=data.paths, expansion_strategy=expansion, similarity_metric=metric)
            # Read mapping results, that were already computed before
            mapping.read_mapping(run_nr=mapping_ser['run_nr'])

            # Merge db1_renamed_facts and db2_facts into db_merged_facts
            mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)

            # Execute the common analysis on the merged database
            run_merged_program_analyses(mapping, dl_programs,nemo_rt_df)



            # check if bijected results correspond to correct results from base
            verify_merge_results(data, mapping)
            overlap = count_overlap_merge_db(mapping.db_merged_results)
    '''
            # noinspection PyUnboundLocalVariable
            reasoning_res.append([mapping.name, "merged results"] + overlap)

            # The casting of new_result_df to str is necessary because sqlite sometimes inserts BLOB for other data types
            if not new_result.empty:
                sql_con.insert_series("ExpansionResults", series=new_result.astype(str), write_index=False)

        gc.collect()

            break

        # Evaluation function to analyse if the mapping reduces storage
        if not new_mapping.empty:
            sql_con.insert_series("MappingSetup", series=new_mapping, write_index=False)

        break


def generate_pa_dbs():

    # Retrieve relevant data from Database
    query = "SELECT * FROM  DbConfig WHERE Use=\"datalog-evaluation\";"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping dataframes
    existing_mappings_df = sql_con.get_table('MappingSetup')
    existing_result_df = sql_con.get_table('ReasoningResults')

    #########################################################
    # Important parameters:
    dl_programs = [Doop_CFG,Doop_PointerAnalysis]

    RUN_NR = [1]

    # Setup best Anchor value:
    q = QuantileAnchorTerms(0.90)

    # Set Expansion Strategies
    expansions = [IterativeAnchorExpansion(anchor_quantile=q, DYNAMIC=False),
                  IterativeAnchorExpansion(anchor_quantile=q, DYNAMIC=True)]

    # Since we want to evaluate the quality of each structural metric (without any expansion) we only evaluate the str. metrics
    # The best metric weight will be chosen in the evaluation of Expansion Strategy
    # Set up Structural similarity metrics
    w = 0.8
    metrics = [JaccardIndex(metric_weight=w), Dice(n=2, metric_weight=w)]

    ############################################################
    # Log Results:
    nemo_rt_df = pd.DataFrame(columns=['Analysis','db_config_id','total_rt','loading_rt','reasoning_rt','output_rt'])

    # Iterate through all relevant database pairs that are used for the datalog-evaluation

    for curr_db_config_id, db_pair in db_config_df.iterrows():
        # Setup data bases
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

            # Read mapping results, that were already computed before
            mapping.read_mapping()

            # Merge db1_renamed_facts and db2_facts into db_merged_facts
            mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)



            # check if bijected results correspond to correct results from base
            verify_merge_results(data, mapping)
            overlap = count_overlap_merge_db(mapping.db_merged_results)
            # noinspection PyUnboundLocalVariable
            reasoning_res.append([mapping.name, "merged results"] + overlap)

            # The casting of new_result_df to str is necessary because sqlite sometimes inserts BLOB for other data types
            if not new_result.empty:
                sql_con.insert_series("ExpansionResults", series=new_result.astype(str), write_index=False)

            gc.collect()

            break

        # Evaluation function to analyse if the mapping reduces storage
        if not new_mapping.empty:
            sql_con.insert_series("MappingSetup", series=new_mapping, write_index=False)

        break


    '''
