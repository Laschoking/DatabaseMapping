import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import OriginalFactsContainer,DlSeparateResultsContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.Libraries import ShellLib
from src.Classes.QuantileAnchorElements import QuantileAnchorElements
from src.StructuralSimilarityMetrics.FactSimilarity import FactSimilarity
from src.LexicalSimilarityMetrics.Dice import Dice
import itertools
import gc
import pandas as pd
import src.Libraries.PandasUtility as pd_util
from src.Libraries.EvaluateMappings import verify_merge_results,count_overlap_merge_db
from src.StructuralSimilarityMetrics.FactPairSimilarity import FactPairSimilarity
from src.Classes.SimilarityMetric import MixedSimilarityMetric
from src.LexicalSimilarityMetrics.Dice import Dice
from src.Config_Files.Setup import run_mappings_on_dbs

def run_separate_program_analyses(fact_container, result_container, dl_program):
    nemo_rt_df = pd.DataFrame()
    # Run the program analysis separated from each other: pa(db1), pa(db2) to generate base-stats
    nemo_rt_db1 = ShellLib.chase_nemo(dl_program.sep_dl, fact_container.db1_original_facts.path,
                                      result_container.db1_original_results.path)
    nemo_rt_df = pd_util.add_series_to_df(series=nemo_rt_db1, df=nemo_rt_df)

    nemo_rt_db2 = ShellLib.chase_nemo(dl_program.sep_dl, fact_container.db2_original_facts.path,
                                      result_container.db2_original_results.path)

    nemo_rt_df = pd_util.add_series_to_df(series=nemo_rt_db2, df=nemo_rt_df)

    result_container.db1_original_results.read_db_relations()
    result_container.db2_original_results.read_db_relations()
    return nemo_rt_df.sum(numeric_only=True)



def run_merged_program_analyses(mapping):
    # run Nemo-Rules on merged fact_container (merge_db2 )
    nemo_runtime = ShellLib.chase_nemo(mapping.dl_merged_program, mapping.db_merged_facts.path,
                                       mapping.db_merged_results.path)

    # Read PA-dl_sep_container
    mapping.db_merged_results.read_db_relations()

    # Apply mapping_func to merged-result (from db2_facts)
    # mapping_func.map_df(mapping_func.db_merged_results, mapping_func.db1_unravelled_results)
    mapping.unravel_merge_dbs()
    mapping.db1_unravelled_results.log_db_relations()
    mapping.db2_unravelled_results.log_db_relations()

    return nemo_runtime

if __name__ == "__main__":


    # Step 1: Retrieve more MVN-Libs
    # Step 2: Generate DBs: DOOP-Generation.py
    # Step 3: Run best Mapping function (mapping_id=92	Local	quantile=0.95	metric=FactPair-Sim_Dice	gamma=0.7)
    sim_th = [0.0,0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    fp_sim = FactPairSimilarity(imp_alpha=0)
    dice = Dice(n=2, imp_alpha=0)
    mixed_metric = MixedSimilarityMetric(struct_ratio=0.6, lex_metric=dice, imp_alpha=0, struct_metric=fp_sim)
    expansions = [IterativeAnchorExpansion(anchor_quantile=QuantileAnchorElements(0.95),DYNAMIC=True,sim_th=s) for s in sim_th]
    USE = "reasoning-evaluation"
    run_mappings_on_dbs(USE, 'FinalMappingResults', expansions, [mixed_metric],
                        nr_runs=[1])
    '''
    # Step 4: Evaluate the Datalog Programs

    # Retrieve relevant fact_container from Database Use=\"reasoning-evaluation\" OR U
    query = "SELECT * FROM  DbConfig WHERE Use LIKE \"reasoning-evaluation%\" ORDER BY file_name;"
    db_config_df = sql_con.query_table(query, ind_col='db_config_id')

    # Setup mapping_func dataframes
    mapping_df = sql_con.get_table('MappingSetup')
    merged_facts_df = sql_con.query_table("SELECT * FROM  FinalMappingResults WHERE mapping_id=91;")

    result_df = sql_con.get_table('DLResults')

    # Get mapping_func information
    merged_facts_df = pd.merge(merged_facts_df, mapping_df, on='mapping_id')

    #########################################################
    # Important parameters:
    dl_programs = [Doop_CFG,Doop_PointerAnalysis]
    #########################################################

    # Log Results:
    nemo_rt = pd.DataFrame(columns=['Analysis', 'db_config_id', 'total_rt', 'loading_rt', 'reasoning_rt', 'output_rt'])

    # Iterate through all relevant database pairs that are used for the datalog-evaluation

    for db_id, db_pair in db_config_df.iterrows():
        # Setup fact_container bases
        print(f"file: {db_id}")
        db_config = DbConfig(*db_pair[['use','type','file_name','db1','db2']])
        fact_container = OriginalFactsContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)

        # Load fact_container into the fact_container structure
        db1_facts = fact_container.db1_original_facts.read_db_relations()
        db2_facts = fact_container.db2_original_facts.read_db_relations()

        for dl_program in dl_programs:
            dl_sep_container = DlSeparateResultsContainer(db_config.base_output_path,  db_config.db1_path,
                                                          db_config.db2_path,dl_name=dl_program.name)

            sep_results_fp = pd.Series({'db_config_id' : db_id,'mapping_id' : 'separate','dl_program' : dl_program.name})

            # Run the single Dl analysis on Db1 and Db2 if results are not in DB
            if not is_series_in_df(series=sep_results_fp,df=result_df):
                print(f"Run datalog program separate: {dl_program.name}")

                nemo_rt = run_separate_program_analyses(fact_container, dl_sep_container, dl_program)
                sep_results_overlap = compute_overlap_dbs(dl_sep_container.db1_original_results,dl_sep_container.db2_original_results)
                sep_results = pd.concat([sep_results_fp,sep_results_overlap,nemo_rt])
                result_df = add_series_to_df(series=sep_results,df=result_df)
                sql_con.insert_series(table='DLResults', series=sep_results)

            
            else:
                dl_sep_container.db1_original_results.read_db_relations()
                dl_sep_container.db2_original_results.read_db_relations()
            
            
            # Retrieve all mapping_func configuration that were used in the ReasoningDB
            mapping_results = merged_facts_df[merged_facts_df['db_config_id'] == db_id]

            for index,mapping_res in mapping_results.iterrows():


                m_results_fp = pd.Series(
                    {'db_config_id': db_id, 'mapping_id': mapping_res['mapping_id'], 'dl_program': dl_program.name,'run':mapping_res['run_nr']})

                # If result was already computed skip the DL-run
                if is_series_in_df(series=m_results_fp, df=result_df):
                    continue

                print(f"mapping_id: {mapping_res['mapping_id']}")
                print(f"Run datalog program merged: {dl_program.name}")
                mapping = MappingContainer(fact_paths=fact_container.paths, expansion_strategy=None,
                                           similarity_metric=None,mapping_id=mapping_res['mapping_id'],
                                           run_nr=mapping_res['run_nr'], dl_program=dl_program
                                           )


                # Read mapping_func dl_sep_container, that were already computed before
                # This is used for the inverse operation (during unravelling)
                mapping.read_mapping()

                # Execute the common analysis on the merged database
                nemo_rt = run_merged_program_analyses(mapping)

                # check if bijected dl_sep_container correspond to correct dl_sep_container from base
                verify_merge_results(dl_sep_container, mapping)
                merge_overlap = pd.Series(count_overlap_merge_db(mapping.db_merged_results))
                merge_overlap = pd.concat([pd.Series({'db_config_id': db_id,'mapping_id' : mapping_res['mapping_id'],
                                                      'run' : mapping_res['run_nr'],'dl_program' : dl_program.name}), merge_overlap, nemo_rt])
                result_df = add_series_to_df(series=merge_overlap,df=result_df)
                sql_con.insert_series(table='DLResults', series=merge_overlap)
'''