from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np


# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

if __name__ == "__main__":
    PLOT_FIG = True
    NR_RUNS = 1
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use LIKE \'expansion-same-lib\';")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.get_table(table="ExpansionResults")
    mapping_df = sql_con.get_table(table="MappingSetup")
    res_df = pd.merge(res_df,mapping_df, left_on='mapping_id', right_on='mapping_id')
    print(db_config_df.columns)
    print(single_db_char_df.columns)

    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db1'],
                            right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db1','version' : 'version_db1','nr_constants' : 'nr_constants_db1'},inplace=True)
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db2'],
                            right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db2','version' : 'version_db2','nr_constants' : 'nr_constants_db2'},inplace=True)
    db_config_df.drop(columns=['version_db1','version_db2'],inplace=True)

    db_config_df['nr_poss_facts'] = db_config_df[['nr_facts_db1', 'nr_facts_db2']].min(axis=1)

    res_df = pd.merge(res_df,db_config_df, left_on='db_config_id', right_on='db_config_id')
    # This df holds characteristics of each database
    nr_total_facts = sum(db_config_df['nr_poss_facts'])
    nr_equal_facts = sum(db_config_df['nr_equal_facts'])
    #nr_total_constants = sum(db_config_df['nr_constants'])

    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_records
    #plot_overlap_per_resource(res_df, PLOT_FIG)
    #plot_best_overlap_per_resource(res_df,PLOT_FIG)
    overlap_df = calc_overlap_perc_all_resources(res_df)
    overlap_df['overlap_perc'] = overlap_df['common_records'] * 100 / nr_total_facts
    overlap_df['nr_equal_facts'] = nr_equal_facts
    plot_overlap_per_mapping(overlap_df)
    runtime_df = calc_rt_average(res_df)
    print(overlap_df)

    # TODO wie ist verhalten bei Laufzeit
    #uncertain_df = plot_uncertain_mappings(res_df,PLOT_FIG)
    #df = pd.merge(left=overlap_df,right=uncertain_df,on=['metric','dynamic'])

    '''
    equal_mappings_df = count_correct_mappings(res_df, db_config_df)
    df = pd.merge(left=df, right=equal_mappings_df, on=['metric', 'dynamic'])

    # Divide Sum by nr of records per resource & run
    #df['overlap_perc'] = df['common_records'] * 100 / (nr_total_records * NR_RUNS) # 5 iterations per resource


    # Update uncertain mappings to a percentage, by dividing through nr of all constants
    df['avg_uncertain_mappings'] = df['uncertain_mappings'] * 100 / (nr_total_constants * NR_RUNS)

    df = df.round(2)

    # Change order of columns slightly
    df = df[['metric','dynamic','overlap_perc','avg_uncertain_mappings']]
    db_config_df = db_config_df[['file_name','version','nr_facts','nr_constants']]
    print(df.to_latex())
    print(db_config_df.to_latex())

'''