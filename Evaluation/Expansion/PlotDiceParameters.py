from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np


# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

def plot_overlap_dice(overlap_df):
    fig = px.bar(overlap_df, x='importance_weight', y="overlap_perc", title='Overlap of each combination', color='dynamic',
                 barmode='group')
    fig.show()

if __name__ == "__main__":
    """ Evaluates the impact of dynamic updates & importance weight for Dice"""
    
    PLOT_FIG = True
    NR_RUNS = 3
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use LIKE \'expansion-same-lib\' ;")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.query_table(query="SELECT * FROM  ExpansionResults ")

    # ExpansionResults_MIX_Weight1
    #res_df = sql_con.get_table(table="ExpansionResults")
    mapping_df =sql_con.query_table(query="SELECT * FROM MappingSetup WHERE metric=\"Dice\" ")

    """ Merge all databases together and add db information for each db pair"""
    res_df = pd.merge(res_df,mapping_df, left_on='mapping_id', right_on='mapping_id')
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file','db1'],
                            right_on=['file','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db1','version' : 'version_db1','nr_constants' : 'nr_constants_db1'},inplace=True)
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file','db2'],
                            right_on=['file','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db2','version' : 'version_db2','nr_constants' : 'nr_constants_db2'},inplace=True)
    db_config_df.drop(columns=['version_db1','version_db2'],inplace=True)
    db_config_df['nr_poss_facts'] = db_config_df[['nr_facts_db1', 'nr_facts_db2']].min(axis=1)
    res_df = pd.merge(res_df,db_config_df, left_on='db_config_id', right_on='db_config_id')


    # This df holds characteristics of each database
    nr_total_facts = sum(db_config_df['nr_poss_facts'])
    nr_equal_facts = sum(db_config_df['nr_equal_facts']) * NR_RUNS
    gr_cols = ['metric','dynamic']

    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_records
    overlap_df = calc_overlap_perc_all_resources(res_df,['metric','dynamic','importance_weight'],NR_DBS=5)
    overlap_df['overlap_perc'] = overlap_df['common_records'] * 100 / nr_total_facts
    overlap_df['nr_equal_facts'] = nr_equal_facts

    plot_overlap_dice(overlap_df)

    print(overlap_df[['metric','dynamic','importance_weight','overlap_perc','computed_mappings']].to_latex())
