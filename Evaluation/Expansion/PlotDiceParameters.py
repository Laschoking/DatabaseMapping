from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np


# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

def plot_overlap_dice(overlap_df):
    rename_df = overlap_df.rename({'overlap_perc':'overlap','dynamic':'dynamic updates', 'importance_parameter':'importance parameter alpha'}, axis=1)
    fig = px.bar(rename_df, x='importance parameter alpha', y="overlap", color='dynamic updates',
                 barmode='group',text_auto='.4s')
    fig.update_traces(textfont_size=14, textangle=0, textposition="outside", cliponaxis=False)
    #fig.show()
    fig.write_image('plots/DiceBehaviourForParams.png')

if __name__ == "__main__":
    """ Evaluates the impact of dynamic updates & importance weight for Dice"""
    
    PLOT_FIG = True
    NR_RUNS = 3
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use LIKE \'expansion-same-lib\' ;")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.query_table(query="SELECT * FROM  ExpansionResults ")

    # ExpansionResults_MIX_Weight1
    #res_df_w_alpha = sql_con.get_table(table="ExpansionResults")
    mapping_df =sql_con.query_table(query="SELECT * FROM MappingSetup WHERE metric=\"Dice\" ")

    """ Merge all databases together and add db information for each db pair"""
    res_df = pd.merge(res_df,mapping_df, left_on='mapping_id', right_on='mapping_id')
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db1'],
                            right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db1','version' : 'version_db1','nr_elements' : 'nr_elements_db1'},inplace=True)
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db2'],
                            right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db2','version' : 'version_db2','nr_elements' : 'nr_elements_db2'},inplace=True)
    db_config_df.drop(columns=['version_db1','version_db2'],inplace=True)
    db_config_df['nr_poss_facts'] = db_config_df[['nr_facts_db1', 'nr_facts_db2']].min(axis=1)
    res_df = pd.merge(res_df,db_config_df, left_on='db_config_id', right_on='db_config_id')


    # This df holds characteristics of each database
    nr_total_facts = sum(db_config_df['nr_poss_facts'])
    nr_equal_facts = sum(db_config_df['nr_equal_facts']) * NR_RUNS
    gr_cols = ['metric','dynamic']

    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_facts
    overlap_df = calc_overlap_perc_all_resources(res_df,['metric','dynamic','importance_parameter'],NR_DBS=5)
    overlap_df['overlap_perc'] = overlap_df['common_facts'] * 100 / nr_total_facts
    overlap_df['nr_equal_facts'] = nr_equal_facts

    plot_overlap_dice(overlap_df)

    print(overlap_df[['metric','dynamic','importance_parameter','overlap_perc','computed_mappings']].to_latex())
