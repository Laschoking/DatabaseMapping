from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np


# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

def plot_quantile_results(overlap_df,color=None,group=None):
    #for ind,row in overlap_df.iterrows():
    #    overlap_df['name'] = overlap_df.apply(
    #        lambda row: f"q={row['anchor_quantile']},m={row['metric']}",axis=1)
    fig = px.bar(overlap_df, x='anchor_quantile', y="overlap_perc", title='Overlap of each combination',color='metric',
                 barmode='group')
    #fig.show()
    fig.write_image('plots/QuantileOverlap.png')

if __name__ == "__main__":


    PLOT_FIG = True
    NR_RUNS = 3
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use LIKE \'expansion-same-lib\' ;")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.query_table(query="SELECT * FROM  \'ExpansionResults\' ")

    # ExpansionResults_MIX_No_Gamma
    #res_df_w_alpha = sql_con.get_table(table="ExpansionResults")

    mapping_df =sql_con.query_table(query="SELECT * FROM MappingSetup WHERE metric=\"Dice\" or metric=\"FactPair-Sim\"; ")

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

    #print(calc_dynamic_impact_per_metric(res_df_w_alpha,gr_cols).to_latex())

    # Filter results, for enabled dynamic updates, and reduce Dice variants to 0.2, which is the best one
    res_df = res_df[res_df['dynamic'] == 'True']
    res_df = res_df[~((res_df['metric'] == 'Dice') & (round(res_df['importance_parameter'].astype('double'),2) != 0.2))]

    print(res_df['computed_mappings'].sum())
    print(res_df['max_tuples'].sum())

    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_facts
    overlap_df = calc_overlap_perc_all_resources(res_df,['metric','dynamic','importance_parameter','anchor_quantile'],NR_DBS=5,special_cols=[])
    overlap_df['overlap_perc'] = overlap_df['common_facts'] * 100 / nr_total_facts
    #overlap_df['nr_equal_facts'] = nr_equal_facts
    print(f"number of equal facts:{nr_equal_facts/NR_RUNS} for poss facts {nr_total_facts} -> ratio: {round((100* nr_equal_facts/NR_RUNS)/nr_total_facts,3)}")
    max_matchings = overlap_df.at[0,'max_tuples']
    print(max_matchings)
    #plot_quantile_results(overlap_df,color=None)
    overlap_df['perc_c_m'] = 100 * overlap_df['computed_mappings'] / overlap_df['max_tuples']
    overlap_df['computed_mappings'] /= 1000000
    max_rt = overlap_df['runtime'].max()
    overlap_df['runtime'] /= max_rt
    print(max_rt)
    print(overlap_df.round(2)[['metric','anchor_quantile','overlap_perc','computed_mappings','perc_c_m','runtime']].to_latex())
    # nr_equal_facts = 30603, nr-all-facts?
