import pandas as pd

from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np


# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

def plot_grouped_overlap_mixed_metrics(overlap_df):
    #fig = px.line(overlap_df, x="struct_ratio", y="overlap_perc", title='Overlap of each combination',color='alpha_weight', symbol='alpha_weight')
    #fig.update_traces(marker=dict(size=30))
    fig = px.bar(overlap_df, x="struct_ratio", y="overlap_perc",color='alpha_weight',opacity=0.7,text_auto='.3s')
    fig.update_traces(textfont_size=12, textangle=0, textposition="inside", cliponaxis=False)
    fig.update_layout(barmode='group',
        xaxis=dict(
            tickvals=overlap_df['struct_ratio'],
            title=dict(
                font=dict(
                    size=18,
                    color='#7f7f7f'
                )
            )
        ),
          title=dict(
              font=dict(
                  size=18,
                  color='#7f7f7f'
                  )
              )                      ,
    legend_title_text = 'Importance Weight',
    xaxis_title='Gamma',
    yaxis_title='Overlap in %'

                      )

    fig.show()
    fig.write_image('plots/MixedMetric_same_db_grouped.png',scale=3,width=1000,height=400)
def plot_overlap_mixed_metrics(overlap_df):
    #fig = px.line(overlap_df, x="struct_ratio", y="overlap_perc", title='Overlap of each combination',color='alpha_weight', symbol='alpha_weight')
    #fig.update_traces(marker=dict(size=30))
    fig = px.bar(overlap_df, x="struct_ratio", y="overlap_perc", title='Overlap of each combination',color='alpha_weight',opacity=0.5)
    #fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)text_auto='.4s',
    fig.update_layout(barmode='overlay',
        xaxis=dict(
            tickvals=overlap_df['struct_ratio'],
            titlefont=dict(
                family='Courier New, monospace',
                size=18,
                color='#7f7f7f'
            )))
    for ind,val in enumerate(overlap_df['struct_ratio']):
        fig.add_annotation(
            x=val,
            text=str(round(overlap_df.at[ind,'overlap_perc'],2) ),
            y= 40 if overlap_df.at[ind,'alpha_weight'] else 60,
            showarrow=False,
            font=dict(color='blue' if overlap_df.at[ind,'alpha_weight'] else 'darkred')
        )
    fig.show()
    #fig.write_image('plots/MixedMetric_same_db.png')
def plot_line_overlap_mixed_metrics(overlap_df):
    fig = px.line(overlap_df, x="struct_ratio", y="overlap_perc", title='Overlap of each combination',color='alpha_weight', symbol='alpha_weight')
    fig.update_traces(marker=dict(size=20))

    fig.show()

def plot_runtime(df1,df2):
    # mapping_id = 91
    # group by db_config_id & compute mean von runtime
    # plot by max_tuples / runtime
    df1 = pd.DataFrame()
    overlap_df = pd.concat([df1,df2],axis=0)
    overlap_df = overlap_df[['mapping_id','db_config_id','runtime','max_tuples','run_nr']]
    #overlap_df = overlap_df[overlap_df['mapping_id'] == 91]

    overlap_df['mapping_id'] = overlap_df['mapping_id'].astype(str)

    g = overlap_df.groupby(['mapping_id','db_config_id'])
    m = g.mean(numeric_only=True)
    m = m.reset_index()
    #fig = px.line(m, x="max_tuples", y="runtime",color='mapping_id',markers=True)
    #fig.show()
    fig = px.scatter(overlap_df, x="max_tuples", y="runtime",color='mapping_id')
    fig.update_traces(marker=
                    dict(size=7))
    fig.update_xaxes(title=r"$|\textbf{adom}^1| \cdot |\textbf{adom}^2|$")
    fig.update_yaxes(title="runtime in seconds")
    fig.show()
    fig.write_image('plots/Runtime_mapping_91.png',width=600,height=400,scale=2)


if __name__ == "__main__":
    """ Selects the best two metrics Dice with alpha 0.1,dynamic (mapping_id 16) and FactPair-Sim 0.2,dynamic (mapping_id 58):
    mapping_id=17:	Iterative	0.95	Dice	0.1	True
    mapping_id=58:	Iterative	0.95	FactPair-Sim	0.2	True	"""
    PLOT_FIG = True
    NR_RUNS = 3
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use LIKE \'expansion-same-lib\' ;")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df_w_alpha = sql_con.query_table(query="SELECT * FROM  ExpansionResults ")
    res_df_wo_alpha = sql_con.query_table(query="SELECT * FROM  ExpansionResults_MIX_No_Gamma ")
    res_df_w_alpha['alpha_weight'] = True
    res_df_wo_alpha['alpha_weight'] = False
    res_df_w_alpha = pd.concat([res_df_w_alpha, res_df_wo_alpha], axis=0)
    # for runtimes only
    final_result_df = sql_con.get_table('FinalMappingResults')
    #plot_runtime(res_df_w_alpha,final_result_df)


    # ExpansionResults_MIX_No_Gamma
    #res_df_w_alpha = sql_con.get_table(table="ExpansionResults")
    mapping_df = sql_con.query_table(query="SELECT * FROM MappingSetup WHERE metric=\"FactPair-Sim_Dice\"")


    """ Merge all databases together and add db information for each db pair"""
    res_df_w_alpha = pd.merge(res_df_w_alpha, mapping_df, left_on='mapping_id', right_on='mapping_id')
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db1'],
                           right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db1','version' : 'version_db1','nr_elements' : 'nr_elements_db1'},inplace=True)
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db2'],
                            right_on=['file_name','version'])
    db_config_df.rename(columns={'nr_facts':'nr_facts_db2','version' : 'version_db2','nr_elements' : 'nr_elements_db2'},inplace=True)
    db_config_df.drop(columns=['version_db1','version_db2'],inplace=True)
    db_config_df['nr_poss_facts'] = db_config_df[['nr_facts_db1', 'nr_facts_db2']].min(axis=1)
    res_df_w_alpha = pd.merge(res_df_w_alpha, db_config_df, left_on='db_config_id', right_on='db_config_id')


    # This df holds characteristics of each database
    nr_total_facts = sum(db_config_df['nr_poss_facts'])
    nr_equal_facts = sum(db_config_df['nr_equal_facts']) * NR_RUNS
    gr_cols = ['metric','dynamic']


    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_facts
    overlap_df = calc_overlap_perc_all_resources(res_df_w_alpha, ['metric', 'struct_ratio','alpha_weight'], NR_DBS=5,special_cols=['alpha_weight'])

    overlap_df['overlap_perc'] = overlap_df['common_facts'] * 100 / nr_total_facts

    plot_grouped_overlap_mixed_metrics(overlap_df)
    overlap_df['perc_c_m'] = 100 * overlap_df['computed_mappings'] / overlap_df['max_tuples']
    overlap_df['computed_mappings'] /= 1000000
    max_rt = overlap_df['runtime'].max()
    overlap_df['runtime'] /= 796.108
    print(overlap_df[['metric','alpha_weight','struct_ratio', 'overlap_perc','perc_c_m', 'computed_mappings','runtime']].to_latex())


