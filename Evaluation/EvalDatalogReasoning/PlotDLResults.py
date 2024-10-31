import pandas as pd

from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con
import numpy as np
import plotly
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"


# Analyse impact of the Datalog programs (how much overlap did db1&db2 have and how is the separate overlap D1,D2)
def evaluate_impact_of_sep_DL(db_config, dl_results):
    """ Plot the overlap of equal facts between both dbs without any matching & the overlap of results after running a
    separate Program analysis """

    db_config = db_config[['db_config_id','equal_facts_perc','nr_equal_facts','use']]
    db_config['dl_program'] = 'None'
    db_config.rename(columns={'equal_facts_perc':'overlap_perc','nr_equal_facts':'common_facts'}, inplace=True)
    dl_results = dl_results[dl_results['mapping_id'] == 'separate']
    dl_results = dl_results[['db_config_id','overlap_perc','common_facts','dl_program','use']]
    df = pd.concat([db_config,dl_results],axis=0)

    df = df[df['use'] == 'reasoning-evaluation-same-lib']

    df['common_facts'] = pd.to_numeric(df['common_facts'], errors='coerce')

    # Normalize 'common_facts' within each `db_config_id` group
    df['normalized_common_facts'] = df.groupby('db_config_id')['common_facts'].transform(
        lambda x: x / x.max() if x.max() != 0 else 0
    )
    # Scale normalized sizes by a factor, e.g., 20 for visibility
    df['scaled_common_facts'] = df['normalized_common_facts'] * 10

    fig = px.scatter(df,x='db_config_id',y='overlap_perc',color='dl_program',size='normalized_common_facts',size_max=10,
                     labels={
                         "overlap_perc": "overlap in %",
                         "dl_program": ""
                     }
                     )
    newnames = {"None": r'$overlap(\textbf{I}^1,\textbf{I}^2)$', "CFG": r'$overlap(CFG(\textbf{I}^1),CFG(\textbf{I}^2))$', "PointerAnalysis"  : r'$overlap(PA(\textbf{I}^1),PA(\textbf{I}^2))$'}
    fig.for_each_trace(lambda t: t.update(name=newnames[t.name]))
    fig.update_layout(xaxis_title="")
    fig.show()
    fig.write_image('plots/SeparateAnalysisOverlapSize.png',scale=3,width=800,height=600)


def evaluate_impact_of_matching(db_config, matching_results, dl_results):
    """ Plot the percentage of overlap after matching with the overlap after applying the modified dl program"""

    matching_results = matching_results[matching_results['sim_th'] == 0.0]
    matching_results = matching_results[['db_config_id','overlap_perc','common_facts','mapping_id','use']]
    matching_results['dl_program'] = 'matching'

    # mapping_id 91 is with sim_th=0.0
    dl_results = dl_results[dl_results['mapping_id'] == '91']

    dl_results = dl_results[['db_config_id','overlap_perc','common_facts','dl_program','use']]

    df = pd.concat([matching_results,dl_results],axis=0)
    df = df[df['use'] == 'reasoning-evaluation-same-lib']
    df['common_facts'] = pd.to_numeric(df['common_facts'], errors='coerce')

    # Normalize 'common_facts' within each `db_config_id` group
    df['normalized_common_facts'] = df.groupby('db_config_id')['common_facts'].transform(
        lambda x: x / x.max() if x.max() != 0 else 0
    )
    # Scale normalized sizes by a factor, e.g., 20 for visibility
    df['scaled_common_facts'] = df['normalized_common_facts'] * 10

    fig = px.scatter(df,x='db_config_id',y='overlap_perc',color='dl_program',size='normalized_common_facts',size_max=10,
                     labels={
                         "overlap_perc": "overlap in %",
                         "dl_program": ""
                     }
                     )
    newnames = {"matching": r"$overlap(\textbf{I}^m)$", "CFG": r'$overlap(CFG^m(\textbf{I}^m))$', "PointerAnalysis"  : r'$overlap(PA^m(\textbf{I}^m))$'}

    fig.for_each_trace(lambda t: t.update(name=newnames[t.name]))
    fig.update_layout(xaxis_title="")
    fig.show()
    fig.write_image('plots/MatchedAnalysisOverlapSizeSameLib.png',scale=3,width=800,height=600)
'''
,
          legend=dict(
        orientation="h",
        yanchor="bottom",
          entrywidth=150,
        y=1.02,
        xanchor="right",
        x=1
    )
    '''

def evaluate_impact_of_matching_cross(db_config, matching_results, dl_results):
    """ Plot the percentage of overlap after matching with the overlap after applying the modified dl program"""

    matching_results = matching_results[matching_results['sim_th'] == 0.0]
    matching_results = matching_results[matching_results['mapping_id'] == 91]

    matching_results = matching_results[['db_config_id','overlap_perc','common_facts','mapping_id','use']]
    matching_results['dl_program'] = 'matching'

    # mapping_id 91 is with sim_th=0.0
    dl_results = dl_results[dl_results['mapping_id'] == '91']

    dl_results = dl_results[['db_config_id','overlap_perc','common_facts','dl_program','use']]

    df = pd.concat([matching_results,dl_results],axis=0)
    df = df[df['use'] == 'reasoning-cross-evaluation']
    df['common_facts'] = pd.to_numeric(df['common_facts'], errors='coerce')

    # Normalize 'common_facts' within each `db_config_id` group
    df['normalized_common_facts'] = df.groupby('db_config_id')['common_facts'].transform(
        lambda x: x / x.max() if x.max() != 0 else 0
    )
    # Scale normalized sizes by a factor, e.g., 20 for visibility
    df['scaled_common_facts'] = df['normalized_common_facts'] * 10

    fig = px.scatter(df,x='db_config_id',y='overlap_perc',color='dl_program',size='normalized_common_facts',size_max=10,
                     labels={
                         "overlap_perc": "overlap in %",
                         "dl_program": ""
                     }
                     )
    newnames = {"matching": r"$overlap(\textbf{I}^m)$", "CFG": r'$overlap(CFG^m(\textbf{I}^m))$', "PointerAnalysis"  : r'$overlap(PA^m(\textbf{I}^m))$'}

    fig.for_each_trace(lambda t: t.update(name=newnames[t.name]))
    fig.update_layout(xaxis_title="")

    fig.update_xaxes(
        tickvals=['checker-qual-_mix_org.osgi.service.component.annotations-_checker-qual-_3.0.0_org.osgi.service.component.annotations-_1.5.1','org.osgi.service.component.annotations-_mix_jakarta.validation-api-_org.osgi.service.component.annotations-_1.3.0_jakarta.validation-api-_3.1.0','jakarta.validation-api-_mix_jamm-_jakarta.validation-api-_2.0.1_jamm-_0.4.0','jamm-_mix_annotations-_jamm-_0.3.3_annotations-_26.0.1','annotations-_mix_commons-logging-api-_annotations-_13.0_commons-logging-api-_1.1','commons-logging-api-_mix_noark-game-_commons-logging-api-_1.0.3_noark-game-_3.4.0-Final','noark-game-_mix_scala-guice__noark-game-_3.2.0.Final_scala-guice__3-7.0.0','nscala-time__mix_jmh-generator-annprocess-_nscala-time__2.10.0-0.2.0_jmh-generator-annprocess-_1.9','jmh-generator-annprocess-_mix_groovy-datetime-_jmh-generator-annprocess-_1.37_groovy-datetime-_3.0.22'],
        ticktext=['checker_3.0.0_org.osgi_1.5.1','org.osgi_1.3.0_jakarta.validation_3.1.0','jakarta.validation_2.0.1_jamm_0.4.0','jamm_0.3.3_annotations_26.0.1','annotations_13.0_commons-logging_1.1','commons-logging_1.0.3_noark_3.4.0-Final','noark_3.2.0.Final_scala-guice_3-7.0.0','nscala-time_2.10_jmh-generator_1.9','jmh-generator_1.37_groovy-datetime_3.0.22'])
    fig.show()
    fig.write_image('plots/MatchedAnalysisOverlapSizeCross.png',scale=3,width=800,height=600)


def calc_avg_overlap(db_config, matching_results, dl_results):
    db_config.rename(columns={'equal_facts_perc':'overlap_perc'}, inplace=True)
    db_config['dl_program'] = 'None'
    db_config['total_rt'] = 0
    db_config['mapping_id'] = 'separate'
    matching_results = matching_results[matching_results['sim_th'] == 0.0]
    matching_results['dl_program'] = 'None'
    matching_results['total_rt'] = 0
    dl_results_m = dl_results[dl_results['mapping_id'] == '91']
    dl_results_s = dl_results[dl_results['mapping_id'] == 'separate']

    df = pd.concat([db_config,matching_results,dl_results_m,dl_results_s],axis=0)
    df = df[df['use'] == 'reasoning-cross-evaluation']

    df = df[['mapping_id','dl_program','total_rt']]
    fig = px.scatter(df, x='db_config_id', y='overlap_perc', color='dl_program', size='normalized_common_facts',
                     size_max=10,
                     labels={
                         "overlap_perc": "overlap in %",
                         "dl_program": ""
                     }
                     )
    fig.show()

def plot_runtime( dl_results):
    dl_results_m = dl_results[dl_results['mapping_id'] == '91']
    dl_results_s = dl_results[dl_results['mapping_id'] == 'separate']

    df = pd.concat([dl_results_m, dl_results_s], axis=0)
    print(df.columns)

    df = df[df['use'] == 'reasoning-evaluation-same-lib']

    df = df[['mapping_id', 'db_config_id','dl_program', 'total_rt','loading_rt','reasoning_rt','output_rt','run']]
    df_sep = df[df['mapping_id'] == 'separate']
    df_sep_CFG = df_sep[df_sep['dl_program']=='CFG']
    df_sep_PA = df_sep[df_sep['dl_program']=='PointerAnalysis']

    df_m = df[df['mapping_id'] == '91']
    df_m = df_m[df_m['run'] == 3]

    df_m_CFG = df_m[df_m['dl_program']=='CFG']
    df_m_PA = df_m[df_m['dl_program']=='PointerAnalysis']

    separate_color = 'GoldenRod'  # Color for 'separate' bars
    merge_color = 'OliveDrab'  # Color for 'merge' bars

    fig = make_subplots(
        rows=4, cols=2,
        subplot_titles=('CFG total runtime in s','CFG loading runtime in s','CFG reasoning runtime in s','CFG output runtime in s',
                        'PA total runtime in s','PA loading runtime in s','PA reasoning runtime in s','PA output runtime in s'),
        vertical_spacing=0.1,  # Adjusts space between rows
        horizontal_spacing=0.1
    )
    fig.add_trace(go.Bar(name='separate',x=df_sep_CFG['db_config_id'],y=df_sep_CFG['total_rt'],marker_color=separate_color, showlegend=False),row=1,col=1)
    fig.add_trace(go.Bar(name='merge',x=df_m_CFG['db_config_id'],y=df_m_CFG['total_rt'],marker_color=merge_color, showlegend=False),row=1,col=1)

    fig.add_trace(go.Bar(name='separate',x=df_sep_CFG['db_config_id'],y=df_sep_CFG['loading_rt'],marker_color=separate_color, showlegend=False),row=1,col=2)
    fig.add_trace(go.Bar(name='merge',x=df_m_CFG['db_config_id'],y=df_m_CFG['loading_rt'],marker_color=merge_color, showlegend=False),row=1,col=2)

    fig.add_trace(go.Bar(name='separate', x=df_sep_CFG['db_config_id'], y=df_sep_CFG['reasoning_rt'],marker_color=separate_color, showlegend=False), row=2, col=1)
    fig.add_trace(go.Bar(name='merge', x=df_m_CFG['db_config_id'], y=df_m_CFG['reasoning_rt'],marker_color=merge_color, showlegend=False), row=2, col=1)

    fig.add_trace(go.Bar(name='separate', x=df_sep_CFG['db_config_id'], y=df_sep_CFG['output_rt'],marker_color=separate_color, showlegend=False), row=2, col=2)
    fig.add_trace(go.Bar(name='merge', x=df_m_CFG['db_config_id'], y=df_m_CFG['output_rt'],marker_color=merge_color, showlegend=False), row=2, col=2)

    # for PA:

    fig.add_trace(go.Bar(name='separate', x=df_sep_PA['db_config_id'], y=df_sep_PA['total_rt'],marker_color=separate_color, showlegend=False), row=3, col=1)
    fig.add_trace(go.Bar(name='merge', x=df_m_PA['db_config_id'], y=df_m_PA['total_rt'],marker_color=merge_color, showlegend=False), row=3, col=1)

    fig.add_trace(go.Bar(name='separate', x=df_sep_PA['db_config_id'], y=df_sep_PA['loading_rt'],marker_color=separate_color, showlegend=False), row=3, col=2)
    fig.add_trace(go.Bar(name='merge', x=df_m_PA['db_config_id'], y=df_m_PA['loading_rt'],marker_color=merge_color, showlegend=False), row=3, col=2)

    fig.add_trace(go.Bar(name='separate', x=df_sep_PA['db_config_id'], y=df_sep_PA['reasoning_rt'],marker_color=separate_color, showlegend=False), row=4, col=1)
    fig.add_trace(go.Bar(name='merge', x=df_m_PA['db_config_id'], y=df_m_PA['reasoning_rt'],marker_color=merge_color, showlegend=False), row=4, col=1)

    fig.add_trace(go.Bar(name='separate', x=df_sep_PA['db_config_id'], y=df_sep_PA['output_rt'],marker_color=separate_color, showlegend=False), row=4, col=2)
    fig.add_trace(go.Bar(name='merge', x=df_m_PA['db_config_id'], y=df_m_PA['output_rt'],marker_color=merge_color, showlegend=False), row=4, col=2)

    # Increase title size
    fig.update_layout(
        title_font=dict(size=40),  # Adjust main title size if you have one
        font=dict(size=25)  # Adjust general font size
    )

    # Manually set the size for subplot titles
    for i, title in enumerate(fig['layout']['annotations']):
        title['font']['size'] = 40  # Adjust the font size as needed
    fig.update_layout(barmode="group")
    fig.update_xaxes(showticklabels=False)
    #fig.show()
    #fig.write_image('plots/runtimeDL.png',width=2481, height=3508,scale=1)
    g = df.groupby(['dl_program','mapping_id'])
    m = round(g.mean(numeric_only=True),3)
    m = m.reset_index()
    m = m[['mapping_id','dl_program','total_rt','loading_rt','reasoning_rt','output_rt']]
    print(m.to_latex())



if __name__ == "__main__":
    dl_results = sql_con.get_table('DLResults')
    mapping_setup = sql_con.get_table('MappingSetup')

    db_config = sql_con.get_table('DbConfig')
    dl_results = pd.merge(db_config[['db_config_id','use']],dl_results,on='db_config_id')
    matching_results = sql_con.get_table('FinalMappingResults')
    matching_results = pd.merge(matching_results,mapping_setup, on='mapping_id', how='left')
    matching_results = pd.merge(db_config[['db_config_id','use']],matching_results,on='db_config_id')

    #evaluate_impact_of_sep_DL(db_config, dl_results)
    #evaluate_impact_of_matching(db_config, matching_results, dl_results)
    #evaluate_impact_of_matching_cross(db_config, matching_results, dl_results)
    #calc_avg_overlap(db_config, matching_results, dl_results)
    plot_runtime(dl_results)
