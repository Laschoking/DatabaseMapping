import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from pathlib import Path
import pandas as pd
import sqlite3
from matplotlib.patches import Rectangle
import plotly.graph_objects as go
from src.Libraries.PathLib import sql_con

pd.options.plotting.backend = "plotly"

def barplot_nr_use():
    query = '''SELECT resource,use_nr_sim ,SUM(nr_pairs) as MP ,SUM(Equality_corr_pairs) as EQ, SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM LexicalResults GROUP BY resource,use_nr_sim;'''
    df = sql_con.query_table(query)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)
    df['eq_mp'] = round(100 * df['EQ'] / df['MP'], 3)

    res_df = df[['resource','use_nr_sim','avg']]
    res_df = res_df.replace(to_replace='True',value='All lexical metrics with numeric metric')
    res_df = res_df.replace(to_replace='False',value='All lexical metrics without numeric metric')


    new_df = df.query('use_nr_sim==\"True\"')
    new_df = new_df[['resource','use_nr_sim','eq_mp']]
    new_df.rename(columns={'eq_mp':'avg'}, inplace=True)
    new_df['use_nr_sim'] = 'Equal String-Pairs after removing numbers'
    res_df = pd.concat([new_df,res_df])
    res_df.rename(columns={'use_nr_sim': 'Type'},inplace=True)



    fig = px.bar(res_df,x="resource",y="avg",barmode="group",color="Type")
    fig.show()
    fig.write_image("plots/grouped_bars_use_nr_metric.png")

def calc_avg_quality_use_nr_sim():
    query = ('''SELECT use_nr_sim, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM LexicalResults GROUP BY use_nr_sim;''')
    df = sql_con.query_table(query)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)
    for index,r in df.iterrows():
        print(f"use_nr_sim: {r['use_nr_sim']}, average quality: {r['avg']}%")

def calc_avg_rt_use_nr_sim():
    query = (
        '''SELECT use_nr_sim,(AVG(ISUB_runtime) + AVG(JaroWinkler_runtime) +
         AVG(Levenshtein_runtime) + AVG(LCS_runtime) + AVG(QGram_runtime) + AVG(Dice_runtime) )/6 as runtime  
         FROM LexicalResults GROUP BY use_nr_sim;''')
    df = sql_con.query_table(query)

    for index,r in df.iterrows():
        print(f"use_nr_sim: {r['use_nr_sim']}, average runtime: {r['runtime']}s")

    #print(f"quality increasement: {round((nr_qual / no_nr_qual - 1) * 100,2)}% , runtime increasement: {round((nr_rt / no_nr_rt - 1) * 100,2)}%")


def calc_avg_quality_alpha():
    query = ('''SELECT ALPHA, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM LexicalResults  WHERE use_nr_sim="True" GROUP BY ALPHA;''')

    df = sql_con.query_table(query)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)
    for index,r in df.iterrows():
        print(f"ALPHA: {r['ALPHA']}, average quality: {r['avg']}%")

def lineplot_alpha_metrics():
    query = '''SELECT ALPHA, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM LexicalResults  WHERE use_nr_sim="True" GROUP BY ALPHA;'''
    df = sql_con.query_table(query)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    new_avg_list = []
    for metric in metric_list:
        df[metric + "_avg"] = df[metric] / df['MP']
        new_avg_list.append(metric + "_avg")

    df = df.melt(id_vars=['ALPHA'],value_vars=new_avg_list,value_name='precision',var_name='metric')
    #fig = px.box(res_df,x='nr_fake_pairs',y='metric_res')
    #fig.write_image("plots/fake_pair_box_plots.png")

    #fig.show()

    fig = px.line(df, x="ALPHA", y="precision", markers=True,color='metric')
    fig.show()
    fig.write_image("plots/metric_ALPHA_comparison.png")

def lineplot_fake_pairs_metrics():
    query = '''SELECT nr_fake_pairs, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM LexicalResults  WHERE use_nr_sim="True" AND ALPHA="0.95" GROUP BY nr_fake_pairs;'''
    df = sql_con.query_table(query)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    new_avg_list = []
    for metric in metric_list:
        df[metric + "_avg"] = df[metric] / df['MP']
        new_avg_list.append(metric + "_avg")

    df = df.melt(id_vars=['nr_fake_pairs'],value_vars=new_avg_list,value_name='precision',var_name='metric')
    #fig = px.box(res_df,x='nr_fake_pairs',y='metric_res')
    #fig.write_image("plots/fake_pair_box_plots.png")

    #fig.show()

    fig = px.line(df, x="nr_fake_pairs", y="precision", markers=True,color='metric')
    fig.show()
    fig.write_image("plots/metric_nr_pairs_comparison.png")



def barplot_metrics_runtime():
    query = (
        '''SELECT AVG(ISUB_runtime) as ISUB,AVG(JaroWinkler_runtime) as JW,
         AVG(Levenshtein_runtime) as LS,AVG(LCS_runtime) as LCS,AVG(QGram_runtime) as QGRAM, AVG(Dice_runtime) as DICE   
         FROM LexicalResults WHERE ALPHA=0.95;''')
    df = sql_con.query_table(query)
    df = df.T.rename(columns={0:'runtime'})
    fig = px.bar(df,y='runtime',x=df.index, color=df.index,text_auto=True)
    fig.show()
    fig.write_image("plots/metric_runtime_avg.png")
    
def barplot_dice_qgram_by_resource():
    query = '''SELECT resource,nr_pairs, QGram_corr_pairs as QGRAM, Dice_corr_pairs as DICE, ISUB_corr_pairs as ISUB  
        FROM LexicalResults WHERE ALPHA=0.95 AND use_nr_sim="True" AND nr_fake_pairs=12;'''
    df = sql_con.query_table(query)
    df1 = df.melt(id_vars=['resource','nr_pairs'],value_vars=['QGRAM','DICE','ISUB'], var_name='Metric',value_name='corr_pairs')
    df1['avg'] = df1['corr_pairs'] * 100 / df1['nr_pairs']


    # Bar plot
    fig = px.bar(df1,x="resource",y="avg",barmode="group",color="Metric")
    fig.show()
    #fig.write_image("plots/grouped_bars_top_metric.png")
if __name__ == "__main__":

    barplot_nr_use()
    calc_avg_quality_use_nr_sim()
    calc_avg_quality_alpha()
    calc_avg_rt_use_nr_sim()
    lineplot_fake_pairs_metrics()
    lineplot_alpha_metrics()
    barplot_metrics_runtime()

    barplot_dice_qgram_by_resource()


