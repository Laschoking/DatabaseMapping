import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from pathlib import Path
import pandas as pd
import sqlite3
from matplotlib.patches import Rectangle
import plotly.graph_objects as go

pd.options.plotting.backend = "plotly"


def plot_nr_use_avg(con):
    query = '''SELECT resource,use_nr_sim ,SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM Lexical_Results GROUP BY resource,use_nr_sim;'''
    df = pd.read_sql_query(query,con)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)

    fig = px.bar(df,x="resource",y="avg",barmode="group",color="use_nr_sim")
    #fig.show()
    fig.write_image("plots/grouped_bars_use_nr_metric.png")

def calc_avg_quality_use_nr_sim(con):
    query = ('''SELECT use_nr_sim, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM Lexical_Results GROUP BY use_nr_sim;''')
    df = pd.read_sql_query(query,con)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)
    for index,r in df.iterrows():
        print(f"use_nr_sim: {r['use_nr_sim']}, average quality: {r['avg']}%")

def calc_avg_rt_use_nr_sim(con):
    query = (
        '''SELECT use_nr_sim,(AVG(ISUB_runtime) + AVG(JaroWinkler_runtime) +
         AVG(Levenshtein_runtime) + AVG(LCS_runtime) + AVG(QGram_runtime) + AVG(Dice_runtime) )/6 as runtime  
         FROM Lexical_Results GROUP BY use_nr_sim;''')
    df = pd.read_sql_query(query, con)

    for index,r in df.iterrows():
        print(f"use_nr_sim: {r['use_nr_sim']}, average runtime: {r['runtime']}s")

    #print(f"quality increasement: {round((nr_qual / no_nr_qual - 1) * 100,2)}% , runtime increasement: {round((nr_rt / no_nr_rt - 1) * 100,2)}%")


def calc_avg_quality_alpha(con):
    query = ('''SELECT ALPHA, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM Lexical_Results  WHERE use_nr_sim="True" GROUP BY ALPHA;''')

    df = pd.read_sql_query(query,con)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    df['sum'] = df[metric_list].sum(axis=1)
    df['avg'] = round(100 * df['sum'] / (6 * df['MP']),3)
    for index,r in df.iterrows():
        print(f"ALPHA: {r['ALPHA']}, average quality: {r['avg']}%")

def lineplot_alpha_metrics(con):
    query = '''SELECT ALPHA, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM Lexical_Results  WHERE use_nr_sim="True" GROUP BY ALPHA;'''
    df = pd.read_sql_query(query,con)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    new_avg_list = []
    for metric in metric_list:
        df[metric + "_avg"] = df[metric] / df['MP']
        new_avg_list.append(metric + "_avg")

    df = df.melt(id_vars=['ALPHA'],value_vars=new_avg_list,value_name='precision',var_name='metric')
    #fig = px.box(df,x='nr_fake_pairs',y='metric_res')
    #fig.write_image("plots/fake_pair_box_plots.png")

    #fig.show()

    fig = px.line(df, x="ALPHA", y="precision", markers=True,color='metric')
    fig.show()
    fig.write_image("plots/metric_ALPHA_comparison.png")

def lineplot_fake_pairs_metrics(con):
    query = '''SELECT nr_fake_pairs, SUM(nr_pairs) as MP ,SUM(ISUB_corr_pairs) as ISUB, SUM(JaroWinkler_corr_pairs) as JW, 
    SUM(Levenshtein_corr_pairs) as LS, SUM(LCS_corr_pairs) as LCS, SUM(QGram_corr_pairs) as QGRAM, SUM(Dice_corr_pairs) as DICE  
    FROM Lexical_Results  WHERE use_nr_sim="True" AND ALPHA="0.95" GROUP BY nr_fake_pairs;'''
    df = pd.read_sql_query(query,con)
    metric_list = ['ISUB','JW','LS','LCS','QGRAM','DICE']
    new_avg_list = []
    for metric in metric_list:
        df[metric + "_avg"] = df[metric] / df['MP']
        new_avg_list.append(metric + "_avg")

    df = df.melt(id_vars=['nr_fake_pairs'],value_vars=new_avg_list,value_name='precision',var_name='metric')
    #fig = px.box(df,x='nr_fake_pairs',y='metric_res')
    #fig.write_image("plots/fake_pair_box_plots.png")

    #fig.show()

    fig = px.line(df, x="nr_fake_pairs", y="precision", markers=True,color='metric')
    fig.show()
    fig.write_image("plots/metric_nr_pairs_comparison.png")



def barplot_metrics_runtime(con):
    query = (
        '''SELECT AVG(ISUB_runtime) as ISUB,AVG(JaroWinkler_runtime) as JW,
         AVG(Levenshtein_runtime) as LS,AVG(LCS_runtime) as LCS,AVG(QGram_runtime) as QGRAM, AVG(Dice_runtime) as DICE   
         FROM Lexical_Results WHERE ALPHA=0.95;''')
    df = pd.read_sql_query(query, con)
    df = df.T.rename(columns={0:'runtime'})
    fig = px.bar(df,y='runtime',x=df.index, color=df.index,text_auto=True)
    fig.show()
    fig.write_image("plots/metric_runtime_avg.png")
    

if __name__ == "__main__":
    db_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Evaluation/Evaluation.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    # Note: returning the AVG(x_percentage) via sql is wrong because the resource tables have a different length
    # The correct way is to sum the correct pairs & divide by entries of the tables
    #plot_nr_use_avg(con)
    #calc_avg_quality_use_nr_sim(con)
    #calc_avg_quality_alpha(con)
    #calc_avg_rt_use_nr_sim(con)
    #lineplot_fake_pairs_metrics(con)
    #lineplot_alpha_metrics(con)
    barplot_metrics_runtime(con)
    con.close()


