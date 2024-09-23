import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import jinja2
from pathlib import Path
import pandas as pd
import sqlite3
from matplotlib.patches import Rectangle
import plotly.graph_objects as go
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df
from src.Libraries.PathLib import sql_con, base_out_path
from src.Libraries.ShellLib import count_facts_in_dir
from src.Classes.DataContainerFile import DataContainer

# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

# Calculate the average of each metric for dynamic and static in total
def plot_std_dev_over_runs(res_df,PLOT_FIG):
    res_df = res_df[['db_config_id', 'metric', 'dynamic', 'overlap_perc', 'uncertain_mappings']]
    group_df = res_df.groupby(['metric', 'dynamic'])
    #print(group_df.mean(numeric_only=True))
    gb_db = res_df.groupby(['metric', 'dynamic','db_config_id'])
    gb_db = gb_db.var(numeric_only=True)
    gb_db = gb_db.reset_index()
    gb_metric = gb_db.groupby(['metric', 'dynamic'])
    r = gb_metric.sum(numeric_only=True)
    r['std_dev'] = np.sqrt(r['overlap_perc'] / 8)
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r,x='metric', y='std_dev',color='dynamic',barmode="group")
        fig.show()
        fig.write_image("plots/std_dev_of_metrics_over_3_runs.png")
    return r[['metric','dynamic','std_dev']]
def plot_uncertain_mappings(res_df,PLOT_FIG):
    res_df = res_df[['db_config_id', 'metric', 'dynamic', 'uncertain_mappings']]

    gb_metric = res_df.groupby(['metric', 'dynamic'])
    r = gb_metric.mean(numeric_only=True)
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r, x='metric', y='uncertain_mappings', color='dynamic', barmode="group")
        fig.show()
    return r[['metric','dynamic','uncertain_mappings']]

def plot_overlap_perc_all_metrics(res_df,PLOT_FIG):
    """"""
    df = res_df[['dynamic', 'metric', 'overlap_perc','runtime']]
    group_df = df.groupby(['metric', 'dynamic'])
    mean_df = group_df.mean(numeric_only=True)
    mean_df = mean_df.reset_index()
    if PLOT_FIG:
        fig = px.bar(mean_df,x='metric',y="overlap_perc",barmode="group",color="dynamic",title="Overlap Percentage")
        fig.show()
    return mean_df[['metric','dynamic','overlap_perc','runtime']]

def plot_dynamic_overlap_perc_for_resource(res_df, config_df,PLOT_FIG):
    df = res_df[res_df['dynamic'] == "True"]
    df = df[['metric','overlap_perc','db_config_id']]
    df = df.merge(config_df, left_on='db_config_id', right_on='db_config_id')
    df = df.groupby(['file_name', 'metric', 'db1'])
    df = df.mean(numeric_only=True)
    df.reset_index(inplace=True)
    if PLOT_FIG:
        fig = px.bar(df,x='file_name',y="overlap_perc",barmode="group",color="metric")
        fig.show()




def count_correct_mappings(res_df, config_df):
    """Count for each run (from 5) the number of correct mappings, that are identical x -> x"""

    res_df = res_df.merge(config_df, left_on='db_config_id', right_on='db_config_id')
    # Iterate through each DB-setup (inlcuding the run-nr)
    for index, row in res_df.iterrows():
        mapping_path = (base_out_path.joinpath(row.at['type']).joinpath(row.at['file_name'])
                        .joinpath(row.at['db1'] + "_" + row.at['db2']).joinpath('mappings'))

        # Generate the path where the accepted mappings are saved for each run
        dyn = 'dynamic' if row.dynamic == 'True' else 'static'
        metric_name = row.metric.replace(' ', '')
        mapping_file = f"{dyn}_Iterative-{metric_name}_{row.run_nr}.tsv"
        mapping_path = mapping_path.joinpath(mapping_file)

        # Calculate the number of mappings (eq. number of constants) and the number of correct mappings (c -> c)
        if mapping_path.exists():
            final_mapping_df = pd.read_csv(mapping_path,sep='\t', names=['const1', 'const2', 'sim'], usecols=['const1', 'const2'])
            id_rows = final_mapping_df[final_mapping_df['const1'] == final_mapping_df['const2']]
            res_df.at[index, 'corr_mappings'] = len(id_rows)
            res_df.at[index, 'total_mappings'] = round(len(final_mapping_df))
        else:
            print(f"file does not exist: {mapping_file}")

    res_df['avg_corr_mappings'] = 100 * res_df['corr_mappings'] / res_df['total_mappings']


    nr_mappings_df = res_df[['file_name','db1','total_mappings']]
    res_df = res_df[['db_config_id', 'metric', 'dynamic','avg_corr_mappings']]
    group_df = res_df.groupby(['metric','dynamic'])
    df = group_df.mean(numeric_only=True)
    df.reset_index(inplace=True)
    nr_mappings_df.drop_duplicates(inplace=True,ignore_index=True)
    return df[['metric','dynamic','avg_corr_mappings']],nr_mappings_df


def count_facts_per_db(config_df):
    """ Returns the number of lines that doop generated for a jar-file"""


    config_df = config_df[config_df['use'] == "structural-evaluation"]
    facts_df = pd.DataFrame(columns=['file_name','nr_facts'])
    for index, row in config_df.iterrows():
        fact_path = base_out_path.joinpath(row.at['type']).joinpath(row.at['file_name']) \
                    .joinpath(row.at['db1'] + "_" + row.at['db2']).joinpath(row['db1']).joinpath('facts').joinpath('db1')
        nr_facts = count_facts_in_dir(fact_path)
        s = pd.Series({'file_name':row['file_name'],'nr_facts':nr_facts})
        facts_df = add_series_to_df(series=s,df=facts_df)
    return facts_df

if __name__ == "__main__":
    PLOT_FIG = False
    res_df = sql_con.get_table(table="StructuralResults")
    mapping_df = sql_con.get_table(table="MappingSetup")
    res_df = res_df.merge(mapping_df, left_on='mapping_id', right_on='mapping_id')
    mean_df = plot_overlap_perc_all_metrics(res_df,PLOT_FIG)

    std_dev_df = plot_std_dev_over_runs(res_df,PLOT_FIG)
    df = pd.merge(left=mean_df,right=std_dev_df,left_on=['metric','dynamic'],
                  right_on=['metric','dynamic'])

    uncertain_df = plot_uncertain_mappings(res_df,PLOT_FIG)
    df = pd.merge(left=df,right=uncertain_df,left_on=['metric','dynamic'],
                  right_on=['metric','dynamic'])

    config_df = sql_con.get_table(table="DbConfig")
    corr_mappings_df,resource_df = count_correct_mappings(res_df, config_df)
    facts_df = count_facts_per_db(config_df)
    resource_df = pd.merge(resource_df,facts_df,on='file_name')

    df = pd.merge(left=df, right=corr_mappings_df, left_on=['metric', 'dynamic'],
                  right_on=['metric', 'dynamic'])
    # Update uncertain mappings to a percentage, by dividing through nr of all constants
    nr_total_mappings = np.sum(resource_df['total_mappings'])
    df['avg_uncertain_mappings'] = df['uncertain_mappings'] * 100 / nr_total_mappings
    df = df.round(2)
    # Change order of columns slightly
    df = df[['metric','dynamic','overlap_perc','avg_corr_mappings','avg_uncertain_mappings','std_dev','runtime']]
    print(df.to_latex())
    print(resource_df.to_latex())
    #plot_dynamic_overlap_perc_for_resource(res_df, config_df)

