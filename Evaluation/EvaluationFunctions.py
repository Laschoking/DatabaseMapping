from src.Libraries.PathLib import base_out_path
import numpy as np
import plotly.express as px
import pandas as pd
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df

def plot_std_dev_over_runs(res_df,PLOT_FIG,nr_total_facts):
    """ In case several runs were done for the same configuration: Calculate the variance over all runs """
    """ In the second step: sum variance over all DBS (they are independent) and get std_dev by calculating square root"""
    res_df = res_df[['db_config_id', 'metric', 'dynamic', 'common_records']]
    # TODO check if this makes sense / if it says something valuable
    gb_db = res_df.groupby(['metric', 'dynamic','db_config_id'])
    gb_db = gb_db.var(numeric_only=True)
    gb_db = gb_db.reset_index()
    gb_metric = gb_db.groupby(['metric', 'dynamic'])
    r = gb_metric.sum(numeric_only=True)
    r['std_dev'] = np.sqrt(r['common_records'] / 8)
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r,x='metric', y='std_dev',color='dynamic',barmode="group")
        fig.show()
        fig.write_image("plots/std_dev_of_metrics_over_3_runs.png")
    return r[['metric','dynamic','std_dev']]

def plot_uncertain_mappings(res_df,PLOT_FIG):
    res_df = res_df[['db_config_id', 'metric', 'dynamic', 'uncertain_mappings']]

    gb_metric = res_df.groupby(['metric', 'dynamic'])
    r = gb_metric.sum(numeric_only=True)
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r, x='metric', y='uncertain_mappings', color='dynamic', barmode="group")
        fig.show()
    return r[['metric','dynamic','uncertain_mappings']]

def calc_rt_average(res_df):
    df = res_df[['dynamic', 'metric','runtime']]
    group_df = df.groupby(['metric', 'dynamic'])
    mean_df = group_df.mean(numeric_only=True)
    mean_df = mean_df.reset_index()
    return mean_df[['metric','dynamic','runtime']]

def calc_overlap_perc_all_resources(res_df):
    """ Calculate the average by collecting common records  and divide by nr of constants (using overlap perc. is inaccurate)"""
    df = res_df[['mapping_id', 'metric', 'dynamic','anchor_quantile','importance_weight','common_records','uncertain_mappings','computed_mappings','runtime']]
    group_df = df.groupby(['mapping_id', 'metric', 'dynamic','anchor_quantile','importance_weight'])
    sum_df = group_df.sum(numeric_only=True)
    sum_df = sum_df.reset_index()

    return sum_df

def plot_overlap_per_mapping(overlap_df):
    for ind,row in overlap_df.iterrows():
        overlap_df['name'] = overlap_df.apply(
            lambda row: f"q={row['anchor_quantile']},w={row['importance_weight']},m={row['metric']},d={row['dynamic']}",
            axis=1)
    fig = px.bar(overlap_df, x='name', y="overlap_perc", title='Overlap of each combination')
    fig.show()

def plot_overlap_per_resource(res_df, PLOT_FIG):
    res_df = res_df[['metric','dynamic','anchor_quantile','importance_weight','overlap_perc','db_config_id']]
    if PLOT_FIG:
        for group,df in res_df.groupby(['db_config_id']):
            #df['name'] = f"q={df['anchor_quantile']},w={df['importance_weight']},m={df['metric']}"
            df['name'] = df.apply(
                lambda row: f"q={row['anchor_quantile']},w={row['importance_weight']},m={row['metric']},d={row['dynamic']}", axis=1)
            fig = px.bar(df,x='name',y="overlap_perc",title=str(group[0]),color='metric')
            fig.show()

def plot_best_overlap_per_resource(res_df, PLOT_FIG):
    res_df = res_df[['metric','dynamic','anchor_quantile','importance_weight','overlap_perc','db_config_id','file_name'
        ,'equal_facts_perc']]
    plot_df = pd.DataFrame()
    equal_facts_df = pd.DataFrame(res_df[['db_config_id','equal_facts_perc','file_name']])
    equal_facts_df.rename(columns={'equal_facts_perc':'overlap_perc'},inplace=True)
    equal_facts_df['metric'] = 'equal_facts'
    res_df = pd.concat([res_df,equal_facts_df],axis=0,ignore_index=True)
    # make nr_equal_facts 3. bar
    if PLOT_FIG:
        for group,df in res_df.groupby(['db_config_id','metric']):
            max_ind = df['overlap_perc'].idxmax()
            ser = df.loc[max_ind]
            plot_df = add_series_to_df(series=ser,df=plot_df)

            #plot_df['name'] = df.apply(
            #    lambda row: f"q={row['anchor_quantile']},w={row['importance_weight']},m={row['metric']},d={row['dynamic']}", axis=1)

        fig = px.bar(plot_df,x='file_name',y="overlap_perc",title='Test',color='metric',barmode="group")
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
        else:
            print(f"file does not exist: {mapping_file}")
    group_df = res_df.groupby(['metric','dynamic'])
    group_df = group_df.sum(numeric_only=True)
    group_df = group_df.reset_index()
    return group_df[['metric','dynamic','corr_mappings']]
