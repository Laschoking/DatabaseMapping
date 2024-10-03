from src.Libraries.PathLib import base_out_path
import numpy as np
import plotly.express as px
import pandas as pd
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df

def plot_std_dev_over_runs(res_df,PLOT_FIG,gb_cols,nr_total_records):
    """ In case several runs were done for the same configuration: Calculate the variance over all runs """
    """ In the second step: sum variance over all DBS (they are independent) and get std_dev by calculating square root"""
    res_df = res_df[gb_cols + ['db_config_id', 'common_records']]

    gb_db = res_df.groupby(gb_cols + ['db_config_id'])
    gb_db = gb_db.var(numeric_only=True)
    gb_db = gb_db.reset_index()
    gb_metric = gb_db.groupby(gb_cols)
    r = gb_metric.sum(numeric_only=True)
    r['std_dev'] = np.sqrt(r['common_records'] / 8) * 100 /nr_total_records
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r,x='metric', y='std_dev',color='dynamic',barmode="group")
        fig.show()
        fig.write_image("plots/std_dev_of_metrics_over_3_runs.png")
    return r[gb_cols + ['std_dev']]

def plot_uncertain_mappings(res_df,PLOT_FIG,gb_cols):
    res_df = res_df[gb_cols + ['db_config_id', 'uncertain_mappings']]

    gb_metric = res_df.groupby(gb_cols)
    r = gb_metric.sum(numeric_only=True)
    r = r.reset_index()
    if PLOT_FIG:
        fig = px.bar(r, x='metric', y='uncertain_mappings', color='dynamic', barmode="group")
        fig.show()
    return r[gb_cols + ['uncertain_mappings']]

def calc_rt_average(res_df,gb_cols):
    df = res_df[gb_cols + ['runtime']]
    group_df = df.groupby(gb_cols)
    mean_df = group_df.mean(numeric_only=True)
    mean_df = mean_df.reset_index()
    return mean_df[gb_cols + ['runtime']]

def calc_overlap_perc_all_resources(res_df,gb_cols):
    """ Calculate the average by collecting common records  and divide by nr of constants (using overlap perc. is inaccurate)"""
    df = res_df[['mapping_id', 'metric', 'dynamic','anchor_quantile','importance_weight','common_records','computed_mappings']]
    group_df = df.groupby(gb_cols)
    sum_df = group_df.sum(numeric_only=True)
    sum_df = sum_df.reset_index()

    return sum_df

def plot_overlap_per_mapping(overlap_df):
    for ind,row in overlap_df.iterrows():
        overlap_df['name'] = overlap_df.apply(
            lambda row: f"q={row['anchor_quantile']},w={row['importance_weight']},m={row['metric']},d={row['dynamic']}",
            axis=1)
    fig = px.bar(overlap_df, x='name', y="overlap_perc", title='Overlap of each combination',color='metric')
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

def calc_best_anchor_quantile(res_df,gr_cols):
    #res_df = res_df[['anchor_quantile','common_records','use','file_name','nr_poss_facts','metric',
    #                 'runtime','computed_mappings']]
    group_df = res_df.groupby(gr_cols)
    s = group_df.size()

    group_df = group_df.sum(numeric_only=True)
    gr_size = s.iat[0]
    group_df['avg_overlap'] = round(group_df['common_records'] * 100 / (group_df['nr_poss_facts']),2)
    group_df['avg_rt'] = round(group_df['runtime'] / (gr_size),1)
    group_df['avg_comp_mappings'] = round(group_df['computed_mappings'] / (gr_size * 1000))


    group_df.reset_index(inplace=True)

    return group_df[gr_cols + ['anchor_quantile','avg_overlap','avg_rt','avg_comp_mappings']]


def calc_best_importance_weight(res_df,gr_cols):
    res_df = res_df[res_df['dynamic'] == 'True']
    res_df = res_df[res_df['anchor_quantile'] == 0.95]

    res_df = res_df[['importance_weight','common_records','use','file_name','nr_poss_facts','metric',
                     'runtime','computed_mappings','uncertain_mappings']]
    group_df = res_df.groupby(gr_cols)
    s = group_df.size()

    group_df = group_df.sum(numeric_only=True)
    gr_size = s.iat[0]
    group_df['avg_overlap'] = round(group_df['common_records'] * 100 / (group_df['nr_poss_facts']),2)
    group_df['avg_rt'] = round(group_df['runtime'] / (gr_size),1)
    group_df['avg_comp_mappings'] = round(group_df['computed_mappings'] / (gr_size * 1000))



    group_df.reset_index(inplace=True)

    return group_df[['importance_weight','use','avg_overlap','avg_rt','avg_comp_mappings']]




def count_correct_mappings(res_df, config_df,gb_cols):
    """Count for each run (from 5) the number of correct mappings, that are identical x -> x"""

    res_df = res_df.merge(config_df, left_on='db_config_id', right_on='db_config_id')
    # Iterate through each DB-setup (inlcuding the run-nr)
    for index, row in res_df.iterrows():
        mapping_path = (base_out_path.joinpath(row.at['type']).joinpath(row.at['file_name'])
                        .joinpath(row.at['db1'] + "_" + row.at['db2']).joinpath('mappings'))

        # Generate the path where the accepted mappings are saved for each run
        metric_name = row.metric.replace(' ', '')
        mapping_file = f"id_{row.mapping_id}_run_{row.run_nr}.tsv"
        mapping_path = mapping_path.joinpath(mapping_file)

        # Calculate the number of mappings (eq. number of constants) and the number of correct mappings (c -> c)
        if mapping_path.exists():
            final_mapping_df = pd.read_csv(mapping_path,sep='\t', names=['const1', 'const2', 'sim'], usecols=['const1', 'const2'])
            id_rows = final_mapping_df[final_mapping_df['const1'] == final_mapping_df['const2']]
            res_df.at[index, 'corr_mappings'] = len(id_rows)
        else:
            print(f"file does not exist: {mapping_file}")
    group_df = res_df.groupby(gb_cols)
    group_df = group_df.sum(numeric_only=True)
    group_df = group_df.reset_index()
    return group_df[gb_cols + ['corr_mappings']]


def calc_dynamic_impact_per_metric(res_df,gr_cols):
    group_df = res_df.groupby(gr_cols)
    group_df = group_df.sum(numeric_only=True)
    group_df = group_df.reset_index()
    group_df['avg_overlap'] = round(group_df['common_records'] * 100 / group_df['nr_poss_facts'],2)
    return group_df[['metric','dynamic','avg_overlap']]

