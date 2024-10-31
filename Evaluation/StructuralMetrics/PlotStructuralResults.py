from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con

# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

if __name__ == "__main__":
    PLOT_FIG = False
    NR_RUNS = 3
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use=\'structural-evaluation\';")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.get_table(table="StructuralResults")
    mapping_df = sql_con.get_table(table="MappingSetup")
    res_df = pd.merge(res_df,mapping_df, left_on='mapping_id', right_on='mapping_id')
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db1'], right_on=['file_name','version'])


    gb_cols = ['metric','dynamic','importance_parameter']

    # This df holds characteristics of each database
    nr_total_facts = sum(db_config_df['nr_facts'])
    nr_total_elements = sum(db_config_df['nr_elements'])

    # This collects the sum of correct mapped factS per metric and is normalised with nr_total_facts
    overlap_df = calc_overlap_perc_all_resources(res_df,gb_cols)

    runtime_df = calc_rt_average(res_df,gb_cols)
    rt_dynamic = calc_rt_average(res_df,['dynamic'])
    print(rt_dynamic)


    #std_dev_df = plot_std_dev_over_runs(res_df_w_alpha,PLOT_FIG,gb_cols,nr_total_facts)
    #print(std_dev_df)

    df = pd.merge(left=runtime_df,right=std_dev_df,on=gb_cols)
    df = pd.merge(overlap_df,df,on=gb_cols)
    uncertain_df = plot_uncertain_mappings(res_df,PLOT_FIG,gb_cols)
    df = pd.merge(left=df,right=uncertain_df,on=gb_cols)

    corr_mappings_df = count_correct_mappings(res_df, db_config_df,gb_cols)


    df = pd.merge(left=df, right=corr_mappings_df, on=gb_cols)

    # Divide Sum by nr of facts per resource & run
    df['overlap_perc'] = df['common_facts'] * 100 / (nr_total_facts * NR_RUNS) # 5 iterations per resource


    # Update uncertain mappings to a percentage, by dividing through nr of all elements
    df['avg_uncertain_mappings'] = df['uncertain_mappings'] * 100 / (nr_total_elements * NR_RUNS)
    df['avg_corr_mappings'] = df['corr_mappings'] * 100 / (nr_total_elements * NR_RUNS)


    df['rt_normalised']  = df['runtime'] / max(df['runtime'])
    df = df.round(2)

    # Change order of columns slightly
    df = df[['metric','dynamic','importance_parameter','overlap_perc','avg_corr_mappings','avg_uncertain_mappings','rt_normalised']]
    db_config_df = db_config_df[['file_name','version','nr_facts','nr_elements']]
    print(df.sort_values(['metric','dynamic',"importance_weight"]).to_latex())
    print(db_config_df.to_latex())

