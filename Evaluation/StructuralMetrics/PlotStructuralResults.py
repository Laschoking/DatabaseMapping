from Evaluation.EvaluationFunctions import *
from src.Libraries.PathLib import sql_con

# Set option to display all rows and columns
pd.set_option('display.max_rows', None)  # Display all rows
pd.set_option('display.max_columns', None)  # Display all columns
pd.options.plotting.backend = "plotly"

if __name__ == "__main__":
    PLOT_FIG = False
    NR_RUNS = 5
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use=\'structural-evaluation\';")
    single_db_char_df = sql_con.get_table(table="DbFingerPrint")
    res_df = sql_con.get_table(table="StructuralResults")
    mapping_df = sql_con.get_table(table="MappingSetup")
    res_df = pd.merge(res_df,mapping_df, left_on='mapping_id', right_on='mapping_id')
    db_config_df = pd.merge(db_config_df,single_db_char_df, left_on=['file_name','db1'], right_on=['file_name','version'])

    # This df holds characteristics of each database
    nr_total_records = sum(db_config_df['nr_facts'])
    nr_total_constants = sum(db_config_df['nr_constants'])

    # This collects the sum of correct mapped RECORDS per metric and is normalised with nr_total_records
    overlap_df = calc_overlap_perc_all_resources(res_df, PLOT_FIG)

    runtime_df = calc_rt_average(res_df)

    std_dev_df = plot_std_dev_over_runs(res_df,PLOT_FIG,nr_total_records)

    df = pd.merge(left=runtime_df,right=std_dev_df,on=['metric','dynamic'])
    df = pd.merge(overlap_df,df,on=['metric','dynamic'])

    uncertain_df = plot_uncertain_mappings(res_df,PLOT_FIG)
    df = pd.merge(left=df,right=uncertain_df,on=['metric','dynamic'])

    corr_mappings_df = count_correct_mappings(res_df, db_config_df)


    df = pd.merge(left=df, right=corr_mappings_df, on=['metric', 'dynamic'])

    # Divide Sum by nr of records per resource & run
    df['overlap_perc'] = df['common_records'] * 100 / (nr_total_records * NR_RUNS) # 5 iterations per resource


    # Update uncertain mappings to a percentage, by dividing through nr of all constants
    df['avg_uncertain_mappings'] = df['uncertain_mappings'] * 100 / (nr_total_constants * NR_RUNS)
    df['avg_corr_mappings'] = df['corr_mappings'] * 100 / (nr_total_constants * NR_RUNS)


    #TODO check if the std_dev is correct

    df = df.round(2)

    # Change order of columns slightly
    df = df[['metric','dynamic','overlap_perc','avg_corr_mappings','avg_uncertain_mappings','std_dev','runtime']]
    db_config_df = db_config_df[['file_name','version','nr_facts','nr_constants']]
    print(df.to_latex())
    print(db_config_df.to_latex())

