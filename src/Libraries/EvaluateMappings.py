import pandas as pd
from prettytable import PrettyTable
from typing import Dict, Union
import pandas as pd


def compute_overlap_dbs(db1, db2, print_flag=False) -> pd.Series:
    l_records_db1 = 0
    l_records_db2 = 0
    l_records_db_merge = 0
    if not db1.files:
        raise ValueError(f"db1 has no entries: {db1.name} ")
    if not db2.files:
        raise ValueError(f"db2 has no entries: {db2.name} ")

    for file_name, df1 in db1.files.items():
        if file_name not in db2.files:
            raise FileNotFoundError("res_df does not exist in db2: " + db2.name + " file: " + file_name)

        df2 = db2.files[file_name]
        if df1.empty or df2.empty:
            # create dummy merge-res_df
            df1_sep = df1
            df2_sep = df2
            df_both = pd.DataFrame()
        else:
            # Merge both dataframes by records
            # pd will append a new column _merge to explain, for which side (db1,db2,both) the new fact holds
            df = pd.merge(df1, df2, how='outer', indicator=True)
            df1_sep = df[df['_merge'] == 'left_only']
            df2_sep = df[df['_merge'] == 'right_only']
            df_both = df[df['_merge'] == 'both']

        l_df1_only = df1_sep.shape[0]
        l_df2_only = df2_sep.shape[0]
        l_df_both = df_both.shape[0]

        l_records_db1 += l_df1_only
        l_records_db2 += l_df2_only
        l_records_db_merge += l_df_both

        if l_df1_only > 0 and print_flag:
            print(f"\n db1 unique-rows in: {file_name} ")
            print(df1_sep.to_csv(sep='\t', index=False, header=False))
        if l_df2_only > 0 and print_flag:
            print(f"\n db2 unique-rows in: {file_name} ")
            print(df2_sep.to_csv(sep='\t', index=False, header=False))

        if l_df1_only + l_df2_only + l_df_both == 0:
            continue

    total_rows = min(l_records_db1, l_records_db2) + l_records_db_merge

    return pd.Series({'unique_records_db1': int(l_records_db1),'unique_records_db2': int(l_records_db2),
            'common_records': int(l_records_db_merge),'overlap_perc': round(100 * l_records_db_merge / total_rows,2)
            })


def count_overlap_merge_db(merge_db) -> Dict[str,Union[int,float]]:
    c_left = 0
    c_right = 0
    c_both = 0

    for df in merge_db.files.values():
        if df.empty:
            continue
        # access last column that holds db_config_id for each record
        val_count = df.iloc[:, -1].value_counts()
        ind = val_count.index
        if '1' in ind:
            c_left += val_count.at['1']
        if '10' in ind:
            c_right += val_count.at['10']
        if '0' in ind:
            c_both += val_count.at['0']
    # the intuition is that matching 5 rows from 7 in each db is a success of 5/7
    total_records = min(c_left, c_right) + c_both
    if total_records > 0:
        overlap = round(100 * c_both / total_records, 2)
    else:
        overlap = 0.0
    return {"unique_records_db1": c_left, "unique_records_db2": c_right, "common_records": c_both,
            "overlap_perc": overlap}


def verify_merge_results(dl_sep_results, mapping) -> bool:
    t = PrettyTable()
    # Color
    r = "\033[0;31;40m"  # RED
    n = "\033[0m"  # Reset

    t.field_names = ["1. DB", "2. DB (merge dl)", "rows of 1.", "rows of 2.", "common rows", "overlap in %"]

    # DB1-separate-results == db1_unravelled_results
    diff_db1 = compute_overlap_dbs(dl_sep_results.db1_original_results, mapping.db1_unravelled_results, print_flag=True)
    if diff_db1['unique_records_db1'] > 0 or diff_db1['unique_records_db2'] > 0:
        l = ([r + dl_sep_results.db1_original_results.name, mapping.db1_unravelled_results.name] +
             list(diff_db1[['unique_records_db1','unique_records_db2','common_records']]) +
             [str(diff_db1['overlap_perc']) + n])
        t.add_row(l)

    # DB2-separate-results == db2_unravelled_results
    diff_db2 = compute_overlap_dbs(dl_sep_results.db2_original_results, mapping.db2_unravelled_results, print_flag=True)
    if diff_db2['unique_records_db1'] > 0 or diff_db2['unique_records_db2'] > 0:
        l = ([r + dl_sep_results.db2_original_results.name, mapping.db2_unravelled_results.name] +
             list(diff_db2[['unique_records_db1','unique_records_db2','common_records']]) + [str(diff_db2['overlap_perc']) + n])
        t.add_row(l)

    if len(t.rows) > 0:
        print(t)

    return t.rows == 0

