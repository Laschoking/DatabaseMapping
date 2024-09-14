from pathlib import Path
from src.Libraries.PathLib import sql_con,lex_eval_db
import sqlite3
from src.LexicalSimilarityMetrics import ISUB,LevenshteinSimilarity,JaroWinkler,QGram,LCS,Equality,Dice
import pandas as pd
import time
import re
from src.Libraries.SqlConnector import SqlConnector

# This evaluation is based on true data, that is pairs of renamed variables, classes, methods, and parameters
# The quality of a metric is measured, by how good it can identify the correct pair from a set of fake pairs


# TODO in best case: meld data in Lexical Results (with 1 Result-record per Metric)

def get_fake_tuples(corr_pair, corr_id, rename_df, NR_FAKE_PAIRS):
    fake_pairs = []
    i = corr_id
    go_backwards = False
    while len(fake_pairs) < NR_FAKE_PAIRS:
        if not go_backwards:
            i += 1
        else:
            i -= 1
        # Handle end of data, where sliding window would cause IndexError
        if i >= len(rename_df):
            go_backwards = True
            continue

        next_corr_pair = rename_df.iloc[i,:]
        fake_name1 = next_corr_pair['OldName']
        fake_name2 = next_corr_pair['NewName']
        if fake_name1 != corr_pair['OldName']:
            fake_pairs.append((fake_name1,corr_pair['NewName']))

        if fake_name2 != corr_pair['NewName']:
            fake_pairs.append((corr_pair["OldName"],fake_name2))



    return fake_pairs


def evaluate_sim_metric(metric, corr_pair, fake_pairs, parsed_res, COMP_NR_SIM,ALPHA):
    t0 = time.time()
    old_name = corr_pair['OldName']
    new_name = corr_pair['NewName']

    c = 1 # Count how many pairs are evaluated
    # If the numerical metric is used, calculate the lexical metric on the remaining String-Components
    if COMP_NR_SIM:
        old_str, old_nr = parsed_res[corr_pair[old_name]]
        new_str, new_nr = parsed_res[corr_pair[new_name]]
        corr_sim = ALPHA * metric.compute_lexical_similarity(old_str, new_str) + (1 - ALPHA) * metric.number_similarity(old_nr, new_nr)
    else:
        # If no numerical metric is used, calculate the lexical metric on the full name (incl. numbers)
        corr_sim = metric.compute_lexical_similarity(old_name, new_name)

    for fake_pair in fake_pairs:
        old_name = fake_pair['OldName']
        new_name = fake_pair['NewName']

        c += 1

        # Same system as for the correct string pair above
        if COMP_NR_SIM:
            old_str, old_nr = parsed_res[corr_pair[old_name]]
            new_str, new_nr = parsed_res[corr_pair[new_name]]
            incorr_sim = ALPHA * metric.compute_lexical_similarity(old_str, new_str) + (1 - ALPHA) * metric.number_similarity(old_nr, new_nr)

        else:
            incorr_sim = metric.compute_lexical_similarity(old_name, new_name)

        if incorr_sim >= corr_sim:
            t1 = time.time()
            return corr_pair,corr_sim,fake_pair,incorr_sim, (t1 - t0) / c

    t1 = time.time()
    return corr_pair,corr_sim,None,None, (t1 - t0) / c


if __name__ == "__main__":
    #############################################################
    # Important constants:
    SQL_TABLES = ['rename_attribute','rename_variable','rename_class','rename_method','rename_parameter']
    fake_pairs = [2,4,6,8,10,12]
    alphas = [0.6,0.8,0.95]
    no_nr_use_combinations = [(fp,False,1) for fp in fake_pairs]
    nr_use_combinations = [(fp,True,alpha) for fp in fake_pairs for alpha in alphas]
    combinations = no_nr_use_combinations + nr_use_combinations
    #############################################################

    lex_data_con = SqlConnector(lex_eval_db)


    EQ = Equality.Equality()
    ISUB = ISUB.IsubStringMatcher()
    JW = JaroWinkler.JaroWinkler()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()
    LCS = LCS.LCS()
    QGRAM2 = QGram.QGram(2)
    DICE2 = Dice.Dice(2)
    metric_objs = [EQ,ISUB,JW,LS,LCS,QGRAM2,DICE2]

    # This implementation could be improved since we insert several results into one row

    cols = ["resource","nr_pairs","nr_fake_pairs","use_nr_sim","ALPHA"]
    for metric in metric_objs:
        cols.append(metric.name + "_corr_pairs")
        cols.append(metric.name + "_percentage")
        cols.append(metric.name + "_runtime") # Runtime is for 10000 computed pairs

    existing_lex_res = sql_con.get_table("MappingSetup")
    new_lex_res = pd.DataFrame(columns=cols)


    for SQL_TABLE in SQL_TABLES:
        print(SQL_TABLE)
        t_total0 = time.time()
        data = []
        old_names = set()
        new_names = set()
        # TODO remove limit once everything works
        query = f"SELECT OldName, NewName FROM {SQL_TABLE}  GROUP BY OldName, NewName LIMIT 100;"
        rename_df = lex_data_con.query_table(query)

        # Adapt the actual size from qual_res, because it filters & groups
        MAX_PAIRS = len(rename_df)
        parsed_res = dict()


        for index,(term_name1,term_name2) in rename_df.iterrows():
            red_name1 = term_name1.lower()
            red_name2 = term_name2.lower()

            # Filter both strings for numbers, and process numbers separately
            nrs1 = re.findall(r'\d+', red_name1)
            nrs2 = re.findall(r'\d+', red_name2)
            red_name1 = re.sub(r'\d+', "",red_name1)
            red_name2 = re.sub(r'\d+', "",red_name2)
            parsed_res[term_name1] = (red_name1,nrs1)
            parsed_res[term_name2] = (red_name2,nrs2)

        for (NR_FAKE_PAIRS,COMP_NR_SIM,ALPHA) in combinations:
            # Check if combination was already computed
            if (existing_lex_res[existing_lex_res.columns[:5]] == [SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS,COMP_NR_SIM,ALPHA]).all(axis=1).any():
                continue
            print(f"calculate combination:{SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS,COMP_NR_SIM,ALPHA}")


            metrics = {obj: [0, 0] for obj in metric_objs}

            log_it = 0
            for index,corr_pair in rename_df.iterrows():
                t1,n1 = parsed_res[corr_pair.at['OldName']]
                t2, n2 = parsed_res[corr_pair.at['NewName']]

                log_it += 1
                if log_it % 10000 == 0:
                    print(f"evaluated {log_it} pairs")
                row = [corr_pair,0]
                fake_tuples = get_fake_tuples(corr_pair,log_it, rename_df, NR_FAKE_PAIRS)
                for metric in metrics.keys():
                    corr_pair,corr_sim,fake_pair,incorr_sim,rt = evaluate_sim_metric(metric, corr_pair, fake_tuples, parsed_res, COMP_NR_SIM,ALPHA)
                    row += [corr_sim,fake_pair,incorr_sim]
                    metrics[metric][1] += rt
                    if not fake_pair:
                        row[1] += 1
                        metrics[metric][0] += 1
                data.append(row)


            cols = ["corr_pair","corr_metrics"]
            for metric in metrics:
                cols.append(metric.name + "sim")
                cols.append(metric.name + "fake_pair")
                cols.append(metric.name + "fake_sim")

            #df = pd.DataFrame.from_records(data,columns=cols)
            #df.to_csv(out_path.joinpath('MetricResults').with_suffix('.csv'))

            t_total1 = time.time()

            log_row = [SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS, str(COMP_NR_SIM),ALPHA]

            # the logger takes it in exactly this order
            for metric in metrics:
                log_row.append(metrics[metric][0])
                log_row.append(round(100 * metrics[metric][0] / MAX_PAIRS, 2))
                log_row.append(round(10000 * metrics[metric][1] / MAX_PAIRS, 4))

            df1 = pd.DataFrame([log_row],columns=new_lex_res.columns)
            new_lex_res = pd.concat([new_lex_res,df1],ignore_index=True)
            break

            #next_index = res_log.index.max() + 1 if not res_log.empty else 0
            #res_log.loc[next_index] = log_row
    #res_log.to_csv(logging_res_path)
    sql_con.insert_records("LexicalResults",new_lex_res,write_index=False)

