from pathlib import Path
import sqlite3
import random
from src.LexicalSimilarityMetrics import ISUB,LevenshteinSimilarity,JaroWinkler,QGram,LCS,Equality,Dice
import pandas as pd
import time
import re
import numpy as np

def get_fake_tuples(corr_pair, corr_id, renames, NR_FAKE_PAIRS):
    fake_pairs = []
    i = corr_id
    go_backwards = False
    while len(fake_pairs) < NR_FAKE_PAIRS:
        if not go_backwards:
            i += 1
        else:
            i -= 1
        # Handle end of data, where sliding window would cause IndexError
        if i >= len(renames):
            go_backwards = True
            continue

        next_corr_pair = renames[i]
        fake_name1 = next_corr_pair[0]
        fake_name2 = next_corr_pair[1]
        if fake_name1 != corr_pair[0]:
            fake_pairs.append((fake_name1,corr_pair[1]))

        if fake_name2 != corr_pair[1]:
            fake_pairs.append((corr_pair[0],fake_name2))



    return fake_pairs


def evaluate_sim_metric(metric, corr_pair, fake_pairs, parsed_res, COMP_NR_SIM,ALPHA):
    t0 = time.time()
    corr_name1,corr_nr1 = parsed_res[corr_pair[0]]
    corr_name2,corr_nr2 = parsed_res[corr_pair[1]]
    c = 1 # Count how many pairs are evaluated

    corr_sim = metric.compute_lexical_similarity(corr_name1,corr_name2)
    if COMP_NR_SIM:
        corr_sim = ALPHA * corr_sim + (1 - ALPHA) * metric.number_similarity(corr_nr1, corr_nr2)

    for fake_pair in fake_pairs:
        name1, nr1 = parsed_res[fake_pair[0]]
        name2, nr2 = parsed_res[fake_pair[1]]
        c += 1
        incorr_sim = metric.compute_lexical_similarity(name1,name2)
        if COMP_NR_SIM:
            incorr_sim = ALPHA * incorr_sim + (1 - ALPHA) * metric.number_similarity(nr1,nr2)

        if incorr_sim >= corr_sim:
            t1 = time.time()
            return corr_pair,corr_sim,fake_pair,incorr_sim, (t1 - t0) / c

    t1 = time.time()
    return corr_pair,corr_sim,None,None, (t1 - t0) / c


if __name__ == "__main__":
    #############################################################
    # Important constants:
    #MAX_PAIRS = 100000
    SQL_TABLES = ['rename_attribute','rename_variable','rename_class','rename_method','rename_parameter']
    #NR_FAKE_PAIRS = 2
    #COMP_NR_SIM = True
    fake_pairs =[2,4,6,8,10,12]
    alphas = [0.6,0.8,0.95]
    no_nr_use_combinations = [(fp,False,1) for fp in fake_pairs]
    nr_use_combinations = [(fp,True,alpha) for fp in fake_pairs for alpha in alphas]
    combinations = no_nr_use_combinations + nr_use_combinations

    #############################################################

    RAW_SQL_PATH = Path("/home/kotname/Documents/Diplom/Evaluation/SCAM2019.sqlite")
    con = sqlite3.connect(RAW_SQL_PATH)
    out_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Evaluation/LexicalMetrics")
    cur = con.cursor()

    EQ = Equality.Equality()
    ISUB = ISUB.IsubStringMatcher()
    JW = JaroWinkler.JaroWinkler()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()
    LCS = LCS.LCS()
    QGRAM2 = QGram.QGram(2)
    DICE2 = Dice.Dice(2)
    metric_objs = [EQ,ISUB,JW,LS,LCS,QGRAM2,DICE2]


    logging_res_path = out_path.joinpath("Lexical_Results.csv")
    if logging_res_path.exists():
            res_log = pd.read_csv(logging_res_path, sep=',', index_col=0)
    else:
        cols = ["resource","nr_pairs","nr_fake_pairs","use_nr_sim","ALPHA"]
        for metric in metric_objs:
            cols.append(metric.name + "_corr_pairs")
            cols.append(metric.name + "_percentage")
            cols.append(metric.name + "_runtime") # Runtime is for 10000 computed pairs
        res_log = pd.DataFrame(columns=cols)

    for SQL_TABLE in SQL_TABLES:
        print(SQL_TABLE)
        t_total0 = time.time()
        data = []
        old_names = set()
        new_names = set()

        query = f"SELECT OldName, NewName FROM {SQL_TABLE}  GROUP BY OldName, NewName ;"
        cur.execute(query)
        res = cur.fetchall()

        # Adapt the actual size from res, because it filters & groups
        MAX_PAIRS = len(res)
        parsed_res = dict()


        for (term_name1,term_name2) in res:
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
            if (res_log[res_log.columns[:5]] == [SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS,COMP_NR_SIM,ALPHA]).all(axis=1).any():
                continue
            print(f"calculate combination:{SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS,COMP_NR_SIM,ALPHA}")


            metrics = {obj: [0, 0] for obj in metric_objs}

            log_it = 0
            for corr_pair in res:
                t1,n1 = parsed_res[corr_pair[0]]
                t2, n2 = parsed_res[corr_pair[1]]

                log_it += 1
                if log_it % 10000 == 0:
                    print(f"evaluated {log_it} pairs")
                row = [corr_pair,0]
                fake_tuples = get_fake_tuples(corr_pair,log_it, res, NR_FAKE_PAIRS)
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

            #for metric,(corr_count,rt) in metrics.items():
            #    print(f"Metric: {metric.name}, Runtime per 10000 pairs: {round(10000 * rt / MAX_PAIRS, 4)}s | Correct: {corr_count} ({round(100 * corr_count / MAX_PAIRS, 2)})%")
            #    print(f"Metric: {metric.name}   {corr_count,round(100 * corr_count / MAX_PAIRS, 2),round(10000 * rt / MAX_PAIRS, 4)}, ")

            t_total1 = time.time()

            log_row = [SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS, COMP_NR_SIM,ALPHA]

            # the logger takes it in exactly this order
            for metric in metrics:
                log_row.append(metrics[metric][0])
                log_row.append(round(100 * metrics[metric][0] / MAX_PAIRS, 2))
                log_row.append(round(10000 * metrics[metric][1] / MAX_PAIRS, 4))

            next_index = res_log.index.max() + 1 if not res_log.empty else 0
            res_log.loc[next_index] = log_row
    res_log.to_csv(logging_res_path)

