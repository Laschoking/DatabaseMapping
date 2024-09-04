from pathlib import Path
import sqlite3
import random
from src.LexicalSimilarityMetrics import CodeBert,ISUB,LevenshteinSimilarity,JaroWinkler
import pandas as pd
import time
import re
RAW_SQL_PATH = Path("/home/kotname/Documents/Diplom/Evaluation/SCAM2019.sqlite")


def get_fake_tuples(corr_pair, renames,nr_fake_tuples):
    fake_pairs = list()

    j = 0
    # Get 10 fake pairs, with (..., corr_term2)
    while j < nr_fake_tuples / 2:
        g = random.randint(0, len(renames) - 1)
        fake_name = renames[g][0]
        if fake_name != corr_pair[1] and fake_name != corr_pair[0]:
            fake_pair = (fake_name, corr_pair[1])
            j += 1
            fake_pairs.append(fake_pair)

    # Get 10 fake pairs, with (corr_term1,...)
    j = 0
    while j < nr_fake_tuples / 2:
        g = random.randint(0, len(renames) - 1)
        fake_name = renames[g][1]
        if fake_name != corr_pair[1] and fake_name != corr_pair[0]:
            fake_pair = (corr_pair[0],fake_name)
            j += 1
            fake_pairs.append(fake_pair)

    return fake_pairs


def evaluate_sim_metric(metric, corr_pair, fake_pairs, parsed_res, COMP_NR_SIM,ALPHA):
    t0 = time.time()
    corr_name1,corr_nr1 = parsed_res[corr_pair[0]]
    corr_name2,corr_nr2 = parsed_res[corr_pair[1]]
    c = 1 # Count how many pairs are evaluated

    corr_sim = metric.compute_lexical_similarity(corr_name1,corr_name2) #* metric.number_similarity(corr_nr1,corr_nr2)
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
            #print(f"correct: {corr_pair} with {corr_sim} wrong: {fake_pair} with {incor_sim}")
            t1 = time.time()
            return corr_pair,corr_sim,fake_pair,incorr_sim, (t1 - t0) / c

    t1 = time.time()
    return corr_pair,corr_sim,None,None, (t1 - t0) / c


if __name__ == "__main__":
    #############################################################
    # Important constants:
    MAX_PAIRS = 1500
    NR_FAKE_PAIRS = 40
    COMP_NR_SIM = False
    ALPHA = 0.95
    #############################################################



    con = sqlite3.connect(RAW_SQL_PATH)
    out_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Evaluation/LexicalMetrics")
    cur = con.cursor()
    CB = CodeBert.CodeBertSimilarity()
    JW = JaroWinkler.JaroWinkler()
    ISUB = ISUB.IsubStringMatcher()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()
    logging_res_path = out_path.joinpath("Log_Results.csv")
    if logging_res_path.exists():
            res_log = pd.read_csv(logging_res_path, sep=',', index_col=0)
    else:
        res_log = pd.DataFrame(columns=["Pairs","fake pairs","nr_sim","ALPHA","JW-Corrrect","JW-Perc.","JW-rt per 10000","ISUB-Corrrect","ISUB-Perc.","ISUB-rt per 10000","LS-Corrrect","LS-Perc.","LS-rt per 10000"])

    t_total0 = time.time()
    data = []
    old_names = set()
    new_names = set()
    metrics = {ISUB : [0,0], LS : [0,0], JW : [0,0]}
    #metrics = {ISUB : [0,ISUB_df]}
    # TODO: Kick out Bert, Improve other Lexical Metrics by implementing a number variant

    query = f"SELECT OldName, NewName FROM rename_variable WHERE length(OldName) > 3 and length(NewName) > 3 GROUP BY OldName, NewName LIMIT {MAX_PAIRS};"
    cur.execute(query)
    res = cur.fetchall()
    #res = [('AREA_LOW', 'VERTICAL_HEIGHT_LOW'),('AREA_HIGH', 'VERTICAL_WIDTH_HIGH'),('AREA_HIGH', 'ARGS90'),('AND73_tree', 'VERTICAL_HEIGHT_LOW'),('AREA_HIGH', 'VERTICAL_HEIGHT_LOW')]
    parsed_res = dict()


    for (term_name1,term_name2) in res:
        red_name1 = term_name1.lower()
        red_name2 = term_name2.lower()

        # Filter both strings for numbers, and process numbers separately
        nrs1 = re.findall(r'\d+', red_name1)
        nrs2 = re.findall(r'\d+', red_name2)
        red_name1 = re.sub(r'\d+', " ",red_name1)
        red_name2 = re.sub(r'\d+', " ",red_name2)
        parsed_res[term_name1] = (red_name1,nrs1)
        parsed_res[term_name2] = (red_name2,nrs2)

    log_it = 0
    for corr_pair in res:
        t1,n1 = parsed_res[corr_pair[0]]
        t2, n2 = parsed_res[corr_pair[1]]
        '''
        print(corr_pair)
        print(f"JW: {JW.compute_lexical_similarity(t1,t2)}")
        print(f"ISUB: {ISUB.compute_lexical_similarity(t1, t2)}")
        print(f"LS: {LS.compute_lexical_similarity(t1, t2)}")
        print(f"nr sim: {JW.number_similarity(n1,n2)}")
        print("--------------------")

        '''
        log_it += 1
        if log_it % 5000 == 0:
            print(f"evaluated {log_it} pairs")
        row = [corr_pair,0]
        fake_tuples = get_fake_tuples(corr_pair, res, NR_FAKE_PAIRS)
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

    df = pd.DataFrame.from_records(data,columns=cols)
    df.to_csv(out_path.joinpath('MetricResults').with_suffix('.csv'))

    for metric,(corr_count,rt) in metrics.items():
        print(f"Metric: {metric.name}, Runtime per 10000 pairs: {round(10000 * rt / MAX_PAIRS, 4)}s | Correct: {corr_count} ({round(100 * corr_count / MAX_PAIRS, 2)})%")

    t_total1 = time.time()
    print(f"computation time: {round(t_total1 - t_total0, 4)}s | pairs: {MAX_PAIRS} | fake_tuples: {NR_FAKE_PAIRS}")

    log_row = [MAX_PAIRS,NR_FAKE_PAIRS, COMP_NR_SIM,ALPHA]
    # the logger takes it in exactly this order
    for metric in [JW,ISUB,LS]:
        log_row.append(metrics[metric][0])
        log_row.append(round(100 * metrics[metric][0] / MAX_PAIRS, 2))
        log_row.append(round(10000 * metrics[metric][1] / MAX_PAIRS, 4))
    next_index = res_log.index.max() + 1 if not res_log.empty else 0
    res_log.loc[next_index] = log_row
    res_log.to_csv(logging_res_path)
