from pathlib import Path
import sqlite3
import random
from src.LexicalSimilarityMetrics import CodeBert,ISUB,LevenshteinSimilarity,JaroWinkler
import pandas as pd
import time
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


def evaluate_sim_metric(metric,corr_pair, fake_pairs):
    t0 = time.time()
    corr_sim = metric.compute_lexical_similarity(corr_pair[0],corr_pair[1])
    c = 1 # Count how many pairs are evaluated

    for fake_pair in fake_pairs:
        c += 1
        incorr_sim = metric.compute_lexical_similarity(fake_pair[0],fake_pair[1])
        if incorr_sim >= corr_sim:
            #print(f"correct: {corr_pair} with {corr_sim} wrong: {fake_pair} with {incor_sim}")
            t1 = time.time()
            return corr_pair,corr_sim,fake_pair,incorr_sim, t1 - t0

    t1 = time.time()
    return corr_pair,corr_sim,None,None, t1 - t0


if __name__ == "__main__":
    con = sqlite3.connect(RAW_SQL_PATH)
    out_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Evaluation/LexicalMetrics")
    cur = con.cursor()
    CB = CodeBert.CodeBertSimilarity()
    JW = JaroWinkler.JaroWinkler()
    ISUB = ISUB.IsubStringMatcher()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()

    t_total0 = time.time()
    data = []
    old_names = set()
    new_names = set()
    metrics = {ISUB : [0,0], LS : [0,0], JW : [0,0]}
    #metrics = {ISUB : [0,ISUB_df]}
    # TODO: Kick out Bert, Improve other Lexical Metrics by implementing a number variant
    max_pairs = 1000
    nr_fake_tuples = 20
    query = f"SELECT OldName, NewName FROM rename_variable WHERE length(OldName) > 3 and length(NewName) > 3 GROUP BY OldName, NewName LIMIT {max_pairs};"
    cur.execute(query)
    res = cur.fetchall()

    log_it = 0
    for corr_pair in res:
        log_it += 1
        if log_it % 500 == 0:
            print(f"evaluated {log_it} pairs")
        row = [corr_pair,0]
        fake_tuples = get_fake_tuples(corr_pair, res,nr_fake_tuples)
        for metric in metrics.keys():
            corr_pair,corr_sim,fake_pair,incorr_sim,rt = evaluate_sim_metric(metric,corr_pair,fake_tuples)
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
        print(f"Metric: {metric.name}, Runtime: {round(rt,4)}s | Correct: {corr_count} ({round(100 * corr_count / max_pairs,2)})%")

    t_total1 = time.time()
    print(f"computation time: {round(t_total1 - t_total0, 4)}s | pairs: {max_pairs} | fake_tuples: {nr_fake_tuples}")

