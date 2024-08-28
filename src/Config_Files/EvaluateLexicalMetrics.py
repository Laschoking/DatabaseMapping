from pathlib import Path
import sqlite3
import random
from src.LexicalSimilarityMetrics import CodeBert,ISUB,LevenshteinSimilarity,JaroWinkler

RAW_SQL_PATH = Path("/home/kotname/Documents/Diplom/Evaluation/SCAM2019.sqlite")


def get_fake_tuples(corr_pair, renames):
    fake_pairs = list()
    j = 0
    while j < 10:
        g = random.randint(0, len(renames) - 1)
        fake_name = renames[g][0]
        if fake_name != corr_pair[0]:
            fake_pair = (fake_name, corr_pair[1])
            j += 1
            fake_pairs.append(fake_pair)
    return fake_pairs


def evaluate_sim_metric(metric,corr_pair, fake_pairs):
    #print(metric,corr_pair,fake_pairs)
    corr_sim = metric.compute_lexical_similarity(corr_pair[0],corr_pair[1])
    for fake_pair in fake_pairs:
        incor_sim = metric.compute_lexical_similarity(fake_pair[0],fake_pair[1])
        if incor_sim >= corr_sim:
            return 0
    return 1

if __name__ == "__main__":
    con = sqlite3.connect(RAW_SQL_PATH)
    cur = con.cursor()
    CB = CodeBert.CodeBertSimilarity()
    JW = JaroWinkler.JaroWinkler()
    ISUB = ISUB.IsubStringMatcher()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()

    old_names = set()
    new_names = set()
    metrics = {CB : 0}

    #metrics = {LS :0,CB : 0, JW:0, ISUB : 0}

    query = "SELECT OldName, NewName FROM rename_variable GROUP BY OldName, NewName LIMIT 100;"
    cur.execute(query)
    res = cur.fetchall()

    correct_metric = 0
    for corr_pair in res:
        correct_metric += 1
        fake_tuples = get_fake_tuples(corr_pair, res)
        for metric in metrics:
            metrics[metric] += evaluate_sim_metric(metric,corr_pair,fake_tuples)

    print(metrics)
    print(correct_metric)
    print(len(res))

