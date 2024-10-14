from pathlib import Path
from src.Libraries.PathLib import sql_con,lex_eval_db
import sqlite3
from src.LexicalSimilarityMetrics import ISUB,LevenshteinSimilarity,JaroWinkler,QGram,LCS,Equality,Dice
import pandas as pd
import time
import re
from src.Libraries.SqlConnector import SqlConnector

# This evaluation is based on true facts, that is pairs of renamed variables, classes, methods, and parameters
# The quality of a metric is measured, by how good it can identify the correct pair from a set of fake pairs


# TODO in best case: meld facts in Lexical Results (with 1 Result-record per Metric)

def get_fake_tuples(corr_pair, corr_id, rename_df, NR_FAKE_PAIRS):
    fake_pairs = pd.DataFrame(columns=rename_df.columns)
    i = corr_id
    go_backwards = False
    while len(fake_pairs) < NR_FAKE_PAIRS:
        if not go_backwards:
            i += 1
        else:
            i -= 1
        # Handle end of facts, where sliding window would cause IndexError
        if i >= len(rename_df):
            go_backwards = True
            continue

        next_corr_pair = rename_df.iloc[i,:]
        fake_name1 = next_corr_pair['OldName']
        fake_name2 = next_corr_pair['NewName']
        if fake_name1 != corr_pair['OldName']:
            fake_pairs.loc[len(fake_pairs)] = [fake_name1,corr_pair['NewName']]

        if fake_name2 != corr_pair['NewName']:
            fake_pairs.loc[len(fake_pairs)] = [corr_pair["OldName"],fake_name2]




    return fake_pairs


def evaluate_sim_metric(metric, corr_pair, fake_pairs, parsed_res, COMP_NR_SIM,ALPHA):
    t0 = time.time()
    old_name = corr_pair['OldName']
    new_name = corr_pair['NewName']

    c = 1 # Count how many pairs are evaluated
    # If the numerical metric is used, calculate the lexical metric on the remaining String-Components
    if COMP_NR_SIM:
        old_str, old_nr = parsed_res[old_name]
        new_str, new_nr = parsed_res[new_name]
        corr_sim = ALPHA * metric.compute_lexical_similarity(old_str, new_str) + (1 - ALPHA) * metric.number_similarity(old_nr, new_nr)
    else:
        # If no numerical metric is used, calculate the lexical metric on the full name (incl. numbers)
        corr_sim = metric.compute_lexical_similarity(old_name, new_name)

    for index,fake_pair in fake_pairs.iterrows():
        old_name = fake_pair['OldName']
        new_name = fake_pair['NewName']

        c += 1

        # Same system as for the correct string pair above
        if COMP_NR_SIM:
            old_str, old_nr = parsed_res[old_name]
            new_str, new_nr = parsed_res[new_name]
            incorr_sim = ALPHA * metric.compute_lexical_similarity(old_str, new_str) + (1 - ALPHA) * metric.number_similarity(old_nr, new_nr)

        else:
            incorr_sim = metric.compute_lexical_similarity(old_name, new_name)

        if incorr_sim >= corr_sim:
            t1 = time.time()
            return corr_pair,corr_sim,False, (t1 - t0) / c

    t1 = time.time()
    return corr_pair,corr_sim,True, (t1 - t0) / c


if __name__ == "__main__":
    #############################################################
    # Important parameters:
    SQL_TABLES = ['rename_class']# ,'rename_attribute','rename_variable','rename_method','rename_parameter']
    fake_pairs = [12] #[2,4,6,8,10,12]
    alphas = [0.95] # [0.6,0.8,0.95]
    no_nr_use_combinations = [(fp,False,1) for fp in fake_pairs]
    nr_use_combinations = [(fp,True,alpha) for fp in fake_pairs for alpha in alphas]
    combinations = nr_use_combinations #no_nr_use_combinations + nr_use_combinations
    #############################################################

    lex_data_con = SqlConnector(lex_eval_db)


    EQ = Equality.Equality()
    ISUB = ISUB.IsubStringMatcher()
    JW = JaroWinkler.JaroWinkler()
    LS = LevenshteinSimilarity.LevenshteinSimilarity()
    LCS = LCS.LCS()
    QGRAM2 = QGram.QGram(2)
    DICE2 = Dice.Dice(2)
    metric_objs = [QGRAM2,DICE2] #[EQ,ISUB,JW,LS,LCS,QGRAM2,DICE2]

    compare_QGRAM_DICE = {'OldName' :[],'NewName' :[] ,'Dice':[],'Qgram':[]}
    # This implementation could be improved since we insert several results into one row

    cols = ["resource","nr_pairs","nr_fake_pairs","use_nr_sim","ALPHA"]
    for metric in metric_objs:
        cols.append(metric.name + "_corr_pairs")
        cols.append(metric.name + "_percentage")
        cols.append(metric.name + "_runtime")


    existing_lex_res = sql_con.get_table("LexicalResults")
    new_lex_res = pd.DataFrame(columns=cols)


    for SQL_TABLE in SQL_TABLES:
        print(SQL_TABLE)
        t_total0 = time.time()

        query = f"SELECT OldName, NewName FROM {SQL_TABLE}  GROUP BY OldName, NewName;"
        rename_df = lex_data_con.query_table(query)

        # Adapt the actual size from str_res, because it filters & groups
        MAX_PAIRS = len(rename_df)
        parsed_res = dict()


        for index,(element_name1,element_name2) in rename_df.iterrows():
            red_name1 = element_name1.lower()
            red_name2 = element_name2.lower()

            # Filter both strings for numbers, and process numbers separately
            nrs1 = re.findall(r'\d+', red_name1)
            nrs2 = re.findall(r'\d+', red_name2)
            red_name1 = re.sub(r'\d+', "",red_name1)
            red_name2 = re.sub(r'\d+', "",red_name2)
            parsed_res[element_name1] = (red_name1,nrs1)
            parsed_res[element_name2] = (red_name2,nrs2)

        for (NR_FAKE_PAIRS, USE_NR_SIM, ALPHA) in combinations:
            new_res = pd.Series({'resource' : SQL_TABLE, 'nr_pairs' : MAX_PAIRS, 'nr_fake_pairs' : NR_FAKE_PAIRS,
                                 'use_nr_sim' : str(USE_NR_SIM),'ALPHA' : ALPHA})
            # Convert the bool USE_NR_SIM to string, because comparison with res_df is difficult due to facts-type issues

            # Skip this combination, if it was already computed for the database
            reduced_df = existing_lex_res[['resource','nr_pairs','nr_fake_pairs','use_nr_sim','ALPHA']]
            #if reduced_df.eq(new_res).all(axis=1).any():
            #    continue
            print(f"calculate combination:{SQL_TABLE,MAX_PAIRS,NR_FAKE_PAIRS,str(USE_NR_SIM),ALPHA}")


            metric_res = {obj: {'corr_pairs' : 0, 'rt' : 0} for obj in metric_objs}

            log_it = 0
            for index,corr_pair in rename_df.iterrows():
                t1,n1 = parsed_res[corr_pair.at['OldName']]
                t2, n2 = parsed_res[corr_pair.at['NewName']]
                compare_QGRAM_DICE['OldName'].append(corr_pair.at['OldName'])
                compare_QGRAM_DICE['NewName'].append(corr_pair.at['NewName'])

                log_it += 1
                if log_it % 10000 == 0:
                    print(f"evaluated {log_it} pairs")

                fake_tuples = get_fake_tuples(corr_pair,log_it, rename_df, NR_FAKE_PAIRS)
                for metric in metric_res.keys():
                    corr_pair,corr_sim,det_mapping,rt = evaluate_sim_metric(metric, corr_pair, fake_tuples, parsed_res, USE_NR_SIM, ALPHA)
                    metric_res[metric]['rt'] += rt
                    if det_mapping:
                        metric_res[metric]['corr_pairs'] += 1
                    if metric == DICE2:
                        compare_QGRAM_DICE['Dice'].append(det_mapping)
                    if metric == QGRAM2:
                        compare_QGRAM_DICE['Qgram'].append(det_mapping)
            t_total1 = time.time()

            df = pd.DataFrame(compare_QGRAM_DICE)
            df['count'] = df[['Dice','Qgram']].sum(axis=1)
            df = df[df['count'] == 1]
            df.to_csv("Comparision_Dice_Qgram.tsv",sep='\t')


            break
