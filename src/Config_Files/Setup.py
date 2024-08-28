import datetime
import time

import git

from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.Classes.SimOutlier import SimOutlier,QuantileOutlier
from src.Config_Files.Analysis_Configs import *
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.StructuralSimilarityMetrics.DynamicRecordTupleCount import DynamicRecordTupleCount
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.StructuralSimilarityMetrics.NodeDegree import NodeDegree
from src.LexicalSimilarityMetrics.ISUB import IsubStringMatcher
from src.LexicalSimilarityMetrics.LevenshteinSimilarity import LevenshteinSimilarity
from src.LexicalSimilarityMetrics.JaroWinkler import JaroWinkler


from src.Libraries import ShellLib


#TODO filter input for duplicated atoms

if __name__ == "__main__":
    # TODO Unit_Test_Dyn_Max_Cardinality
    # specify Java-files & Programm Analysis
    db_config = Doop_Gocd_Websocket_Notifier_v1_v4
    program_config = Doop_PointerAnalysis

    GEN_FACTS = False  # if true, run doop again for new fact-gen, otherwise just copy from doop/out
    COMP_MAPPING = True
    RUN_DL = False

    # Fact Creation of Java-Files (or .Jar)
    data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)

    # for collecting results
    global_log = ShellLib.GlobalLogger(data.paths.global_log)
    repo = git.Repo(search_parent_directories=True)
    commit = repo.head.object.hexsha
    date = datetime.datetime.now()

    if GEN_FACTS:
        ShellLib.create_input_facts(db_config, db_config.db1_dir_name, db_config.db1_file_name,
                                    data.db1_original_facts.path)
        ShellLib.create_input_facts(db_config, db_config.db2_dir_name, db_config.db2_file_name,
                                    data.db2_original_facts.path)

    # load facts into data-object
    data.db1_original_facts.read_db_relations()
    data.db2_original_facts.read_db_relations()

    # compute & evaluate equality base line
    if RUN_DL:
        nemo_runtime = ShellLib.chase_nemo(program_config.sep_dl, data.db1_original_facts.path,
                                           data.db1_original_results.path)
        global_log.reasoning_df.loc[len(global_log.reasoning_df)] = [date, commit,
                                                                     db_config.dir_name + "-" + db_config.db1_dir_name,
                                                                     None,
                                                                     program_config.sep_dl.stem] + nemo_runtime

        nemo_runtime = ShellLib.chase_nemo(program_config.sep_dl, data.db2_original_facts.path,
                                           data.db2_original_results.path)
        global_log.reasoning_df.loc[len(global_log.reasoning_df)] = [date, commit,
                                                                     db_config.dir_name + "-" + db_config.db2_dir_name,
                                                                     None, program_config.sep_dl.stem] + nemo_runtime

        data.db1_original_results.read_db_relations()
        data.db2_original_results.read_db_relations()

        reasoning_res = []

    db1_facts = data.db1_original_facts
    db2_facts = data.db2_original_facts

    # add mappings to data
    '''data.add_mapping(MappingContainer(data.paths, "full_expansion", full_expansion_strategy, "term_equality", term_equality))
    data.add_mapping(MappingContainer(data.paths, "full_expansion", full_expansion_strategy, "jaccard_min", jaccard_min))
    data.add_mapping(MappingContainer(data.paths, "full_expansion", full_expansion_strategy, "isub", isub_sequence_matcher))
    data.add_mapping(MappingContainer(data.paths, "full_expansion", full_expansion_strategy, "jaccard+isub",  jaccard_isub_mix))
    '''
    # Set Anchor Quantile
    q_80 = QuantileAnchorTerms(0.80)
    q_90 = QuantileAnchorTerms(0.90)
    q_95 = QuantileAnchorTerms(0.95)
    q_98 = QuantileAnchorTerms(0.98)

    # Set SimOutlier
    sim_outlier = QuantileOutlier()

    # Set Expansion Strategy
    dynamic_iterative_expansion = IterativeAnchorExpansion(q_95, sim_outlier, DYNAMIC=False)
    static_iterative_expansion_80 = IterativeAnchorExpansion(q_80, sim_outlier, DYNAMIC=False)
    static_iterative_expansion_90 = IterativeAnchorExpansion(q_90, sim_outlier, DYNAMIC=False)
    static_iterative_expansion_95 = IterativeAnchorExpansion(q_95, sim_outlier, DYNAMIC=False)
    static_iterative_expansion_98 = IterativeAnchorExpansion(q_98, sim_outlier, DYNAMIC=False)
    dynamic_iterative_expansion_95 = IterativeAnchorExpansion(q_95, sim_outlier, DYNAMIC=True)
    dynamic_iterative_expansion_98 = IterativeAnchorExpansion(q_98, sim_outlier, DYNAMIC=True)

    # Set up Similarity Metrics
    jaccard_index = JaccardIndex()
    dynamic_min_rec_tuples = DynamicRecordTupleCount()
    node_degree = NodeDegree()
    levenshtein_dist = LevenshteinSimilarity()
    isub = IsubStringMatcher()
    jaro_winkler = JaroWinkler()

    # Add combinations as new Mapping Container

    #data.add_mapping(MappingContainer(data.paths, dynamic_iterative_expansion, jaccard_index))
    #data.add_mapping(MappingContainer(data.paths, static_iterative_expansion_80, jaccard_index))
    #data.add_mapping(MappingContainer(data.paths, static_iterative_expansion_90, jaccard_index))
    #data.add_mapping(MappingContainer(data.paths, static_iterative_expansion_95, jaccard_index))
    #data.add_mapping(MappingContainer(data.paths, static_iterative_expansion_98, jaccard_index))
    data.add_mapping(MappingContainer(data.paths, dynamic_iterative_expansion_95, jaccard_index))
    data.add_mapping(MappingContainer(data.paths, dynamic_iterative_expansion_98, jaccard_index))
    #data.add_mapping(MappingContainer(data.paths, "dynamic", iterative_anchor_expansion,dynamic_min_rec_tuples))
    #data.add_mapping(MappingContainer(data.paths, "dynamic", iterative_anchor_expansion,node_degree))

    #data.add_mapping(MappingContainer(data.paths, "dynamic", iterative_anchor_expansion,isub))

    #data.add_mapping(MappingContainer(data.paths, "dynamic", iterative_anchor_expansion,levenshtein_dist))
    #data.add_mapping(MappingContainer(data.paths, "dynamic", iterative_anchor_expansion,jaro_winkler))

    # data.add_mapping(MappingContainer(data.paths, "local_expansion", iterative_anchor_expansion, "isub", isub_sequence_matcher))
    # data.add_mapping(MappingContainer(data.paths,"local_expansion",iterative_anchor_expansion,"jaccard+isub",jaccard_isub_mix))

    eval_tab = PrettyTable()
    eval_tab.field_names = ["Method", "data set", "unique rows DB1", "unique rows DB2", "Common Rows",
                            "overlap in %"]
    eval_tab.add_row(
        ["No mapping", "original facts"] + compute_overlap_dbs(data.db1_original_facts, data.db2_original_facts,
                                                               print_flag=False))

    time_tab = PrettyTable()
    time_tab.field_names = ["MappingContainer", "#blocked Mappings", "# 1:1 Mappings", "#synthetic Terms", "# hub comp.",
                            "uncertain mappings", "# comp. tuples", "comp. tuples in %", "run-time"]

    # iterate through all selected mapping functions
    for mapping in data.mappings:
        print("--------------------------")
        print(mapping.name)
        mapping.initialize_records_terms_db1(data.db1_original_facts)
        mapping.init_records_terms_db2(data.db2_original_facts)
        c_max_tuples = len(mapping.terms_db1) * len(mapping.terms_db2)

        # calculate similarity_matrix & compute maximal mapping from db1_facts to db2_facts
        if COMP_MAPPING:
            t0 = time.time()
            mapping.compute_mapping(db1_facts,db2_facts, program_config.blocked_terms)
            t1 = time.time()
            mapping.db1_renamed_facts.log_db_relations()
            mapping_rt = round(t1 - t0, 4)
        else:
            mapping.read_mapping()
            mapping_rt = 0.0

        nr_1_1_mappings = len(mapping.final_mapping)
        # execute best mapping and create merged database: merge(map(db1_facts), db2_facts) -> merge_db2
        mapping.merge_dbs(mapping.db1_renamed_facts, db2_facts, mapping.db_merged_facts)

        mapping.log_mapping()
        mapping.db_merged_facts.log_db_relations()
        res = count_overlap_merge_db(mapping.db_merged_facts)
        if mapping == data.mappings[-1]:
            eval_tab.add_row([mapping.name, "merged facts"] + res, divider=True)
        else:
            eval_tab.add_row([mapping.name, "merged facts"] + res, divider=False)

        l_blocked_terms = len(program_config.blocked_terms)

        time_tab.add_row(
            [mapping.name, l_blocked_terms, nr_1_1_mappings, mapping.new_term_counter, mapping.c_hub_recomp,
             mapping.c_uncertain_mappings, mapping.c_mappings,
             str(round(mapping.c_mappings * 100 / c_max_tuples, 2)) + "%", mapping_rt])
        if COMP_MAPPING:
            global_log.mapping_df.loc[len(global_log.mapping_df)] = (
                    [date, commit, db_config.full_name, mapping.name, mapping.expansion_strategy.name,
                     mapping.similarity_metric.name, mapping.c_mappings,
                     str(round(mapping.c_mappings * 100 / c_max_tuples, 2)) + "%", nr_1_1_mappings,
                     mapping.new_term_counter, mapping.c_hub_recomp, mapping.c_uncertain_mappings] + res + [mapping_rt])
            print(f"expanded anchor nodes: {mapping.c_anchor_nodes}")
            print(f"accepted mappings: {mapping.c_accepted_anchor_mappings}")

        if RUN_DL:
            # run Nemo-Rules on merged facts (merge_db2 )
            nemo_runtime = ShellLib.chase_nemo(program_config.merge_dl, mapping.db_merged_facts.path,
                                               mapping.db_merged_results.path)

            # Read PA-results
            mapping.db_merged_results.read_db_relations()

            # Apply mapping to merged-result (from db2_facts)
            # mapping.map_df(mapping.db_merged_results, mapping.db1_unravelled_results)
            mapping.unravel_merge_dbs()
            mapping.db1_unravelled_results.log_db_relations()
            mapping.db2_unravelled_results.log_db_relations()

            # check if bijected results correspond to correct results from base
            verify_merge_results(data, mapping)
            overlap = count_overlap_merge_db(mapping.db_merged_results)
            # noinspection PyUnboundLocalVariable
            reasoning_res.append([mapping.name, "merged results"] + overlap)
            # global_log nemo-runtime
            global_log.reasoning_df.loc[len(global_log.reasoning_df)] = [date, commit, db_config.full_name,
                                                                         mapping.name,
                                                                         program_config.merge_dl.stem] + nemo_runtime
            # "Date","SHA","MergeDB","MappingContainer","Expansion","Metric", "Unique Records DB1","Unique Records DB2","Mutual Records","Overlap in %"
            global_log.merge_db_df.loc[len(global_log.merge_db_df)] = [date, commit, db_config.full_name, mapping.name,
                                                                       mapping.expansion_strategy.__name__,
                                                                       mapping.similarity_metric.name] + overlap

    # Evaluation function to analyse if the mapping reduces storage
    print(time_tab)
    if RUN_DL:
        eval_tab.add_row(["No mapping", "original results"] + compute_overlap_dbs(data.db1_original_results,
                                                                                  data.db2_original_results))
        # unfortunately we cant evalute this during the mapping bc. eval_tab should be separated by fact-eval & DL-eval
        eval_tab.add_rows(reasoning_res)

    print(eval_tab)

    # data.log_terms()
    global_log.save_results()