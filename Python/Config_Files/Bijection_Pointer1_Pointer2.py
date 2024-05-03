from Python.Libraries.Shell_Lib import *
from Python.Libraries.Merge_Lib import *
from Python.Config_Files.Analysis_Configs import *

if __name__ == "__main__":
    db_config = PointerAnalysis12_Config
    pa_sep = analyses["nemo_PA_sep"]
    pa_merge = analyses["nemo_PA_merge_end_fold"]
    # Fact Creation & read into data
    data = Data(db_config.db1_path,db_config.db2_path)
    create_facts(db_config, data.db1_facts.path, data.db2_facts.path)

    data.db1_facts.read_directory()
    data.db2_facts.read_directory()
    merge_baseline_facts(data)

    # calculate the bijection & write merged fact-files
    data = forward_bijection(data)
    data.db2_merge_facts_bij.write_data_to_file()

    pa_runtime = []
    # Run Program Analysis on separate fact-files (as separate_base)
    pa_runtime.append(run_single_pa(pa_sep, data.db1_facts.path, data.db1_pa_base.path))
    pa_runtime.append(run_single_pa(pa_sep, data.db2_facts.path, data.db2_pa_base.path))

    # Run Program Analysis on merged fact-files (for base & bij_fact database)
    pa_runtime.append(run_single_pa(pa_merge, data.db2_merge_facts_base.path, data.db2_merge_pa_base.path))
    pa_runtime.append(run_single_pa(pa_merge, data.db2_merge_facts_bij.path, data.db2_merge_pa_bij.path))
    print_nemo_runtime(pa_runtime)

    # Read PA-results
    data.db1_pa_base.read_directory()
    data.db2_pa_base.read_directory()
    data.db2_merge_pa_bij.read_directory()
    data.db2_merge_pa_base.read_directory()

    # Apply bijection to merged-result (from db2)
    reverse_bijection_on_pa(data.db2_merge_pa_bij, data.db1_pa_inv_bij, data.bijection, 1)
    data.db1_pa_inv_bij.write_data_to_file()

    # check if bijected results correspond to correct results from base
    check_data_correctness(data)

    # Evaluation function to analyse if the bijection reduces storage
    evaluate_bijection_overlap(data)

