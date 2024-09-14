# This file defines all necessary paths for the comparison pipeline
from pathlib import Path
from enum import Enum
import pandas as pd
from src.Libraries import SqlConnector
# create path to doop base and its output dir
DOOP_BASE = Path("/home/kotname/Documents/Diplom/Code/doop/")
DOOP_OUT = Path("/home/kotname/Documents/Diplom/Code/doop/out/")
NEMO_ENGINE = Path("/home/kotname/Documents/Diplom/Code/nemo/nemo")


# this path will be the base for all of our file manipulation
base_diff_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping")
datalog_programs_path = base_diff_path.joinpath("Datalog-Programs")
base_out_path = base_diff_path.joinpath("out")
java_source_dir = base_diff_path.joinpath("Java-Programs")
eval_dir = base_diff_path.joinpath("Evaluation")
mvn_dir = eval_dir.joinpath("mvn_libs")
lex_eval_db = eval_dir.joinpath("SCAM2019.sqlite")
eval_db_path = eval_dir.joinpath('Evaluation.db')
sql_con = SqlConnector.SqlConnector(eval_db_path)

#db_configs = base_diff_path.joinpath("src").joinpath("Config_Files").joinpath("DbConfig.csv")
#db_config_df = pd.read_csv(db_configs,index_col='DbConfig_Id')


SOUFFLE_ANALYSIS_BASE = Path("/home/kotname/Documents/Diplom/Code/ex_souffle/Analysis")
DOOP_PA_BASE = Path("/home/kotname/Documents/Diplom/Code/DiffAnalysis/Datalog-Programs/DoopProgramAnalysis")
SYNTHETIC_EXAMPLES_BASE = Path("/home/kotname/Documents/Diplom/Code/DiffAnalysis/Datalog-Programs/SouffleSynthetic")
base_dir = "/out/"

Engine = Enum("Engine", ["SOUFFLE", "NEMO"])
NR_LEFT = 1
NR_RIGHT = 10
NR_TARGET = 0