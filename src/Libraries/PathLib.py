
from pathlib import Path
from enum import Enum
import pandas as pd
from src.Libraries import SqlConnector

""" Define the  directory structure of this project """

# create path to doop and its output dir and to the Nemo-Engine
# INSERT YOUR OWN PATHS HERE
DOOP_BASE = Path("/home/kotname/Documents/Diplom/Code/doop/")
DOOP_OUT = Path("/home/kotname/Documents/Diplom/Code/doop/out/")

# from this path we will call $NEMO_ENGINE/target/release/nmo
NEMO_ENGINE = Path("/home/kotname/Documents/Diplom/Code/nemo/nemo")


# This path will be the foundation of this project
base_diff_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping")

# Setup internal structure
datalog_programs_path = base_diff_path.joinpath("Datalog-Programs")
base_out_path = base_diff_path.joinpath("out")
java_source_dir = base_diff_path.joinpath("Java-Programs")
eval_dir = base_diff_path.joinpath("Evaluation")
mvn_dir = eval_dir.joinpath("mvn_libs")
lex_eval_db = eval_dir.joinpath("SCAM2019.sqlite")
eval_db_path = eval_dir.joinpath('Evaluation.db')
sql_con = SqlConnector.SqlConnector(eval_db_path)


base_dir = "/out/"

Engine = Enum("Engine", ["SOUFFLE", "NEMO"])

# The 3 identifiers that are added to the merged databases
ID_BOTH = 0
ID_LEFT = 1
ID_RIGHT = 2



