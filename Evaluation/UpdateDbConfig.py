import time
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df,get_mapping_id,skip_current_computation
from src.Libraries.PathLib import sql_con
from src.Classes.DataContainerFile import DataContainer
from src.Classes.MappingContainerFile import MappingContainer
from src.Classes.QuantileAnchorTerms import QuantileAnchorTerms
from src.Config_Files.Analysis_Configs import *
from src.Libraries.EvaluateMappings import compute_overlap_dbs
from src.ExpansionStrategies.IterativeAnchorExpansion import IterativeAnchorExpansion
from src.Libraries.EvaluateMappings import *
from src.StructuralSimilarityMetrics.JaccardIndex import JaccardIndex
from src.LexicalSimilarityMetrics.Dice import Dice
import itertools
import gc
import pandas as pd



if __name__ == "__main__":
    """ Initially the number of equal-facts was not logged in DbConfig """
    """ -> Update the missing information, by reading all configs & update db"""
    # Retrieve relevant data from Database
    db_config_df = sql_con.query_table(query="SELECT * FROM DbConfig WHERE use=\'cross-expansion\';")
    new_db_config_df = pd.DataFrame()

    # Iterate through all relevant database pairs that are used for the structural-evaluation
    for curr_id, db_pair in db_config_df.iterrows():
        db_config = DbConfig(*db_pair[['use','type','file_name','db1','db2']])
        data = DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)

        # Load facts into the data structure
        try:
            db1_facts = data.db1_original_facts.read_db_relations()
            db2_facts = data.db2_original_facts.read_db_relations()
            facts = pd.Series(compute_overlap_dbs(db1=db1_facts, db2=db2_facts, print_flag=False))
            db_config_df.at[curr_id, 'nr_equal_facts'] = facts['common_records']
            db_config_df.at[curr_id, 'equal_facts_perc'] = facts['overlap_perc']
        except FileNotFoundError:
            print(f"Directory is missing, skip {db_pair['db_config_id']}")


    sql_con.insert_df(table="DbConfig_New",df=db_config_df,write_index=False)
