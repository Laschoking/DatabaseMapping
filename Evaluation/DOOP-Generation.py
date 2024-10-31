import re
import os
from pathlib import Path
import shutil
import subprocess
from prettytable import PrettyTable
from src.Libraries.PandasUtility import is_series_in_df,add_series_to_df
import pandas as pd
import re
import shutil
import itertools
from src.Config_Files.Analysis_Configs import DbConfig
from src.Libraries import ShellLib,PathLib
from src.Classes import DataContainerFile
from src.Libraries.PathLib import sql_con
from src.Libraries.EvaluateMappings import compute_overlap_dbs

import subprocess

""" This generation file reads all jars in the directory maven_files, and runs doop on the to create databases 
    the databases are saved under out, and prepared for the pairwise matching process """


def get_jar_size(jar_file_path):
    if os.path.isfile(jar_file_path):
        # Get the size of the JAR file in bytes
        file_size = os.path.getsize(jar_file_path)

        # Convert the size to more readable formats (KB, MB)
        file_size_kb = file_size / 1024
        print(jar_file_path,file_size_kb)
        return file_size_kb


def retrieve_mvn_jars(MVN_PATH):
    jars = dict()
    for file in MVN_PATH.glob("*.jar"):
        match = re.search(r'\d',file.stem)
        file_name = file.stem[:match.start()]
        version = file.stem[match.start():]
        jars.setdefault(file_name,[]).append(version)

    # Make sure, that the versions are in an ascending order
    for file_name in jars.keys():
        jars[file_name].sort()

    return jars

def create_dir(file_name,versions,MVN_PATH,db_config_df,db_finger_print_df):
    JAVA_PATH = PathLib.java_source_dir
    dir = JAVA_PATH.joinpath(file_name)
    new_pair_db_df = pd.DataFrame()
    new_single_db_df = pd.DataFrame()


    if not dir.is_dir():
        dir.mkdir()
    for version in versions:
        v_dir = dir.joinpath(version)
        if not v_dir.is_dir():
            v_dir.mkdir()
        # Copy jars to Java-Programs
        jar = MVN_PATH.joinpath(file_name + version + ".jar")
        shutil.copy(jar,v_dir.joinpath(version + ".jar"))

        # Create Pairs of identical databased for structural evaluation
        id_db_config = DbConfig(use='structural-evaluation', type='DoopProgramAnalysis', file_name=file_name,
                                db1_name=version, db2_name=f"{version}_copy")
        pair_db_ser = pd.Series(id_db_config.get_finger_print())
        data = DataContainerFile.OriginalFactsContainer(id_db_config.base_output_path, id_db_config.db1_path, id_db_config.db2_path)
        try:
            ShellLib.create_input_facts(db_config=id_db_config, db_version=id_db_config.db1_name,
                                        db_dir_name=id_db_config.file_name, fact_path=data.db1_original_facts.path,
                                        force_gen=False)

            # Copy files from db1 to the copy at db1_copy
            shutil.copytree(data.db1_original_facts.path,data.db2_original_facts.path,dirs_exist_ok=True)

            db1_facts = data.db1_original_facts.read_db_relations()
            single_db_ser = db1_facts.get_nr_facts_elements()
            single_db_ser['file_name'] = pair_db_ser['file_name']
            single_db_ser['version'] = pair_db_ser['db1']

            # Insert the characteristics (nr_of_facts, nr_of_elements) of the database into a DB for single instances
            if not is_series_in_df(series=single_db_ser, df=db_finger_print_df):
                new_single_db_df = add_series_to_df(series=single_db_ser,df=new_single_db_df)

            # Insert combination of (db1,db1_copy) into DbConfig for structural Evaluation
            # Jars of >20kb are difficult to evaluate exhaustively
            #if get_jar_size(jar) < 20 and not is_series_in_df(series=pair_db_ser, df=db_config_df):
            #    new_pair_db_df = add_series_to_df(series=pair_db_ser,df=new_pair_db_df)
                # We only want 1 test-file per library


        except FileNotFoundError:
            print(f"failed to generate facts from jar: {id_db_config.file_name} version: {id_db_config.db1_name}")

    # Create Pairs of databases for final evaluation
    db_pairs = itertools.combinations(versions,r=2)
    for (db1,db2) in db_pairs:

        db_config = DbConfig(use='reasoning-evaluation', type='DoopProgramAnalysis', file_name=file_name, db1_name=db1, db2_name=db2)
        db_params = pd.Series(db_config.get_finger_print())

        data = DataContainerFile.OriginalFactsContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)
        try:
            ShellLib.create_input_facts(db_config=db_config, db_version=db_config.db1_name,
                                        db_dir_name=db_config.file_name,
                                        fact_path=data.db1_original_facts.path, force_gen=False)
            ShellLib.create_input_facts(db_config=db_config, db_version=db_config.db2_name,
                                        db_dir_name=db_config.file_name,
                                        fact_path=data.db2_original_facts.path, force_gen=False)

            db1_facts = data.db1_original_facts.read_db_relations()
            db2_facts = data.db2_original_facts.read_db_relations()
            facts = compute_overlap_dbs(db1=db1_facts, db2=db2_facts, print_flag=False)
            db_params['nr_equal_facts'] = int(facts['common_facts'])
            db_params['equal_facts_perc'] = round(facts['overlap_perc'],2)

            # Insert combination into DbConfig, if it does not exist there:
            if not is_series_in_df(series=db_params, df=db_config_df):
                new_pair_db_df = add_series_to_df(series=db_params,df=new_pair_db_df)

        except FileNotFoundError:
            print(f"failed to generate facts from jar: {db_config.file_name} version: {db_config.db1_name} or {db_config.db2_name}")

    # Insert background information about each database version
    if not new_single_db_df.empty:
        sql_con.insert_df('DbFingerPrint2', new_single_db_df)

    # Insert information about a database pair (that is the input to the mapping_func process)
    if not new_pair_db_df.empty:
        sql_con.insert_df('DbConfig', new_pair_db_df)




if __name__ == "__main__":
    MVN_PATH = PathLib.mvn_dir
    jars = retrieve_mvn_jars(MVN_PATH)
    db_config_df = sql_con.get_table('DbConfig')
    #db_finger_print_df = sql_con.get_table('DbFingerPrint1')
    for file_name, versions in jars.items():
        db_finger_print_df = pd.DataFrame()
        create_dir(file_name,versions,MVN_PATH,db_config_df,db_finger_print_df)

