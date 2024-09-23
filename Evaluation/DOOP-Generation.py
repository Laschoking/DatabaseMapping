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
import subprocess


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

def create_dir(file_name,versions,MVN_PATH,db_config_df):
    JAVA_PATH = PathLib.java_source_dir
    dir = JAVA_PATH.joinpath(file_name)
    df_new_params = pd.DataFrame()
    found_id_pair = False

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
        id_db_config = DbConfig(use='structural-evaluation', type='DoopProgramAnalysis', dir_name=file_name,
                                db1_name=version, db2_name=f"{version}_copy")
        id_db_params = pd.Series(id_db_config.get_finger_print())
        data = DataContainerFile.DataContainer(id_db_config.base_output_path, id_db_config.db1_path, id_db_config.db2_path)
        try:
            ShellLib.create_input_facts(db_config=id_db_config, db_version=id_db_config.db1_name,
                                        db_dir_name=id_db_config.dir_name, fact_path=data.db1_original_facts.path,
                                        force_gen=False)

            # Copy files from db1 to the copy at db1_copy
            shutil.copytree(data.db1_original_facts.path,data.db2_original_facts.path,dirs_exist_ok=True)

            # Jars of >20kb are difficult to evaluate exhaustively
            if get_jar_size(jar) > 20:
                continue
            found_id_pair = True
            # Insert combination into DbConfig, if it does not exist there:
            if not is_series_in_df(series=id_db_params, df=db_config_df) and not found_id_pair:
                df_new_params = add_series_to_df(series=id_db_params,df=df_new_params)
                # We only want 1 test-file per library


        except FileNotFoundError:
            print(f"failed to generate facts from jar: {id_db_config.dir_name} version: {id_db_config.db1_name}")

    # Create Pairs of databases for final evaluation
    db_pairs = itertools.combinations(versions,r=2)
    for (db1,db2) in db_pairs:

        db_config = DbConfig(use='eval', type='DoopProgramAnalysis', dir_name=file_name, db1_name=db1, db2_name=db2)
        db_params = pd.Series(db_config.get_finger_print())

        data = DataContainerFile.DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)
        try:
            ShellLib.create_input_facts(db_config=db_config, db_version=db_config.db1_name,
                                        db_dir_name=db_config.dir_name,
                                        fact_path=data.db1_original_facts.path, force_gen=False)
            ShellLib.create_input_facts(db_config=db_config, db_version=db_config.db2_name,
                                        db_dir_name=db_config.dir_name,
                                        fact_path=data.db2_original_facts.path, force_gen=False)

            # TODO order the files s.t. db1 is the smaller one!

            # Insert combination into DbConfig, if it does not exist there:
            if not is_series_in_df(series=db_params, df=db_config_df):
                df_new_params = add_series_to_df(series=db_params,df=df_new_params)

        except FileNotFoundError:
            print(f"failed to generate facts from jar: {db_config.dir_name} version: {db_config.db1_name} or {db_config.db2_name}")

    if not df_new_params.empty:
        sql_con.insert_df('DbConfig', df_new_params,write_index=False)






if __name__ == "__main__":
    MVN_PATH = PathLib.mvn_dir
    jars = retrieve_mvn_jars(MVN_PATH)
    db_config_df = sql_con.get_table('DbConfig')
    for file_name, versions in jars.items():
        create_dir(file_name,versions,MVN_PATH,db_config_df)

