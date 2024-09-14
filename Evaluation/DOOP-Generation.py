import re
import os
from pathlib import Path
import shutil
import subprocess
from prettytable import PrettyTable
import pandas as pd
import re
import shutil
import itertools
from src.Config_Files.Analysis_Configs import DbConfig
from src.Libraries import ShellLib,PathLib
from src.Classes import DataContainerFile

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

def create_dir(file_name,versions,MVN_PATH):
    JAVA_PATH = PathLib.java_source_dir
    dir = JAVA_PATH.joinpath(file_name)
    df_new_params = []

    if not dir.is_dir():
        dir.mkdir()
    for version in versions:
        v_dir = dir.joinpath(version)
        if not v_dir.is_dir():
            v_dir.mkdir()
        jar = MVN_PATH.joinpath(file_name + version + ".jar")
        shutil.copy(jar,v_dir)

        # Create Pairs of identical databased for structural evaluation
        id_db_params = ['structural-evaluation',"DoopProgramAnalysis",file_name,version,version + "_copy",None,None]
        id_db_config = DbConfig(*id_db_params)
        id_db_params.insert(0, id_db_config.full_name)

        data = DataContainerFile.DataContainer(id_db_config.base_output_path, id_db_config.db1_path, id_db_config.db2_path)
        try:
            ShellLib.create_input_facts(id_db_config, id_db_config.db1_dir_name, id_db_config.db1_file_name,data.db1_original_facts.path,force_gen=False)
            # Copy files from db1 to the copy at db1_copy
            shutil.copytree(data.db1_original_facts.path,data.db2_original_facts.path,dirs_exist_ok=True)
            if id_db_config.full_name in PathLib.db_config_df.index:
                PathLib.db_config_df.drop(id_db_config.full_name)
            df_new_params.append(id_db_params)
        except FileNotFoundError:
            print(f"failed to generate facts from jar: {id_db_config.dir_name} version: {id_db_config.db1_dir_name}")


    # Create Pairs of databases for final evaluation
    db_pairs = itertools.combinations(versions,r=2)
    for (db1,db2) in db_pairs:
        db_params = ['eval',"DoopProgramAnalysis",file_name,db1,db2,None,None]
        db_config = DbConfig(*db_params)
        db_params.insert(0,db_config.full_name)
        data = DataContainerFile.DataContainer(db_config.base_output_path, db_config.db1_path, db_config.db2_path)
        try:
            ShellLib.create_input_facts(db_config, db_config.db1_dir_name, db_config.db1_file_name,data.db1_original_facts.path,force_gen=False)
            ShellLib.create_input_facts(db_config, db_config.db2_dir_name, db_config.db2_file_name,data.db2_original_facts.path,force_gen=False)

            # TODO order the files s.t. db1 is the smaller one!

            # Remove old entry if we insert a record with identical Identifier
            if db_config.full_name in PathLib.db_config_df.index:
                PathLib.db_config_df.drop(db_config.full_name)

            df_new_params.append(db_params)
        except FileNotFoundError:
            print(f"failed to generate facts from jar: {db_config.dir_name} version: {db_config.db1_dir_name} or {db_config.db2_dir_name}")

    df = pd.DataFrame.from_records(df_new_params,columns=["DbConfig_Id","Use","Type","Folder","Db1","Db2","Db1_Name","Db2_Name"],index="DbConfig_Id")
    PathLib.db_config_df = pd.concat([PathLib.db_config_df,df],axis=0)







if __name__ == "__main__":
    MVN_PATH = PathLib.mvn_dir
    jars = retrieve_mvn_jars(MVN_PATH)
    for file_name, versions in jars.items():
        create_dir(file_name,versions,MVN_PATH)
    PathLib.db_config_df.to_csv(PathLib.db_configs)

