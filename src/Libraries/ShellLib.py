import re
import os
from pathlib import Path
from src.Libraries import PathLib
import shutil
import subprocess
from prettytable import PrettyTable
import pandas as pd
from sqlalchemy import create_engine,text

# Shell commands
def clear_directory(directory):
    if directory.exists():
        shutil.rmtree(str(directory))
    os.system("mkdir -p " + str(directory))

def clear_file(file_path):
    if file_path.exists():
        os.remove(file_path)


def init_synth_souffle_database(db_config, db_name, fact_path,force_gen):
    gen_facts_path = PathLib.datalog_programs_path.joinpath(db_config.db_type).joinpath(db_config.dir_name)
    os.chdir(gen_facts_path)
    command = ["./gen_facts.sh", "small", str(fact_path)]
    p = subprocess.run(command, capture_output=True)
    if p.returncode != 0:
        raise ChildProcessError(p.stderr.decode("utf-8"))


def create_input_facts(db_config, db_dir_name, db_file_name, fact_path,force_gen):
    if db_config.db_type == "DoopProgramAnalysis":
        create_doop_facts(db_config, db_dir_name, db_file_name, fact_path,force_gen)
    elif db_config.db_type == "SouffleSynthetic":
        init_synth_souffle_database(db_config, db_dir_name, fact_path,force_gen)
    print("initialized database: " + db_dir_name)


def create_doop_facts(db_config, db_version, db_file_name, fact_path, force_gen):
    os.chdir(PathLib.DOOP_BASE)
    clear_directory(fact_path)

    java_path = Path.joinpath(PathLib.java_source_dir, db_config.dir_name).joinpath(db_version).joinpath(
        db_file_name + ".java")
    jar_path1 = Path.joinpath(PathLib.java_source_dir, db_config.dir_name).joinpath(db_version).joinpath(
        db_file_name + ".jar")
    jar_path2 = Path.joinpath(PathLib.java_source_dir, db_config.dir_name).joinpath(db_version).joinpath(
        db_file_name + db_version + ".jar")
    if os.path.isfile(java_path):
        os.system("bin/mkjar " + str(java_path)
                  + " 1.8 " + str(jar_path1.parents[0]))  # + ">/dev/null 2>&1")
    else:
        print("No Java-file found, use .jar ")

    if os.path.isfile(jar_path1):
        jar_path = jar_path1
    elif os.path.isfile(jar_path2):
        jar_path = jar_path2
    else:
        raise FileNotFoundError("Java & Jar File do not exist: \n" + str(java_path) + "\n"+ str(jar_path1) + "\n"+ str(jar_path2))

    # cannot name the java or jar files appart bc. javac would complain that Class name & file-name differ
    doop_out_name = db_config.dir_name + "_" + db_version
    doop_out_path = PathLib.DOOP_OUT.joinpath(doop_out_name).joinpath("database")

    # Skip fact-generation, if the target directory contains facts already
    if any(fact_path.iterdir()) and not force_gen:
        return

    # Only run DOOP, if no output with doop_out_name exists
    if not any(doop_out_path.iterdir()) or force_gen:
        # Run DOOP to generate new facts for given java or jar
        os.system(f"./doop -a context-insensitive -i {jar_path} --id {doop_out_name} --facts-only --Xfacts-subset"
              f" APP --cache --generate-jimple ")
              #f"--platform java_11 --use-local-java-platform /home/kotname/.sdkman/candidates/java/current/bin/java")

    if not doop_out_path.is_dir():
        raise FileNotFoundError(f"the doop-facts do not exist: {PathLib.DOOP_OUT.joinpath(doop_out_name)}")

    for file in doop_out_path.glob("*.facts"):
        new_file_name = file.with_suffix('.tsv').name
        target_file = fact_path.joinpath(new_file_name)

        # Open the target file for writing
        with target_file.open("w") as out_file:
            #command = ["sort", str(file),"|","uniq"]
            command = f"sort {file} | uniq"
            #print("Command:", command)
            try:
                # Run the command, capture the output and write to target_file
                res = subprocess.run(command, shell=True, stdout=out_file, check=True)
            except subprocess.CalledProcessError as e:
                raise ChildProcessError(f"Command failed with error: {e}")

def chase_nemo(dl_rule_path, fact_path, result_path):
    if not dl_rule_path:
        return
    command = [str(PathLib.NEMO_ENGINE.joinpath("target/release/nmo")), str(dl_rule_path), "-I", str(fact_path), "-D",
               str(result_path), "--overwrite-results", "-e", "keep"]
    res = subprocess.run(command, capture_output=True)
    if res.returncode != 0:
        raise ChildProcessError(res.stderr.decode("utf-8"))

    nemo_runtime = split_nemo_stdout(res.stdout)
    os.chdir(PathLib.DOOP_BASE)
    return nemo_runtime


# parse Nemo output for runtimes
def split_nemo_stdout(stdout):
    stdout = stdout.decode("utf-8")
    stdout = stdout.split("\n")
    nemo_runtime = [re.search('[0-9m]*ms', stdout[0]).group(0), re.search('[0-9m]*ms', stdout[1]).group(0),
                    re.search('[0-9m]*ms', stdout[2]).group(0), re.search('[0-9m]*ms', stdout[3]).group(0)]
    return nemo_runtime


def print_nemo_runtime(runtime):
    t = PrettyTable()
    t.field_names = ["Program Analysis", "DB", "Total Reasoning", "Loading Input", "Reasoning", "Saving Output"]
    t.add_rows(runtime)
    print(t)

