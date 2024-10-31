import os
from src.Libraries import ShellLib, PathLib
import pandas as pd


class DbInstance:
    def __init__(self, db_base_path, sub_dir):
        self.db_base_path = db_base_path
        self.name = db_base_path.stem + "-" + sub_dir
        self.path = db_base_path.joinpath(sub_dir)
        self.files = dict()

        # ShellLib.clear_directory(self.path)

    def read_db_relations(self):
        if not self.path.is_dir():
            raise FileNotFoundError("Directory does not exist: " + str(self.path))
        for rel_path in self.path.glob("*"):
            file_name = rel_path.stem
            if os.stat(rel_path).st_size == 0:
                df = pd.DataFrame()
            else:
                try:
                    df = pd.read_csv(rel_path, sep='\t', keep_default_na=False, dtype='string', header=None,
                                     on_bad_lines='warn', lineterminator='\n')
                except pd.errors.ParserError as e:
                    print(f"{e} parser error for path: {rel_path}")
            self.insert_df(file_name, df)
        return self

    def insert_df(self, file_name, df):
        self.files[file_name] = df

    def get_nr_facts_elements(self):
        """ Returns nr of facts and the number of elements
            Finding the number of elements is not very elaborate, because only the mapping_obj
            has normally access to them
        """
        elements = set()
        nr_facts = 0
        for file_df in self.files.values():
            nr_facts += len(file_df)
            for col in file_df.columns:
                elements.update(file_df[col].unique())

        return pd.Series({'nr_facts': nr_facts, 'nr_elements': len(elements)})

    def log_db_relations(self):
        ShellLib.clear_directory(self.path)
        for file_name, df in self.files.items():
            df.to_csv(self.path.joinpath(file_name).with_suffix('.tsv'), sep="\t",
                      index=False, header=False)


class BasePaths:
    def __init__(self, base_output_path, db1_base_path, db2_base_path):
        self.db1_facts = db1_base_path.joinpath("facts")
        self.db2_facts = db2_base_path.joinpath("facts")
        self.db1_results = db1_base_path.joinpath("results")
        self.db2_results = db2_base_path.joinpath("results")
        self.merge_facts = base_output_path.joinpath("merge_db").joinpath("facts")
        self.merge_results = base_output_path.joinpath("merge_db").joinpath("results")
        self.mapping_results = base_output_path.joinpath("mappings")


class DlSeparateResultsContainer:
    def __init__(self, base_output_path,  db1_base_path, db2_base_path,dl_name):

        self.paths = BasePaths(base_output_path, db1_base_path, db2_base_path)
        # set up the databse instances for separate Program Analysis without Mapping & merging
        self.db1_original_results = DbInstance(self.paths.db1_results.joinpath(dl_name), "db1_sep")
        self.db2_original_results = DbInstance(self.paths.db2_results.joinpath(dl_name), "db2_sep")


class OriginalFactsContainer:
    def __init__(self, base_output_path, db1_base_path, db2_base_path):
        self.paths = BasePaths(base_output_path, db1_base_path, db2_base_path)
        # origin of the facts for both databases

        self.db1_original_facts = DbInstance(self.paths.db1_facts, "db1")
        self.db2_original_facts = DbInstance(self.paths.db2_facts, "db2")

        self.mappings = []

    def add_mapping(self, mapping):
        self.mappings.append(mapping)

    def add_mappings(self, mappings):
        self.mappings += mappings

    def log_elements(self):
        elements_db1_df = pd.Series(self.elements_db1.keys())
        if not self.paths.elements_db1.exists():
            elements_db1_df.to_csv(self.paths.elements_db1, sep='\t', index=False, header=False)

        elements_db2_df = pd.Series(self.elements_db2.keys())
        if not self.paths.elements_db2.exists():
            elements_db2_df.to_csv(self.paths.elements_db2, sep='\t', index=False, header=False)