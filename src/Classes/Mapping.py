import pandas as pd
from bidict import bidict

from src.Classes import Databases, Terms, Records
from src.Libraries import ShellLib


# each Mapping has a Strategy and a similarity metric
class Mapping:
    def __init__(self, paths, exp_name, expansion_strategy, sim_name, similarity_metric):
        name = exp_name + "-" + sim_name
        self.name = name

        self.db1_renamed_facts = Databases.DbInstance(paths.db1_facts, name)

        self.db_merged_facts = Databases.DbInstance(paths.merge_facts, name)
        self.db_merged_results = Databases.DbInstance(paths.merge_results, name)

        self.db1_unravelled_results = Databases.DbInstance(paths.db1_results, name)
        self.db2_unravelled_results = Databases.DbInstance(paths.db2_results, name)

        self.final_mapping = pd.DataFrame()
        self.final_rec_tuples = list()

        self.mapping_path = paths.mapping_results.joinpath(self.name).with_suffix('.tsv')
        self.new_term_counter = 0
        self.expansion_strategy = expansion_strategy
        self.similarity_metric = similarity_metric

        self.records_db1 = bidict()
        self.records_db2 = bidict()

        self.terms_db1 = dict()  # could be bidict as well
        self.terms_db2 = dict()

        self.c_uncertain_mappings = 0
        self.c_hub_recomp = 0
        self.c_comp_tuples = 0

    def initialize_records_terms_db1(self, db1):
        self.init_records_terms_db(db1, self.terms_db1, self.records_db1)

    def init_records_terms_db2(self, db2):
        self.init_records_terms_db(db2, self.terms_db2, self.records_db2)

    # terms and records need to be initialised together because term.occurrences points to record_obj 
    # and record.terms points to term
    @staticmethod
    def init_records_terms_db(db_instance, terms, records):

        multi_col_terms = set()
        for file_name, df in db_instance.files.items():
            col_len = df.shape[1]
            for row_ind, row in df.iterrows():

                curr_record = Records.Record(row_ind, file_name, db_instance.name, col_len)
                records[row_ind, file_name] = curr_record

                temp_dict = dict()
                for col_ind, term_name in row.items():
                    # in case a term appears several times in same atom i.e. A("a","a","b") -> make a list [1,2]
                    temp_dict.setdefault(term_name, list()).append(col_ind)

                # unpack values
                for term_name, cols in temp_dict.items():
                    if len(cols) > 1:
                        multi_col_terms.add(term_name)
                    if term_name in terms:
                        term = terms[term_name]
                        term.update(file_name, cols, curr_record)
                    else:
                        term = Terms.Term(term_name, file_name, cols, curr_record)
                        terms[term_name] = term

                    curr_record.add_term(term, cols)

        print("Count of terms with multi-occurrences in " + db_instance.name + " : " + str(len(multi_col_terms)))

    def set_mapping(self, mapping):
        self.final_mapping = mapping

    def compute_mapping(self, db1, pa_non_mapping_terms):
        self.expansion_strategy(self, self.records_db1, self.terms_db1, self.records_db2, self.terms_db2,
                                pa_non_mapping_terms,
                                self.similarity_metric)
        '''
        uncertain_mapping_tuples, count_hub_recomp, comp_tuples = 
        self.c_uncertain_mappings = uncertain_mapping_tuples
        self.c_hub_recomp = count_hub_recomp
        self.c_comp_tuples = comp_tuples
        '''
        # do the renaming of Terms1 & matching of records
        # this could also be avoided through implementation of the record-objs, but is too much work rn
        for file_name, df in db1.files.items():
            mapped_df = self.map_df(df, self.final_mapping[0], self.final_mapping[1])
            self.db1_renamed_facts.insert_df(file_name, mapped_df)
        return

    def map_df(self, df, from_terms, to_terms):
        # assuming that keys & values are unpacked according to insertion order
        return df.replace(from_terms.to_list(), to_terms.to_list())

    def read_mapping(self):
        if self.mapping_path.exists():
            df = pd.read_csv(self.mapping_path, sep='\t', header=None)
            # check how many terms have been mapped to synthetic term
            self.new_term_counter = df[1].str.startswith("new_var").value_counts()
            self.final_mapping = df
        else:
            raise FileNotFoundError(self.mapping_path)

    # write mapping results to CSV file
    def log_mapping(self):
        ShellLib.clear_directory(self.mapping_path.parent)
        self.final_mapping.to_csv(self.mapping_path, sep='\t', index=False, header=False)

    # can be implemented faster, just replace db
    def merge_dbs(self, db1, db2, to_db):
        for file_name in db1.files.keys():
            df1 = db1.files[file_name]
            df2 = db2.files[file_name]
            if not df1.empty and not df2.empty:
                l_cols = len(df1.columns)
                cols = list(range(l_cols))
                df = pd.merge(df1, df2, how='outer', on=cols, indicator=str(l_cols))
                df[str(l_cols)] = df[str(l_cols)].astype(str).replace(
                    {'both': '0', 'left_only': '1', "right_only": '10'})
            elif not df1.empty:
                l_cols = len(df1.columns)
                df = df1
                df[l_cols] = '1'
            elif not df2.empty:
                l_cols = len(df2.columns)
                df = df2
                df[l_cols] = '10'
            else:
                df = pd.DataFrame()
            to_db.insert_df(file_name, df)

    # from_db & to_db are objects of self.mapping, so setting them will modify self.mapping (since its pointers)
    # from_DB is usually db2 & to_db is db1
    def unravel_merge_dbs(self):
        # pa_additionally_terms = set()
        for file_name, df in self.db_merged_results.files.items():
            if not df.empty:
                df0 = df[df.iloc[:, -1] == '0']
                df1 = df[df.iloc[:, -1] == '1']
                df2 = df[df.iloc[:, -1] == '10']
                df1 = pd.concat([df1, df0], axis=0, ignore_index=True)
                df1 = df1.iloc[:, :-1]
                df2 = pd.concat([df2, df0], axis=0, ignore_index=True)
                df2 = df2.iloc[:, :-1]
                # reverse columns of mapping to inverse the mapping
                df1 = self.map_df(df1, self.final_mapping[1], self.final_mapping[0])
                self.db1_unravelled_results.insert_df(file_name, df1)
                self.db2_unravelled_results.insert_df(file_name, df2)
            else:
                self.db1_unravelled_results.insert_df(file_name, pd.DataFrame())
                self.db2_unravelled_results.insert_df(file_name, pd.DataFrame())
        return