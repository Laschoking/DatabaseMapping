import pandas as pd
from bidict import bidict

from src.Classes.DataContainerFile import DbInstance
from src.Classes import Terms, Records
from src.Libraries import ShellLib
from operator import attrgetter
from src.Libraries import PathLib
import copy

# each MappingContainer has a Strategy and a similarity metric
class MappingContainer:
    def __init__(self, paths, expansion_strategy, similarity_metric):

        if expansion_strategy.DYNAMIC:
            exp_type = "dynamic"
        else:
            exp_type = "static"
        name = exp_type + "_" + expansion_strategy.name + "-" + similarity_metric.name.replace(" ", "")
        self.name = name

        self.db1_renamed_facts = DbInstance(paths.db1_facts, name)

        self.db_merged_facts = DbInstance(paths.merge_facts, name)
        self.db_merged_results = DbInstance(paths.merge_results, name)

        self.db1_unravelled_results = DbInstance(paths.db1_results, name)
        self.db2_unravelled_results = DbInstance(paths.db2_results, name)

        self.final_mapping = pd.DataFrame()
        self.final_rec_tuples = dict()

        self.mapping_path = paths.mapping_results.joinpath(self.name).with_suffix('.tsv')
        self.syn_counter = 0
        self.expansion_strategy = expansion_strategy
        self.similarity_metric = similarity_metric

        self.records_db1 = bidict()
        self.records_db2 = bidict()

        self.terms_db1 = dict()
        self.terms_db2 = dict()

        self.anchor_nodes = [set(), set()]  # log how many anchor nodes were expanded for DB1 and DB2
        self.c_accepted_anchor_mappings = 0
        self.c_uncertain_mappings = 0
        self.c_hub_recomp = 0
        self.c_mappings = 0

    def init_records_terms_db1(self, db1):
        max_deg1 = self.init_records_terms_db(db1, self.terms_db1, self.records_db1)
        self.similarity_metric.max_deg1 = max_deg1
    def init_records_terms_db2(self, db2):
        max_deg2 = self.init_records_terms_db(db2, self.terms_db2, self.records_db2)
        self.similarity_metric.max_deg2 = max_deg2


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
                    # in case a term appears several times in same atom i.e. A("a","a","b") -> make a list of cols [1,2]
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
        max_deg_node = max(terms.values(),key=attrgetter('degree'))
        return max_deg_node.degree

    def set_mapping(self, mapping):
        self.final_mapping = mapping

    def compute_mapping(self, db1, db2, DL_blocked_terms):
        c_mappings = self.expansion_strategy.accept_expand_mappings(self, self.terms_db1, self.terms_db2,DL_blocked_terms, self.similarity_metric)
        self.c_mappings = c_mappings

        # do the renaming of Terms1 & matching of records
        # this could also be avoided through implementation of the record-objs, but is too much work rn
        for file_name, df1_original in db1.files.items():
            df2_original = db2.files[file_name]
            df1 = df1_original.copy(deep=True)
            df2 = df2_original.copy(deep=True)
            if file_name in self.final_rec_tuples:
                matched_rec_tuples = self.final_rec_tuples[file_name]
            else:
                matched_rec_tuples = []
            mapped_df = self.map_df(matched_rec_tuples, df1, df2, self.final_mapping['constant1'],
                                    self.final_mapping['constant2'])
            self.db1_renamed_facts.insert_df(file_name, mapped_df)
        return

    def map_df(self, matched_rec_tuples, df1, df2, old_constants_ser, new_constants_ser):
        # assuming that keys & values are unpacked according to insertion order
        matched_records = list()

        # Simplify the renaming if we have information from the expansion strategy, that certain records are equal now
        # Then we can take them out before renaming
        if matched_rec_tuples:
            rec1_indices = list()
            rec2_indices = list()
            for record1, record2 in matched_rec_tuples:
                if record1 in rec1_indices or record2 in rec2_indices:
                    raise ValueError(f"record already in indices {record1, record2}")
                rec1_indices.append(record1)
                rec2_indices.append(record2)

            merged_df = df2.iloc[rec2_indices].reset_index(drop=True)
            df1.drop(rec1_indices, inplace=True)

        df1_replaced = df1.replace(old_constants_ser.to_list(), new_constants_ser.to_list())

        # Concatenate the DataFrames
        if matched_rec_tuples:
            # print(f"remove {len(rec1_indices)} indices from DF1")
            merged_df = pd.concat([merged_df, df1_replaced], ignore_index=True)
            return merged_df
        else:
            return df1_replaced

    def read_mapping(self,run_nr):
        mapping_path = PathLib.add_run_nr_to_path(file_path=self.mapping_path, run_nr=run_nr)
        # TODO put run-number into each mapping and as directory suffix for the merged db
        if self.mapping_path.exists():
            df = pd.read_csv(self.mapping_path, sep='\t', header=None, names=['constant1','constant2','sim'])
            # check how many terms have been mapped to synthetic term
            self.syn_counter = df.iloc[:,1].str.startswith("new_var").value_counts()[True]
            self.final_mapping = df
        else:
            raise FileNotFoundError(self.mapping_path)

    # write mapping results to CSV file
    def log_mapping(self,run_nr):
        ShellLib.clear_file(self.mapping_path)
        mapping_path = PathLib.add_run_nr_to_path(file_path=self.mapping_path, run_nr=run_nr)
        self.final_mapping.to_csv(mapping_path, sep='\t', index=False, header=False)

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
                df1 = self.map_df([], df1, df2, self.final_mapping['constant2'], self.final_mapping['constant1'])
                self.db1_unravelled_results.insert_df(file_name, df1)
                self.db2_unravelled_results.insert_df(file_name, df2)
            else:
                self.db1_unravelled_results.insert_df(file_name, pd.DataFrame())
                self.db2_unravelled_results.insert_df(file_name, pd.DataFrame())


    def get_finger_print(self) -> dict:
        return {"expansion" : self.expansion_strategy.name, "dynamic" : str(self.expansion_strategy.DYNAMIC),
                "anchor_quantile": self.expansion_strategy.anchor_quantile.initial_q, "metric" : self.similarity_metric.name,
                "importance_weight" : self.similarity_metric.metric_weight}

    def get_result_finger_print(self):
        return {"synthetic_terms" : self.syn_counter, "hub_computations" : self.c_hub_recomp,
                 "uncertain_mappings" : self.c_uncertain_mappings, "computed_mappings" : self.c_mappings}




    def get_nr_term1(self):
        return len(self.terms_db1)

    def get_nr_term2(self):
        return len(self.terms_db2)


