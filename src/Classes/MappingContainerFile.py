import pandas as pd
from bidict import bidict

from src.Classes.DataContainerFile import DbInstance
from src.Classes import DomainElements, Facts
from src.Libraries import ShellLib
from operator import attrgetter
from src.Libraries import PathLib
import copy


# each MappingContainer has a Strategy and a similarity metric
class MappingContainer:
    def __init__(self, fact_paths, expansion_strategy, similarity_metric, mapping_id=None, run_nr=None,
                 dl_program=None):

        self.name = f"id_{mapping_id}_run_{run_nr}"

        self.db1_renamed_facts = DbInstance(fact_paths.db1_facts, self.name)

        # SET those to
        self.db_merged_facts = DbInstance(fact_paths.merge_facts, self.name)

        if dl_program is not None:
            self.dl_merged_program = dl_program.merge_dl
            self.db_merged_results = DbInstance(fact_paths.merge_results.joinpath(dl_program.name), self.name)
            self.db1_unravelled_results = DbInstance(fact_paths.db1_results.joinpath(dl_program.name), self.name)
            self.db2_unravelled_results = DbInstance(fact_paths.db2_results.joinpath(dl_program.name), self.name)

        self.final_mapping = pd.DataFrame()
        self.final_fact_pairs = dict()

        self.syn_counter = 0
        self.expansion_strategy = expansion_strategy
        self.similarity_metric = similarity_metric

        self.facts_db1 = bidict()
        self.facts_db2 = bidict()

        self.elements_db1 = dict()
        self.elements_db2 = dict()

        self.anchor_nodes = [set(), set()]  # log how many anchor nodes were expanded for DB1 and DB2
        self.c_accepted_anchor_mappings = 0
        self.c_uncertain_mappings = 0
        self.c_hub_recomp = 0
        self.c_mappings = 0

        # Initialise mapping_func-identifier with potential dummies
        self.mapping_path = fact_paths.mapping_results.joinpath(self.name).with_suffix('.tsv')
        self.mapping_id = mapping_id
        self.run_nr = run_nr

    def init_facts_elements_db1(self, db1):
        max_deg1 = self.init_facts_elements_db(db1, self.elements_db1, self.facts_db1)
        self.similarity_metric.set_max_deg1(max_deg1)

    def init_facts_elements_db2(self, db2):
        max_deg2 = self.init_facts_elements_db(db2, self.elements_db2, self.facts_db2)
        self.similarity_metric.set_max_deg2(max_deg2)

    # elements and facts need to be initialised together because element.occurrences points to fact_obj
    # and fact.elements points to element
    @staticmethod
    def init_facts_elements_db(db_instance, elements, facts):

        multi_col_elements = set()
        for file_name, df in db_instance.files.items():
            col_len = df.shape[1]
            for row_ind, row in df.iterrows():

                curr_fact = Facts.Fact(row_ind, file_name, db_instance.name, col_len)
                facts[row_ind, file_name] = curr_fact

                temp_dict = dict()
                for col_ind, element_name in row.items():
                    # in case a element appears several times in same atom i.e. A("a","a","b") -> make a list of cols [1,2]
                    temp_dict.setdefault(element_name, list()).append(col_ind)

                # unpack values
                for element_name, cols in temp_dict.items():
                    if len(cols) > 1:
                        multi_col_elements.add(element_name)
                    if element_name in elements:
                        element = elements[element_name]
                        element.update(file_name, cols, curr_fact)
                    else:
                        element = DomainElements.Element(element_name, file_name, cols, curr_fact)
                        elements[element_name] = element

                    curr_fact.add_element(element, cols)
        max_deg_node = max(elements.values(), key=attrgetter('degree'))
        return max_deg_node.degree

    def set_mapping(self, mapping):
        self.final_mapping = mapping

    def compute_mapping(self, db1, db2, DL_blocked_elements):
        c_mappings = self.expansion_strategy.accept_expand_mappings(self, self.elements_db1, self.elements_db2,
                                                                    DL_blocked_elements, self.similarity_metric)
        self.c_mappings = c_mappings

        # do the renaming of Elements1 & matching of facts
        # this could also be avoided through implementation of the fact-objs, but is too much work rn
        for file_name, df1_original in db1.files.items():
            df2_original = db2.files[file_name]
            df1 = df1_original.copy(deep=True)
            df2 = df2_original.copy(deep=True)
            if file_name in self.final_fact_pairs:
                matched_fact_pairs = self.final_fact_pairs[file_name]
            else:
                matched_fact_pairs = []
            mapped_df = self.map_df(matched_fact_pairs, df1, df2, self.final_mapping['element1'],
                                    self.final_mapping['element2'])
            self.db1_renamed_facts.insert_df(file_name, mapped_df)
        return

    def map_df(self, matched_fact_pairs, df1, df2, old_elements_ser, new_elements_ser):
        # assuming that keys & values are unpacked according to insertion order
        matched_facts = list()

        # Simplify the renaming if we have information from the expansion strategy, that certain facts are equal now
        # Then we can take them out before renaming
        if matched_fact_pairs:
            rec1_indices = list()
            rec2_indices = list()
            for fact1, fact2 in matched_fact_pairs:
                if fact1 in rec1_indices or fact2 in rec2_indices:
                    raise ValueError(f"fact already in indices {fact1, fact2}")
                rec1_indices.append(fact1)
                rec2_indices.append(fact2)

            merged_df = df2.iloc[rec2_indices].reset_index(drop=True)
            df1.drop(rec1_indices, inplace=True)

        df1_replaced = df1.replace(old_elements_ser.to_list(), new_elements_ser.to_list())

        # Concatenate the DataFrames
        if matched_fact_pairs:
            # print(f"remove {len(rec1_indices)} indices from DF1")
            merged_df = pd.concat([merged_df, df1_replaced], ignore_index=True)
            return merged_df
        else:
            return df1_replaced

    def read_mapping(self):
        if self.mapping_id is None:
            raise ValueError(f"expected mapping_id{self.mapping_path, self.mapping_id}")
        if self.mapping_path.exists():
            df = pd.read_csv(self.mapping_path, sep='\t', header=None, names=['element1', 'element2', 'sim'],
                             keep_default_na=False, lineterminator='\n')
            # check how many elements have been mapped to synthetic element
            syn_counter = df.iloc[:, 1].str.startswith("new_var").value_counts()
            if True in syn_counter:
                self.syn_counter = syn_counter[True]
            elif False in syn_counter:
                self.syn_counter = 0
            else:
                raise ValueError(f"{syn_counter}")

            self.final_mapping = df
        else:
            raise FileNotFoundError(self.mapping_path)

    # write mapping_func results to CSV file
    def log_mapping(self):
        ShellLib.clear_file(self.mapping_path)
        self.final_mapping.to_csv(self.mapping_path, sep='\t', index=False, header=False)

    def merge_dbs(self, db1, db2, to_db):
        for file_name in db1.files.keys():
            df1 = db1.files[file_name]
            df2 = db2.files[file_name]

            if not df1.empty and not df2.empty:
                l_cols = len(df1.columns)
                cols = list(range(l_cols))
                df = pd.merge(df1, df2, how='outer', on=cols, indicator=str(l_cols))
                df[str(l_cols)] = df[str(l_cols)].astype(str).replace(
                    {'both': '0', 'left_only': '1', "right_only": '2'})
            elif not df1.empty:
                l_cols = len(df1.columns)
                df = df1.copy()
                df[l_cols] = '1'
            elif not df2.empty:
                l_cols = len(df2.columns)
                df = df2.copy()
                df[l_cols] = '2'
            else:
                df = pd.DataFrame()
            to_db.insert_df(file_name, df)

    # from_db & to_db are objects of self.mapping_func, so setting them will modify self.mapping_func (since its pointers)
    # from_DB is usually db2 & to_db is db1
    def unravel_merge_dbs(self):
        if self.db1_unravelled_results is None or self.db2_unravelled_results is None:
            raise ValueError(f"db is not setup correctly {self.db1_unravelled_results, self.db2_unravelled_results}")

        # pa_additionally_elements = set()
        for file_name, df in self.db_merged_results.files.items():
            if not df.empty:
                df0 = df[df.iloc[:, -1] == '0']
                df1 = df[df.iloc[:, -1] == '1']
                df2 = df[df.iloc[:, -1] == '2']
                df1 = pd.concat([df1, df0], axis=0, ignore_index=True)
                df1 = df1.iloc[:, :-1]
                df2 = pd.concat([df2, df0], axis=0, ignore_index=True)
                df2 = df2.iloc[:, :-1]

                # reverse columns of mapping_func to inverse the mapping_func
                df1 = self.map_df([], df1, df2, self.final_mapping['element2'], self.final_mapping['element1'])
                self.db1_unravelled_results.insert_df(file_name, df1)
                self.db2_unravelled_results.insert_df(file_name, df2)
            else:
                self.db1_unravelled_results.insert_df(file_name, pd.DataFrame())
                self.db2_unravelled_results.insert_df(file_name, pd.DataFrame())

    def get_finger_print(self) -> dict:
        return {"expansion": self.expansion_strategy.name, "dynamic": str(self.expansion_strategy.DYNAMIC),
                "anchor_quantile": self.expansion_strategy.anchor_quantile.initial_q,
                "metric": self.similarity_metric.name,
                "importance_parameter": self.similarity_metric.imp_alpha}

    def get_result_finger_print(self):
        return {"synthetic_elements": self.syn_counter, "hub_computations": self.c_hub_recomp,
                "uncertain_mappings": self.c_uncertain_mappings, "computed_mappings": self.c_mappings}

    def get_nr_element1(self):
        return len(self.elements_db1)

    def get_nr_element2(self):
        return len(self.elements_db2)