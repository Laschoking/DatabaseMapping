import itertools
from enum import Enum
from pathlib import Path
import pandas as pd
from Python.Libraries import PathLib
from Python.Libraries import ShellLib
from sortedcontainers import SortedList,SortedDict
import csv
from collections import Counter
import os


class FileRecordIds:
    def __init__(self,file_name):
        self.name = file_name
        self.rids = set()
        self.rids1 = dict()
        self.rids2 = dict()

    def add_record_tuples(self,tuples):
        for tuple in tuples:
            self.add_record_tuple(tuple)

    def add_record_tuple(self,rid_tuple):
        self.rids.add(rid_tuple)
        self.rids1.setdefault(rid_tuple.rid1,list()).append(rid_tuple)
        self.rids2.setdefault(rid_tuple.rid2, list()).append(rid_tuple)

    def remove_record_tuple(self,rid_tuple):
        self.rids.remove(rid_tuple)
        self.rids1[rid_tuple.rid1].remove(rid_tuple)
        self.rids2[rid_tuple.rid2].remove(rid_tuple)

        # remove the record-id from the mapping, if the record-combination was the onliest in the set
        if not self.rids1[rid_tuple.rid1] :
            del self.rids1[rid_tuple.rid1]
        if not self.rids2[rid_tuple.rid2] :
            del self.rids2[rid_tuple.rid2]


class Term:
    def __init__(self, term_name, file_name,col_ind,row_ind):
        self.name = term_name
        self.occurrences = dict()
        self.occurrence_c = Counter()
        self.attached_tuples = set()
        self.type = "int" if type(term_name) is int else "string"
        self.degree = 0

        self.update(file_name,col_ind,row_ind)

    # one occurence has the following structure: file_name,cols,row_nr
    # the collection is of following structure {(file_name,cols) : [row_nr1,row_nr2, ...]}
    # this way, all row_nr are stored together, but with file_name,cols as keys
    # those keys can be used for later set-operations while mapping
    def update(self,file_name,cols,row_nr):
        key = (file_name,cols)
        self.occurrence_c.update([key])
        if key in self.occurrences:
            self.occurrences[key].append(row_nr)
        else:
            self.occurrences[key] = [row_nr]
        self.degree += 1

# this will be 1 potential mapping
class TermTuple:
    def __init__(self,term_obj1, term_obj2,similiarity_metric):
        self.term_obj1 = term_obj1
        term_obj1.attached_tuples.add(self)
        term_obj2.attached_tuples.add(self)
        self.term_obj2 = term_obj2
        self.rec_ids = dict()
        self.similiarity_metric = similiarity_metric
        self.sim = 0


    def compute_similarity(self):
        self.sim = self.similiarity_metric(self.term_obj1, self.term_obj2, self.rec_ids)


    # expanded_rid_tuples holds all tuples, that have been considered already
    def occurrence_overlap(self,expanded_rid_tuples,active_rid_tuples,db_files):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = self.term_obj1.occurrence_c & self.term_obj2.occurrence_c

        for file_name, mapped_cols in intersection:

            col_len = db_files[file_name].shape[1]
            self.rec_ids.setdefault(file_name,FileRecordIds(file_name))


            l_ids1 = self.term_obj1.occurrences[(file_name,mapped_cols)]
            l_ids2 = self.term_obj2.occurrences[(file_name,mapped_cols)]
            for rid1 in l_ids1:

                for rid2 in l_ids2:
                    b_rid1_active = rid1 in active_rid_tuples[file_name].rids1
                    b_rid2_active = rid2 in active_rid_tuples[file_name].rids2
                    if b_rid1_active != b_rid2_active:
                        # this means we would expand a record-tuple, where one side of the record-tuple is already activated by a mapping
                        # but the other side not (i.e. active-mapping(1,2) and we want to introduce new mapping (3,2) will never work
                        continue

                    # both record-id1 and record-id2 point to the same object Record-Tuple (consisting of record-id1 and record-id2, etc.)

                    if (file_name,rid1,rid2) not in expanded_rid_tuples:
                        rid_obj = RecordTuple(rid1,rid2,col_len)
                        expanded_rid_tuples[(file_name,rid1,rid2)] = rid_obj
                    else:
                        # get access to the existing record-tuple-object
                        rid_obj = expanded_rid_tuples[(file_name,rid1,rid2)]
                    rid_obj.add_subscriber(self,mapped_cols)
                    print(f"{self.term_obj1.name},{self.term_obj2.name} subscribe to record {rid_obj.rid1},{rid_obj.rid2}")
                    self.rec_ids[file_name].add_record_tuple(rid_obj)

        self.compute_similarity()

    def remove_rid_comb(self,file_name,rid_tuple):
        self.rec_ids[file_name].remove_record_tuple(rid_tuple)
        # if this was the only red-tuple combination for the term tuple, we need to detach the TermTuple from the termObjects
        if not self.rec_ids[file_name].rids:
            del self.rec_ids[file_name]
            if not self.rec_ids:
                self.term_obj1.attached_tuples.remove(self)
                self.term_obj2.attached_tuples.remove(self)

    # if we want to destroy the term tuple, we need to make sure it is not linked to any thing anymore
    def prepare_self_destroy(self):
        #print(f"remove {self} from {self.term_obj1} and from {self.term_obj2}")
        self.term_obj1.attached_tuples.remove(self)
        self.term_obj2.attached_tuples.remove(self)
        for rid_objs in self.rec_ids.values():

            for rid_tuple in rid_objs.rids:
                print(f"self destroy: {self.term_obj1.name},{self.term_obj2.name} from {rid_tuple.rid1},{rid_tuple.rid2}")
                del rid_tuple.subscribers[self]


    def get_similarity(self):
        return self.sim

    def get_records(self):
        return self.rec_ids

class RecordTuple:
    def __init__(self,rid1,rid2,col_len):
        self.rid1 = rid1
        self.rid2 = rid2
        self.col_len = col_len
        self.vacant_cols = list(range(col_len))
        self.subscribers = dict()

    def mark_filled_cell(self, filled_cells):
        for cells in filled_cells:
            self.vacant_cols.remove(cells)
        # return True if the record is finished (no terms are vacant anymore)
        if len(self.vacant_cols) == 0:
            return True
        else:
            return False
# add column to the subscriber?
    def add_subscriber(self,term_tuple,mapped_col):
        self.subscribers[term_tuple] = mapped_col
    def get_subscribers(self):
        return self.subscribers


class DB_Instance:
    def __init__(self,db_base_path, sub_dir):
        self.db_base_path = db_base_path
        self.name = db_base_path.stem + "-" + sub_dir
        self.path = db_base_path.joinpath(sub_dir)
        self.files = dict()

        #ShellLib.clear_directory(self.path)


    def read_db_relations(self):
        if not self.path.is_dir():
            raise FileNotFoundError("Directory does not exist: " + str(self.path))
        for rel_path in self.path.glob("*"):
            file_name = rel_path.stem
            if os.stat(rel_path).st_size == 0:
                df = pd.DataFrame()
            else:
                df = pd.read_csv(rel_path,sep='\t', keep_default_na=False,dtype='string',header=None,on_bad_lines='warn')
            self.insert_df(file_name,df)



    def insert_df(self, file_name, df):
        self.files[file_name] = df



    def log_db_relations(self):
        ShellLib.clear_directory(self.path)
        for file_name,df in self.files.items():
            df.to_csv(self.path.joinpath(file_name).with_suffix('.tsv'),sep="\t",index=False,header=False)

class BasePaths:
    def __init__(self,base_output_path,db1_base_path, db2_base_path):
        self.db1_facts = db1_base_path.joinpath("facts")
        self.db2_facts = db2_base_path.joinpath("facts")
        self.db1_results = db1_base_path.joinpath("results")
        self.db2_results = db2_base_path.joinpath("results")
        self.merge_facts = base_output_path.joinpath("merge_db").joinpath("facts")
        self.merge_results = base_output_path.joinpath("merge_db").joinpath("results")
        self.mapping_results = base_output_path.joinpath("mappings")
        self.terms1 = base_output_path.joinpath("Terms1.tsv")
        self.terms2 = base_output_path.joinpath("Terms2.tsv")
        self.global_log = PathLib.base_out_path.joinpath("Results")

class DataBag:
    def __init__(self, base_output_path, db1_base_path, db2_base_path):
        self.paths = BasePaths(base_output_path,db1_base_path, db2_base_path)
        # origin of the facts for both databases

        self.db1_original_facts = DB_Instance(self.paths.db1_facts, "db1-original")
        self.db2_original_facts = DB_Instance(self.paths.db2_facts, "db2-original")

        # origin for separate Program Analysis without Bijection
        self.db1_original_results = DB_Instance(self.paths.db1_results, "db1-original")
        self.db2_original_results = DB_Instance(self.paths.db2_results, "db2-original")

        self.terms1 = dict()
        self.terms2 = dict()
        self.mappings = []

    def add_mapping(self, mapping):
        self.mappings.append(mapping)

    def read_terms_from_db(self,terms, db_instance):
        multi_col_terms = set()
        for file_name, df in db_instance.files.items():
            for row_ind, row in df.iterrows():
                temp_dict = {}
                for col_ind, term in row.items():
                    if term not in temp_dict:
                        temp_dict[term] = (col_ind,)
                    else:
                        # in case a term appears several times in same atom i.e. A("a","a","b") -> make a tuple (1,2)
                        temp_dict[term] = temp_dict[term] + (col_ind,)

                        multi_col_terms.add(term)

                # unpack values
                for term, cols in temp_dict.items():
                    if term in terms:
                        terms[term].update(file_name, cols,row_ind)
                    else:
                        terms[term] = Term(term, file_name,cols,row_ind)
        print("Count of terms with multi-occurrences in " + db_instance.name + " : " + str(len(multi_col_terms)))
        return terms

    def log_terms(self):
        terms1_df = pd.Series(self.terms1.keys())
        if not self.paths.terms1.exists():
            terms1_df.to_csv(self.paths.terms1,sep='\t',index=False,header=False)

        terms2_df = pd.Series(self.terms2.keys())
        if not self.paths.terms2.exists():
            terms2_df.to_csv(self.paths.terms2,sep='\t',index=False,header=False)




