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


class Record:
    def __init__(self, rid, col_len,file_name):
        self.rid = rid
        self.file_name = file_name
        self.col_len = col_len
        self.vacant_cols = list(range(col_len))
        self.record_tuples = set()
        self.active = False
        self.active_records_tuples = dict()
        self.expanded_record_tuples = dict() # is a dict of other record to object

    def is_active(self):
        return self.active

    def set_active(self):
        if not self.active:
            raise ValueError(f"boolean was active already: {self.rid,self.file_name}")
        self.active = True
    def add_record_tuple(self, rid_tuple):
        self.record_tuples.add(rid_tuple)
    def subscribe_exp_tuple(self,other_rec_obj,rec_tuple_obj):
        self.expanded_record_tuples[other_rec_obj] = rec_tuple_obj

    def subscribe_active_tuple(self,other_rec_obj,rec_tuple_obj):
        self.expanded_record_tuples[other_rec_obj] = rec_tuple_obj


class RecordTuple:
    def __init__(self, rec_obj1, rec_obj2):
        self.rec_obj1 = rec_obj1
        self.rec_obj2 = rec_obj2
        self.subscribers = dict()

    def mark_filled_cell(self, filled_cells):
        for cells in filled_cells:
            self.rec_obj1.vacant_cols.remove(cells)
            self.rec_obj2.vacant_cols.remove(cells)

        # return True if the record is finished (no terms are vacant anymore)
        if len(self.rec_obj1.vacant_cols) == 0:
            return True
        else:
            return False

    def add_subscriber(self, term_tuple, mapped_col):
        self.subscribers[term_tuple] = mapped_col

    def get_subscribers(self):
        return self.subscribers

    def remove_subscriber(self,term_tuple):
        if term_tuple not in self.subscribers:
            KeyError(" Term Tuple does not exist: " + str(term_tuple))
        del self.subscribers[term_tuple]


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
        self.sub_rids1 = set() # everybody holds a different amount of record_objects, so you know where you are subscribed
        self.sub_rids2 = set()
        self.sub_rids = set()
        self.similiarity_metric = similiarity_metric
        self.sim = 0


    def compute_similarity(self):
        self.sim = self.similiarity_metric(self.term_obj1, self.term_obj2, self.sub_rids1,self.sub_rids2)

    # expanded_rid_tuples holds all tuples, that have been considered already
    def occurrence_overlap(self,records1,records2):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = self.term_obj1.occurrence_c & self.term_obj2.occurrence_c

        for file_name, mapped_cols in intersection:

            l_ids1 = self.term_obj1.occurrences[(file_name,mapped_cols)]
            l_ids2 = self.term_obj2.occurrences[(file_name,mapped_cols)]
            for rid1 in l_ids1:
                rec_obj1 = records1[(rid1,file_name)]

                for rid2 in l_ids2:
                    rec_obj2 = records2[rid2, file_name]

                    if rec_obj1.is_active() != rec_obj2.is_active():
                        # this means we would expand a record-tuple, where one side of the record-tuple is already activated by a mapping
                        # but the other side not (i.e. is_active-mapping(1,2) and we want to introduce new mapping (3,2) will never work
                        continue

                    # both record-id1 and record-id2 point to the same object Record-Tuple (consisting of record-id1 and record-id2, etc.)

                    if rec_obj2 not in rec_obj1.expanded_record_tuples.keys():
                        rec_tuple_obj = RecordTuple(rec_obj1,rec_obj2)
                        rec_obj1.subscribe_exp_tuple(rec_obj2,rec_tuple_obj)
                        rec_obj2.subscribe_exp_tuple(rec_obj1,rec_tuple_obj)
                    else:
                        # get access to the existing record-tuple-object
                        rec_tuple_obj = rec_obj1.expanded_record_tuples[rec_obj2]
                    rec_tuple_obj.add_subscriber(self,mapped_cols)
                    print(f"{self.term_obj1.name},{self.term_obj2.name} subscribe to record {rec_tuple_obj.rec_obj1.rid},{rec_tuple_obj.rec_obj2.rid}")
                    self.sub_rids1.add(rec_obj1)
                    self.sub_rids2.add(rec_obj2)
                    self.sub_rids.add(rec_tuple_obj)

        self.compute_similarity()



    # if we want to destroy the term tuple, we need to make sure it is not linked to any thing anymore
    def unlink_from_term_parents(self):
        # pre
        self.term_obj1.attached_tuples.remove(self)
        self.term_obj2.attached_tuples.remove(self)

    def unlink_from_all_rid_tuples(self):
        for rid_obj in self.sub_rids:
            print(f"self destroy: {self.term_obj1.name},{self.term_obj2.name} from {rid_obj.rec_obj1.rid},{rid_obj.rec_obj2.rid}")
            rid_obj.remove_subscriber(self)

    '''def remove_subscription(self,rid_tuple):
        if rid_tuple.term_obj1 not in self.sub_rids1:
            raise  KeyError(rid_tuple.term_obj1)
        if rid_tuple.term_obj2 not in self.sub_rids2:
            raise KeyError(rid_tuple.term_obj2)
        
        self.sub_rids1[rid_tuple.rec_obj1].remove(rid_tuple)        
        self.sub_rids2[rid_tuple.rec_obj2].remove(rid_tuple)

        self.term_obj1.attached_tuples.remove(self)
        self.term_obj2.attached_tuples.remove(self)
    '''
    def get_similarity(self):
        return self.sim

    def get_records(self):
        return self.sub_rids1,self.sub_rids2,self.sub_rids


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




