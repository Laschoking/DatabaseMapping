import itertools
from enum import Enum
from pathlib import Path
#import Python.Config_Files.Setup as setup
import pandas as pd
from Python.Libraries import PathLib
from Python.Libraries import ShellLib
from sortedcontainers import SortedList,SortedDict
import csv
from collections import Counter
import os



class Record:
    def __init__(self, rid, file_name,db):
        self.rid = rid
        self.file_name = file_name
        self.col_len = 0
        self.db = db # denotes, if record belongs to db1 or db2
        self.vacant_cols = list()
        self.record_tuples = set()
        self.in_process = False
        self.gen_active = True

        self.active_records_tuples = set()  #of all record-tuple-objects that are active atm and contain (self)
        self.terms = list()  # we note all term_objs that contribute to the record

    def add_term(self,term):
        self.terms.append(term)
        self.vacant_cols.append(self.col_len)
        self.col_len += 1
        print(f"record {self.rid}, col len {self.col_len}")

    def get_active_record_tuples(self):
        for rec_tuple in self.active_records_tuples.copy():
            if not rec_tuple.is_active():
                self.active_records_tuples.remove(rec_tuple)
        return self.active_records_tuples
    def get_all_record_tuples(self):
        for rec_tuple in self.record_tuples.copy():
            if not rec_tuple.is_active():
                self.record_tuples.remove(rec_tuple)
        return self.record_tuples


    def mark_filled_cols(self, mapped_term_tuple): # this may be called several times
        if self.db == "facts-db1":
            mapped_term = mapped_term_tuple.term_obj1
        else:
            mapped_term = mapped_term_tuple.term_obj2
        if mapped_term not in self.terms:
            raise KeyError(f"{mapped_term.name} not terms {self.terms}")

        # reduce vacant cols by checking in which coll the mapped term appears
        self.vacant_cols = [i for i in self.vacant_cols if self.terms[i] != mapped_term]


        # check if the record is finised (matched with another one), then we can deactivate it
        if not self.vacant_cols:
            print(f"deactivate Record: {self.db,self.rid}")
            self.gen_active = False


    # if a record can not be matched anymore due to a recent mapping, we want to delete the occurrences of the terms within
    def deactivate_self_and_all_rt(self):
        self.gen_active = False
        self.in_process = False
        altered_term_tuples1 = set()
        print(f"delete Record ({self.db,self.rid})")
        # make all connected record-tuples inactive and save the term-tuples that were subscribed to them
        for rec_tuple in self.record_tuples:
            altered_term_tuples1 |= rec_tuple.make_inactive() # only sets bool (so term-tuple still may hold inactive record-tuples)
        term_cols = dict()

        i = 0
        for term_obj in self.terms:
            # makes sure we only update term_objs that are free (not mapped already)
            if not term_obj.is_active():
                term_cols.setdefault(term_obj, list()).append(i)
            i += 1

        # denotes all record_objs where the term is subscribed
        # we want to remove this (self) record from the occurrences
        altered_term_tuples2 = set()
        for term_obj,cols in term_cols.items():
            if term_obj.is_active():
                # remove this record_obj from the occurrences of the term, because it is now obsolete
                print(f"delete occurrence ({self.file_name},{self.rid}) from {term_obj.name} at col {cols}")
                term_obj.remove_occurrence(self.file_name,tuple(cols),self)

            # those terms will later receive a new similarity based on the deleted occurrences
            altered_term_tuples2 |= term_obj.attached_term_tuples
        print(altered_term_tuples2 ^ altered_term_tuples2) # i dont think they are different
        return altered_term_tuples1

    def is_active(self):
        return self.gen_active

    def is_in_process(self):
        return self.in_process

class RecordTuple:
    def __init__(self, rec_obj1, rec_obj2):
        self.rec_obj1 = rec_obj1
        self.rec_obj2 = rec_obj2
        self.subscribers = set()
        self.gen_active = True # means it is not dead



    def is_active(self):
        return self.gen_active
    def add_subscriber(self, term_tuple, mapped_col):
        self.subscribers.add(term_tuple)

    def get_subscribers(self):
        for term_tuple in self.subscribers.copy():
            if not term_tuple.gen_active:
                self.subscribers.remove(term_tuple)
        return self.subscribers


    def make_inactive(self):
        self.gen_active = False
        print(f"deactivate Record Tuple: ({self.rec_obj1.rid},{self.rec_obj2.rid})")
        return self.get_subscribers()


class Term:
    def __init__(self, term_name, file_name,col_ind,rec_obj):
        self.name = term_name
        # occurrences is a representation where each term appears
        # occurrences are of type {file_name1,cols1 : [rec_obj1, rec_obj2,...], file_name2,cols2 : [rec_obj3, rec_obj4,...]}
        self.occurrences = dict()
        self.attached_term_tuples = set()
        self.type = "int" if type(term_name) is int else "string"
        self.degree = 0
        self.gen_active = True

        self.update(file_name,col_ind,rec_obj)

    def deactivate_term_and_all_tt(self):
        self.gen_active = False
        print(f"deactivate Term : ({self.name})")

        for term_tuple in self.attached_term_tuples.copy():
            # term_tuple was deactivated before i.e. bc it has a similarity of 0
            if not term_tuple.is_active():
                self.attached_term_tuples.remove(term_tuple)
            term_tuple.gen_active = False
            print(f"deactivate Term Tuple: ({term_tuple.term_obj1.name},{term_tuple.term_obj2.name})")

        return self.attached_term_tuples

    def is_active(self):
        return self.gen_active

    def update(self,file_name,cols,rec_obj):
        # cols needs to be of type tuple bc. list is mutable object
        if type(cols) is list:
            cols = tuple(cols)
        key = (file_name,cols)
        self.occurrences.setdefault(key,set()).add(rec_obj)
        self.degree += 1

    def remove_occurrence(self,file_name,cols,rec_tuple):
        self.occurrences[file_name,cols].remove(rec_tuple)
        if not self.occurrences[file_name,cols]:
            del self.occurrences[file_name,cols]
        self.degree -= 1
# this will be 1 potential mapping consisting of two term-objects
class TermTuple:
    def __init__(self,term_obj1, term_obj2, expanded_record_tuples,similiarity_metric):
        self.term_obj1 = term_obj1
        self.term_obj2 = term_obj2
        self.term_obj1.attached_term_tuples.add(self)
        self.term_obj2.attached_term_tuples.add(self)
        self.gen_active = True
        self.sub_rids = dict() # {rec_obj : set(rec_tuple1,rec_tuple2,...) }
        self.similiarity_metric = similiarity_metric
        self.sim = 0
        self.calc_initial_record_tuples(expanded_record_tuples)


    def is_active(self):
        return self.gen_active

    # this function is comes up with all record tuples, that a term pair could fulfill
    # it is only called once, when initialising the term-tuple
    def calc_initial_record_tuples(self, expanded_record_tuples):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = set(self.term_obj1.occurrences.keys()) & set(self.term_obj2.occurrences.keys())
        if self.term_obj1.name == "G":
            print("sd")

        for file_name, mapped_cols in intersection:

            rec_objs1 = self.term_obj1.occurrences[(file_name,mapped_cols)]
            rec_objs2 = self.term_obj2.occurrences[(file_name,mapped_cols)]

            for rec_obj1,rec_obj2 in itertools.product(rec_objs1,rec_objs2):

                if rec_obj1.is_in_process() != rec_obj2.is_in_process():
                    # this means we would expand a record-tuple, where one side of the record-tuple is already activated by a mapping
                    # but the other side not (i.e. is_active-mapping(1,2) and we want to introduce new mapping (3,2) will never work
                    continue

                # both record-id1 and record-id2 point to the same object Record-Tuple (consisting of record-id1 and record-id2, etc.)
                if (rec_obj1,rec_obj2) not in expanded_record_tuples.keys():
                    rec_tuple_obj = RecordTuple(rec_obj1,rec_obj2)
                    # expanded_record_tuples is a global dictionary linking rec_obj1,rec_obj2 to the record-tuple
                    expanded_record_tuples[(rec_obj1,rec_obj2)] = rec_tuple_obj
                else:
                    # get access to the existing record-tuple-object
                    rec_tuple_obj = expanded_record_tuples[(rec_obj1,rec_obj2)]
                rec_obj1.record_tuples.add(rec_tuple_obj)
                rec_obj2.record_tuples.add(rec_tuple_obj)
                rec_tuple_obj.add_subscriber(self,mapped_cols)
                print(f"{self.term_obj1.name},{self.term_obj2.name}  subscribes to (in_process={rec_tuple_obj.rec_obj1.is_in_process()}) ({rec_tuple_obj.rec_obj1.rid},{rec_tuple_obj.rec_obj2.rid})")
                if rec_obj1.is_active() == rec_obj2.is_active():
                    rec_obj1.active_records_tuples.add(rec_tuple_obj)
                    rec_obj2.active_records_tuples.add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj1,set()).add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj2, set()).add(rec_tuple_obj)

    def recompute_similarity(self):
        self.clean_record_tuple() #make sure, that all records & record_objects are still valid
        return self.compute_similarity()
    def compute_similarity(self):
        self.sim =  self.similiarity_metric(self.term_obj1, self.term_obj2, self.sub_rids)
        return self.sim

    def get_similarity(self):
        return self.sim

    def clean_record_tuple(self):
        for rec_obj, rec_tuples in self.sub_rids.copy().items():
            if not rec_obj.is_active():
                del self.sub_rids[rec_obj]
                continue
            for rec_tuple in rec_tuples.copy():
                if not rec_tuple.is_active():
                    self.sub_rids[rec_obj].remove(rec_tuple)
                    if not self.sub_rids[rec_obj]:
                        del self.sub_rids[rec_obj]

        return self.sub_rids

    def accept_this_mapping(self):

        # update all record-tuples, that have now one (or more) less cells to fill
        for mapped_rec_obj in self.sub_rids.keys():
            mapped_rec_obj.mark_filled_cols(self)

        # find all mappings that are invalid now
        # in the expansion, they will be removed from prio-dict, otherwise we dont need to do anything to them, bc. either term_obj1, or term_obj2 is now mapped
        delete_term_tuples = set()
        delete_term_tuples |= self.term_obj1.deactivate_term_and_all_tt()
        delete_term_tuples |= self.term_obj2.deactivate_term_and_all_tt()
        delete_term_tuples.remove(self) # we don't want to delete the current from the prio-dict

        # calculate possible record_objs (not tuples!) that are now invalid (they can never be mapped anymore)
        # and make them inactive
        destroy_occ1 = set(self.term_obj1.occurrences.keys()).difference(self.term_obj2.occurrences.keys())
        destroy_occ2 = set(self.term_obj2.occurrences.keys()).difference(self.term_obj1.occurrences.keys())

        destroy_record_objs = set()
        for (file_name, col) in destroy_occ1:
            destroy_record_objs.update(self.term_obj1.occurrences[file_name, col])

        for (file_name, col) in destroy_occ2:
            destroy_record_objs.update(self.term_obj2.occurrences[file_name, col])

        # this will deactivate all record-objects and return those term-tuples that need to be updated (since a record-tuple was deleted from them)
        altered_term_tuples = set()
        for rec_obj in destroy_record_objs:
            if rec_obj.is_active():
                altered_term_tuples |= rec_obj.deactivate_self_and_all_rt()
        return delete_term_tuples,altered_term_tuples



    def get_records(self):
        return self.sub_rids
    '''
    def unlink_from_rid_tuple(self,rid_obj):

        print(f"unlink ({self.term_obj1.name},{self.term_obj2.name}) from  ({rid_obj.rec_obj1.rid},{rid_obj.rec_obj2.rid})")
        self.sub_rids[rid_obj.rec_obj1].remove(rid_obj)
        self.sub_rids[rid_obj.rec_obj2].remove(rid_obj)

        if not self.sub_rids[rid_obj.rec_obj1]:
            del self.sub_rids[rid_obj.rec_obj1]
        if not self.sub_rids[rid_obj.rec_obj2]:
            del self.sub_rids[rid_obj.rec_obj2]


    # usually we unlink one or several record-tuples
    # here we want to unlink all record-tuples that have record_obj as left or right
    def unlink_record(self,record_obj):
        useless_rec_tuples = set()
        if record_obj in self.sub_rids:
            del self.sub_rids[record_obj]

            print(f"removed all record_tuples from sub_rids1 & sub_rids connected to {record_obj.db,record_obj.file_name,record_obj.rid}")

        if record_obj in self.sub_rids2:
            self.sub_rids -= self.sub_rids2[record_obj]

            del self.sub_rids2[record_obj]

        print(f"removed all record_tuples from sub_rids2 & sub_rids connected to {record_obj.db,record_obj.file_name,record_obj.rid}")
    '''
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

        self.db1_original_facts = DB_Instance(self.paths.db1_facts, "db1")
        self.db2_original_facts = DB_Instance(self.paths.db2_facts, "db2")

        # origin for separate Program Analysis without Bijection
        self.db1_original_results = DB_Instance(self.paths.db1_results, "db1")
        self.db2_original_results = DB_Instance(self.paths.db2_results, "db2")


        self.mappings = []

    def add_mapping(self, mapping):
        self.mappings.append(mapping)


    def log_terms(self):
        terms1_df = pd.Series(self.terms1.keys())
        if not self.paths.terms1.exists():
            terms1_df.to_csv(self.paths.terms1,sep='\t',index=False,header=False)

        terms2_df = pd.Series(self.terms2.keys())
        if not self.paths.terms2.exists():
            terms2_df.to_csv(self.paths.terms2,sep='\t',index=False,header=False)




