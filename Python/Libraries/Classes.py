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
        self.active = False
        self.active_records_tuples = set()  # set of all record-tuple-objects that are active atm and contain (self)
        self.terms = list()  # we note all term_objs that contribute to the record

    def add_term(self,term):
        self.terms.append(term)
        self.vacant_cols.append(self.col_len)
        self.col_len += 1

    def get_active_records_tuples(self):
        for rec_tuple in self.active_records_tuples.copy():
            if rec_tuple.is_dead():
                self.active_records_tuples.remove(rec_tuple)
        return self.active_records_tuples

    def is_active(self):
        return self.active

    def set_active(self):
        print(f"activate record Tuple {self.rid}")
        if self.active:
            raise ValueError(f"boolean was active already: {self.rid,self.file_name}")
        self.active = True

    def add_record_tuple(self, rid_tuple):
        self.record_tuples.add(rid_tuple)

    def mark_filled_cols(self,mapped_term):
        # reduce vacant cols by checking in which coll the mapped term appears
        self.vacant_cols = [i for i in self.vacant_cols if self.terms[i] != mapped_term]
        return len(self.vacant_cols)


    # if a record can not be matched anymore due to a recent mapping, we want to delete the occurrences of the terms within
    def deactivate_self(self,mapped_tuple):
        self.active = False
        for rec_tuple in self.record_tuples:
            rec_tuple.make_inactive()
        altered_term_tuples = set()
        term_cols = dict()
        i = 0
        for term_obj in self.terms:
            # makes sure we only update term_objs that are free (not mapped already)
            if not term_obj.is_mapped():
                term_cols.setdefault(term_obj, list()).append(i)
            i += 1

            # denotes all record_objs where the term is subscribed
            # we want to remove this (self) record from the occurrences
        for term_obj,cols in term_cols.items():
            # remove this record_obj from the occurrences of the term, because it is now obsolete
            print(f"delete occurrence ({self.file_name},{self.rid}) from {term_obj.name} at col {cols}")
            term_obj.remove_occurrence(self.file_name,tuple(cols),self)

            for term_tuple in term_obj.attached_term_tuples:
                term_tuple.unlink_record(self)

            # those terms will later receive a new similarity based on the deleted occurrences
            altered_term_tuples |= term_obj.attached_term_tuples
        return altered_term_tuples

class RecordTuple:
    def __init__(self, rec_obj1, rec_obj2):
        self.rec_obj1 = rec_obj1
        self.rec_obj2 = rec_obj2
        self.subscribers = set()
        self.dead = False # means it is not dead

    def mark_filled_cols(self, term_tuple):
        if term_tuple not in self.subscribers:
            raise KeyError(f"{term_tuple.term_obj1.name}, {term_tuple.term_obj2.name} is not {self.rec_obj1.rid},{self.rec_obj2.rid}")

        # one Term Tuple can satisfy several cols of the same record-combination (i.e. A1(a,a,b)  & A2(a,a,c))

        l1 = self.rec_obj1.mark_filled_cols(term_tuple.term_obj1)
        l2 = self.rec_obj2.mark_filled_cols(term_tuple.term_obj2)
        if l1 != l2:
            raise ValueError(f"both records should be filled the same amount")

        # if True the record is finished (no terms are vacant anymore)
        # hence we can deactivate it
        if l1 == 0:
            self.make_inactive()

    def is_dead(self):
        return self.dead


    def add_subscriber(self, term_tuple, mapped_col):
        self.subscribers.add(term_tuple)

    def get_subscribers(self):
        for term_tuple in self.subscribers:
            if not term_tuple.is_active():
                self.subscribers.pop(term_tuple)
        return self.subscribers

    def remove_subscriber(self,term_tuple):
        if term_tuple not in self.subscribers:
            KeyError(" Term Tuple does not exist: " + str(term_tuple))
        self.subscribers.remove(term_tuple)

    def make_inactive(self):
        self.dead = True
        altered_subscribers = set()
        # unsubscribe  term-tuples
        for sub_term in self.subscribers.copy():
            #if not sub_term.is_mapped():
            #sub_term.unlink_from_rid_tuple(self)
            altered_subscribers.add(sub_term)
        self.subscribers = set()
        # unsubscribe self from record-object
        '''for rec_obj in self.rec_obj1, self.rec_obj2:
            if self not in rec_obj.active_records_tuples:
                raise KeyError(f" Record Tuple {rec_obj.rid},{rec_obj.rid} does not occur in active records")
            rec_obj.active_records_tuples.remove(self)
        '''
        return altered_subscribers

    # fokus auf neuberechnung von Term-Tupeln, kein unlinking über Record-Tupel
    # einfach Check, ob Term-Tupel noch aktiv


class Term:
    def __init__(self, term_name, file_name,col_ind,rec_obj):
        self.name = term_name
        self.occurrences = dict()
        self.attached_term_tuples = set()
        self.type = "int" if type(term_name) is int else "string"
        self.degree = 0
        self.mapped = False

        self.update(file_name,col_ind,rec_obj)

    def is_mapped(self):
        return self.mapped
    def set_mapped(self):
        self.mapped = True
        return self.attached_term_tuples

    # one occurence has the following structure: file_name,cols,row_nr
    # the collection is of following structure {(file_name,cols) : [row_nr1,row_nr2, ...]}
    # this way, all row_nr are stored together, but with file_name,cols as keys
    # those keys can be used for later set-operations while mapping
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

# this will be 1 potential mapping
class TermTuple:
    def __init__(self,term_obj1, term_obj2, expanded_record_tuples,similiarity_metric):
        self.term_obj1 = term_obj1
        term_obj1.attached_term_tuples.add(self)
        term_obj2.attached_term_tuples.add(self)
        self.active = True
        self.term_obj2 = term_obj2
        self.sub_rids = dict() # {rec_obj : set(rec_tuple1,rec_tuple2,...) }
        #self.sub_rids = set()
        self.destroy_record_objs = set() # keeps all record-objects that are destroyed (never matched) after applying this mapping
        self.similiarity_metric = similiarity_metric
        self.sim = 0
        self.calc_initial_record_tuples(expanded_record_tuples)


    def is_active(self):
        return self.active

    def set_inactive(self):
        self.active = False

    # this function is comes up with all record tuples, that a term pair could fulfill
    # it is only called once, when initialising the term-tuple
    def calc_initial_record_tuples(self, expanded_record_tuples):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = set(self.term_obj1.occurrences.keys()) & set(self.term_obj2.occurrences.keys())
        destroy_records1 = set(self.term_obj1.occurrences.keys()) - intersection
        destroy_records2 = set(self.term_obj2.occurrences.keys()) - intersection
        # take (file_name,col) that will not get matched, find all record-identifier belonging to it & return the record_object for each combination
        for (file_name, col) in destroy_records1:
            self.destroy_record_objs.update(self.term_obj1.occurrences[file_name, col])
        #self.destroy_record_objs.update(self.term_obj1.occurrences[file_name,col] for (file_name,col) in destroy_records1)

        for (file_name, col) in destroy_records2:
            self.destroy_record_objs.update(self.term_obj2.occurrences[file_name, col])
        #self.destroy_record_objs.update(self.term_obj2.occurrences[file_name,col] for file_name,col in destroy_records2)

        for file_name, mapped_cols in intersection:

            rec_objs1 = self.term_obj1.occurrences[(file_name,mapped_cols)]
            rec_objs2 = self.term_obj2.occurrences[(file_name,mapped_cols)]

            for rec_obj1,rec_obj2 in itertools.product(rec_objs1,rec_objs2):

                if rec_obj1.is_active() != rec_obj2.is_active():
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
                print(f"{self.term_obj1.name},{self.term_obj2.name}  subscribes to (active={rec_tuple_obj.rec_obj1.is_active()}) ({rec_tuple_obj.rec_obj1.rid},{rec_tuple_obj.rec_obj2.rid})")
                if rec_obj1.is_active() == rec_obj2.is_active():
                    rec_obj1.active_records_tuples.add(rec_tuple_obj)
                    rec_obj2.active_records_tuples.add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj1,set()).add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj2, set()).add(rec_tuple_obj)

    def compute_similarity(self):
        self.sim = self.similiarity_metric(self.term_obj1, self.term_obj2, self.sub_rids)
        return self.sim

    def get_similarity(self):
        return self.sim

    def accept_this_mapping(self):
        # set both terms as mapped
        self.term_obj1.is_mapped()
        self.term_obj2.is_mapped()

        # update all record-tuples, that have now one (or more) less cells to fill
        for mapped_rec_tuple in set(self.sub_rids.values()):
            # the record-tuple has the mapped_tuple as a subscriber, and
            mapped_rec_tuple.mark_filled_cols(self)

        # find all mappings that are invalid now
        remove_term_tuples = set()
        remove_term_tuples |= self.term_obj1.set_mapped()
        remove_term_tuples |= self.term_obj2.set_mapped()

        #

        return remove_term_tuples



    def get_records(self):
        return self.sub_rids

    '''
    # if we want to destroy the term tuple, we need to make sure it is not linked to any thing anymore
    def unlink_from_term_parents(self):
        self.term_obj1.attached_term_tuples.remove(self)
        self.term_obj2.attached_term_tuples.remove(self)
        print(f" delete {self.term_obj1.name},{self.term_obj2.name}")

    def unlink_from_all_rid_tuples(self):
        for rid_obj in self.sub_rids.copy():
            self.unlink_from_rid_tuple(rid_obj)
    
    def unlink_from_rid_tuple(self,rid_obj):
        if rid_obj in self.sub_rids:
            print(f"unlink ({self.term_obj1.name},{self.term_obj2.name}) from  ({rid_obj.rec_obj1.rid},{rid_obj.rec_obj2.rid})")
            rid_obj.remove_subscriber(self)
            self.sub_rids.remove(rid_obj)
            self.sub_rids1[rid_obj.rec_obj1].remove(rid_obj)
            self.sub_rids2[rid_obj.rec_obj2].remove(rid_obj)
            if not self.sub_rids1[rid_obj.rec_obj1]:
                del self.sub_rids1[rid_obj.rec_obj1]
            if not self.sub_rids2[rid_obj.rec_obj2]:
                del self.sub_rids2[rid_obj.rec_obj2]
        else:
            print(f"({self.term_obj1.name},{self.term_obj2.name}) is not linked to  ({rid_obj.rec_obj1.rid},{rid_obj.rec_obj2.rid})")
    '''


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




