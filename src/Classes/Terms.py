import itertools
from src.Config_Files import Setup as setup
from src.Classes.Records import RecordTuple


class Term:
    def __init__(self, term_name, file_name,col_ind,rec_obj):
        self.name = term_name
        # occurrences is a representation where each term appears
        # occurrences are of type {file_name1,cols1 : [rec_obj1, rec_obj2,...], file_name2,cols2 : [rec_obj3, rec_obj4,...]}
        self.occurrences = dict()
        self.attached_term_tuples = set()
        self.type = "int" if type(term_name) is int else "string"
        self.degree = 0
        self.db = rec_obj.db # its good for debugging, to know to which side the term belongs
        self.gen_active = True

        self.update(file_name,col_ind,rec_obj)

    def deactivate_term_and_all_tt(self):
        self.gen_active = False
        if setup.debug or self in setup.debug_set: print(f"deactivate Term of {self.db}: ({self.name})")


        for term_tuple in self.attached_term_tuples.copy():
            # term_tuple was deactivated before i.e. bc it has a similarity of 0
            if not term_tuple.is_active():
                self.attached_term_tuples.remove(term_tuple)
            else: # otherwise deactivate now. we cannot destroy it yet bc. it needs to be removed from prio-dict first
                term_tuple.gen_active = False
                if setup.debug or term_tuple in setup.debug_set:
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

        if term_obj1.name in setup.debug_term_names1:
            setup.debug_set.add(term_obj1)
            setup.debug_set.add(self)
        if term_obj2.name in setup.debug_term_names2:
            setup.debug_set.add(self)
            setup.debug_set.add(term_obj2)

        self.calc_initial_record_tuples(expanded_record_tuples)


    def is_active(self):
        return self.gen_active

    # this function is comes up with all record tuples, that a term pair could fulfill
    # it is only called once, when initialising the term-tuple
    def calc_initial_record_tuples(self, expanded_record_tuples):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = set(self.term_obj1.occurrences.keys()) & set(self.term_obj2.occurrences.keys())

        for file_name, mapped_cols in intersection:

            rec_objs1 = self.term_obj1.occurrences[(file_name,mapped_cols)]
            rec_objs2 = self.term_obj2.occurrences[(file_name,mapped_cols)]

            for rec_obj1,rec_obj2 in itertools.product(rec_objs1,rec_objs2):
                # in this case we would expand a side, where one record is inactive already
                if not rec_obj1.is_active() or not rec_obj2.is_active():
                    if setup.debug:
                        print(f"discarded record obj bc. it is inactive {rec_obj1.rid} or {rec_obj2.rid}")
                    continue

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

                    # check if the existing record_tuple is still active
                    if not rec_tuple_obj.is_active():
                        continue

                rec_obj1.record_tuples.add(rec_tuple_obj)
                rec_obj2.record_tuples.add(rec_tuple_obj)
                rec_tuple_obj.add_subscriber(self,mapped_cols)
                if setup.debug or self.term_obj1 in setup.debug_set or self.term_obj2 in setup.debug_set:
                    print(f"{self.term_obj1.name},{self.term_obj2.name}  subscribes to {rec_obj1.file_name}(in_process={rec_tuple_obj.rec_obj1.is_in_process()}) ({rec_tuple_obj.rec_obj1.rid},{rec_tuple_obj.rec_obj2.rid})")
                    setup.debug_set.add(rec_tuple_obj)

                if rec_obj1.is_active() == rec_obj2.is_active():
                    rec_obj1.active_records_tuples.add(rec_tuple_obj)
                    rec_obj2.active_records_tuples.add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj1,set()).add(rec_tuple_obj)
                self.sub_rids.setdefault(rec_obj2, set()).add(rec_tuple_obj)

    def recompute_similarity(self):
        self.get_clean_record_tuples() #make sure, that all records & record_objects are still valid
        return self.compute_similarity()
    def compute_similarity(self):
        self.sim =  self.similiarity_metric(self.term_obj1, self.term_obj2, self.sub_rids)
        return self.sim

    def get_similarity(self):
        return self.sim

    def get_clean_record_tuples(self):
        for rec_obj, rec_tuples in self.sub_rids.copy().items():
            # if record is deactivated, delete it
            if not rec_obj.is_active():
                del self.sub_rids[rec_obj]
                continue

            # if record-tuple is deactivated
            for rec_tuple in rec_tuples.copy():
                if not rec_tuple.is_active():
                    self.sub_rids[rec_obj].remove(rec_tuple)
                    if not self.sub_rids[rec_obj]:
                        del self.sub_rids[rec_obj]

        return self.sub_rids

    def accept_this_mapping(self):
        if self.term_obj1.name == "t4":
            print("t4")

        # update all record-tuples, that have now one (or more) less cells to fill
        remaining_rec_objs = set()
        for mapped_rec_obj in self.sub_rids.keys():
            mapped_rec_obj.mark_filled_cols(self)
            remaining_rec_objs.add(mapped_rec_obj)


        # find all mappings that are invalid now
        # in the expansion, they will be removed from prio-dict, otherwise we dont need to do anything to them, bc. either term_obj1, or term_obj2 is now mapped
        delete_term_tuples = set()
        delete_term_tuples |= self.term_obj1.deactivate_term_and_all_tt()
        delete_term_tuples |= self.term_obj2.deactivate_term_and_all_tt()
        delete_term_tuples.remove(self) # we don't want to delete the current from the prio-dict

        # calculate possible record_objs (not tuples!) that are now invalid (they can never be mapped anymore)
        # and make them inactive
        # TODO: die Bestimmung der Overlaps ist outdated (z.B. wenn t1,t2 in der gleichen Spalte & Relation vorkommen, wird kein Record zerstört
        # TODO: das gilt aber auch, wenn die Verbindung durch ein Record tupel gar nicht existiert
        
        # gather all records where the two terms are involved individually
        all_rec_objs = set()
        for rec_objs in itertools.chain(self.term_obj1.occurrences.values(),self.term_obj2.occurrences.values()):
            all_rec_objs |= rec_objs
        #all_rec_objs.update(rec_objs for rec_objs in self.term_obj1.occurrences.values())
        #all_rec_objs.update(rec_objs for rec_objs in self.term_obj2.occurrences.values())
        destroy_record_objs = all_rec_objs - remaining_rec_objs

        # this will deactivate all record-objects and return those term-tuples that need to be updated (since a record-tuple was deleted from them)
        altered_term_tuples = set()
        for rec_obj in destroy_record_objs:

            if rec_obj.is_active():
                if setup.debug or self in setup.debug_set:
                    print(f"deactivate  record: {rec_obj.db}({rec_obj.file_name, rec_obj.rid})")
                altered_term_tuples |= rec_obj.deactivate_self_and_all_rt()
        return delete_term_tuples,altered_term_tuples



    def get_records(self):
        return self.sub_rids