import src.Config_Files.Setup as setup


class Record:
    def __init__(self, rid, file_name,db,col_len):
        self.rid = rid
        self.file_name = file_name
        self.col_len = col_len
        self.db = db # denotes, if record belongs to db1 or db2
        self.vacant_cols = list(range(col_len))
        self.record_tuples = set()
        self.in_process = False
        self.gen_active = True

        self.active_records_tuples = set()  #of all record-tuple-objects that are active atm and contain (self)
        self.terms = [None] * col_len  # we note all term_objs that contribute to the record

    def add_term(self,term,cols):
        # if cols contains more than one col, we need to insert the term at the correct place
        for col in cols:
            # overwrite shallow terms list
            self.terms[col] = term
            #if setup.debug: print(f"record {self.rid} insert term {term} with col_len {self.col_len}")

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
            #if setup.debug: print(f"deactivate Record: {self.db,self.rid}")
            self.gen_active = False


    # if a record can not be matched anymore due to a recent mapping, we want to delete the occurrences of the terms within
    def deactivate_self_and_all_rt(self):
        self.gen_active = False
        self.in_process = False
        altered_term_tuples1 = set()
        #if setup.debug: print(f"delete Record ({self.db,self.rid})")
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
                if setup.debug or term_obj in setup.debug_term_names: print(f"delete occurrence ({self.file_name},{self.rid}) from {term_obj.name} at col {cols}")
                term_obj.remove_occurrence(self.file_name,tuple(cols),self)

            # those terms will later receive a new similarity based on the deleted occurrences
            altered_term_tuples2 |= term_obj.attached_term_tuples

        if len(altered_term_tuples2 ^ altered_term_tuples2) > 0:
            raise ValueError(f"sets should be of same length, {altered_term_tuples1} and {altered_term_tuples2}")
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
        if setup.debug or self in setup.debug_set:
            print(f"deactivate Record Tuple: {self.rec_obj1.file_name}({self.rec_obj1.rid},{self.rec_obj2.rid})")
        return self.get_subscribers()





