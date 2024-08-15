import src.Config_Files.Setup as Setup


class Record:
    def __init__(self, rid, file_name, db, col_len):
        self.rid = rid
        self.file_name = file_name
        self.col_len = col_len
        self.db = db  # denotes, if record belongs to db1 or db2
        self.vacant_cols = list(range(col_len))
        self.record_tuples = set()
        self.in_process = False
        self.gen_active = True

        self.active_records_tuples = set()  # of all record-tuple-objects that are active atm and contain (self)
        self.terms = [None] * col_len  # we note all terms that contribute to the record

    def add_term(self, term, cols):
        # if cols contains more than one col, we need to insert the term at the correct place
        for col in cols:
            # overwrite shallow terms list
            self.terms[col] = term
            # if Setup.debug: print(f"record {self.rid} insert term {term} with col_len {self.col_len}")

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

    def mark_filled_cols(self, mapped_term_tuple):  # this may be called several times
        if self.db == "facts-db1":
            mapped_term = mapped_term_tuple.term1
        else:
            mapped_term = mapped_term_tuple.term2
        if mapped_term not in self.terms:
            raise KeyError(f"{mapped_term.name} not terms {self.terms}")

        # reduce vacant cols by checking in which coll the mapped term appears
        self.vacant_cols = [i for i in self.vacant_cols if self.terms[i] != mapped_term]

        # check if the record is finised (matched with another one), then we can deactivate it
        if not self.vacant_cols:
            # if Setup.debug: print(f"deactivate Record: {self.db,self.rid}")
            self.gen_active = False

    # if a record can not be matched anymore due to a recent mapping, we want to delete the occurrences of the terms within
    def deactivate_self_and_all_rt(self):
        self.gen_active = False
        self.in_process = False
        altered_term_tuples = set()
        # if Setup.debug: print(f"delete Record ({self.db,self.rid})")
        # make all connected record-tuples inactive and save the term-tuples that were subscribed to them
        for rec_tuple in self.record_tuples:
            altered_term_tuples |= rec_tuple.make_inactive()  # only sets bool (so term-tuple still may hold inactive record-tuples)
        term_cols = dict()

        i = 0

        for term in self.terms:
            # makes sure we only update terms that are free (not mapped already)
            if not term.is_active():
                term_cols.setdefault(term, list()).append(i)
            i += 1

        # denotes all record_objs where the term is subscribed
        # we want to remove this (self) record from the occurrences
        for term, cols in term_cols.items():
            if term.is_active():
                # remove this record_obj from the occurrences of the term, because it is now obsolete
                if Setup.DEBUG or term in Setup.debug_term_names1 or term in Setup.debug_term_names2:
                    print(f"delete occurrence ({self.file_name},{self.rid}) from {term.name} at col {cols}")
                term.remove_occurrence(self.file_name, tuple(cols), self)

        return altered_term_tuples

    def is_active(self):
        return self.gen_active

    def is_in_process(self):
        return self.in_process


class RecordTuple:
    def __init__(self, record1, record2):
        self.record1 = record1
        self.record2 = record2
        self.subscribers = set()
        self.gen_active = True  # means it is not dead

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
        if Setup.DEBUG or self in Setup.debug_set:
            print(f"deactivate Record Tuple: {self.record1.file_name}({self.record1.rid},{self.record2.rid})")
        return self.get_subscribers()