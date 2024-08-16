import itertools
from src.Config_Files import Setup as Setup
from src.Classes.Records import RecordTuple


class Term:
    def __init__(self, term_name, file_name, col_ind, record):
        self.name = term_name
        # occurrences is a representation where each term appears
        # occurrences are of type {file_name1,cols1 : [record1, record2,...], file_name2,cols2 : [record3, record4,...]}
        self.occurrences = dict()
        self.attached_term_tuples = set()
        self.type = "int" if type(term_name) is int else "string"
        self.degree = 0
        self.db = record.db  # its good for debugging, to know to which side the term belongs
        self.gen_active = True

        self.update(file_name, col_ind, record)

    def deactivate_term_and_all_tt(self):
        self.gen_active = False
        if Setup.DEBUG or self in Setup.debug_set: print(f"deactivate Term of {self.db}: ({self.name})")

        for term_tuple in self.attached_term_tuples.copy():
            # term_tuple was deactivated before i.e. bc it has a similarity of 0
            if not term_tuple.is_active():
                self.attached_term_tuples.remove(term_tuple)
            else:  # otherwise deactivate now. we cannot destroy it yet bc. it needs to be removed from prio-dict first
                term_tuple.gen_active = False
                if Setup.DEBUG or term_tuple in Setup.debug_set:
                    print(f"deactivate Term Tuple: ({term_tuple.term1.name},{term_tuple.term2.name})")

        return self.attached_term_tuples

    def is_active(self):
        return self.gen_active

    def update(self, file_name, cols, record):
        # cols needs to be of type tuple bc. list is mutable object
        if type(cols) is list:
            cols = tuple(cols)
        key = (file_name, cols)
        self.occurrences.setdefault(key, set()).add(record)
        self.degree += 1

    def remove_occurrence(self, file_name, cols, rec_tuple):
        self.occurrences[file_name, cols].remove(rec_tuple)
        if not self.occurrences[file_name, cols]:
            del self.occurrences[file_name, cols]
        self.degree -= 1


# this will be 1 potential mapping consisting of two term-objects


class TermTuple:
    def __init__(self, term1, term2, expanded_record_tuples, similiarity_metric):
        self.term1 = term1
        self.term2 = term2
        self.term1.attached_term_tuples.add(self)
        self.term2.attached_term_tuples.add(self)
        self.gen_active = True
        self.sub_rids = dict()  # {record : set(rec_tuple1,rec_tuple2,...) }
        self.similiarity_metric = similiarity_metric
        self.sim = 0

        if term1.name in Setup.debug_term_names1:
            Setup.debug_set.add(term1)
            Setup.debug_set.add(self)
        if term2.name in Setup.debug_term_names2:
            Setup.debug_set.add(self)
            Setup.debug_set.add(term2)

        self.calc_initial_record_tuples(expanded_record_tuples)

    def is_active(self):
        return self.gen_active

    # this function is comes up with all record tuples, that a term pair could fulfill
    # it is only called once, when initialising the term-tuple
    def calc_initial_record_tuples(self, expanded_record_tuples):
        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = set(self.term1.occurrences.keys()) & set(self.term2.occurrences.keys())

        for file_name, mapped_cols in intersection:

            records_db1 = self.term1.occurrences[(file_name, mapped_cols)]
            records_db2 = self.term2.occurrences[(file_name, mapped_cols)]

            for record1, record2 in itertools.product(records_db1, records_db2):
                # in this case we would expand a side, where one record is inactive already
                if not record1.is_active() or not record2.is_active():
                    if Setup.DEBUG:
                        print(f"discarded record obj bc. it is inactive {record1.file_name}{record1.rid, record2.rid}")
                    continue

                if record1.is_in_process() and record2.is_in_process():
                    # if both records are individually in_process, but no active record tuple exist, skip
                    if (record1, record2) not in expanded_record_tuples.keys():
                        continue
                    else:
                        rec_tuple = expanded_record_tuples[(record1, record2)]
                        # check if the existing record_tuple is still active
                        if not rec_tuple.is_active():
                            continue

                elif not record1.is_in_process() and not record2.is_in_process():
                    if (record1, record2) not in expanded_record_tuples.keys():
                        rec_tuple = RecordTuple(record1, record2)
                        expanded_record_tuples[record1,record2] = rec_tuple
                    else:
                        rec_tuple = expanded_record_tuples[(record1, record2)]

                # Avoid record-tuples, where one side is in_process, when the other side is not
                else:
                    continue


                record1.record_tuples.add(rec_tuple)
                record2.record_tuples.add(rec_tuple)
                rec_tuple.add_subscriber(self, mapped_cols)
                if Setup.DEBUG or self.term1 in Setup.debug_set or self.term2 in Setup.debug_set:
                    print(
                        f"{self.term1.name},{self.term2.name}  subscribes to {record1.file_name}({rec_tuple.record1.rid},{rec_tuple.record2.rid}),(in_process1={rec_tuple.record1.is_in_process()}),(in_process2={rec_tuple.record2.is_in_process()})")
                    Setup.debug_set.add(rec_tuple)

                if record1.is_active() == record2.is_active():
                    record1.active_records_tuples.add(rec_tuple)
                    record2.active_records_tuples.add(rec_tuple)
                self.sub_rids.setdefault(record1, set()).add(rec_tuple)
                self.sub_rids.setdefault(record2, set()).add(rec_tuple)

    def recompute_similarity(self):
        self.get_clean_record_tuples()  # make sure, that all records & record_objects are still valid
        return self.compute_similarity()

    def compute_similarity(self):
        self.sim = self.similiarity_metric(self.term1, self.term2, self.sub_rids)
        return self.sim

    def get_similarity(self):
        return self.sim

    def get_clean_record_tuples(self):
        for record, rec_tuples in self.sub_rids.copy().items():
            # if record is deactivated, delete it
            if not record.is_active():
                del self.sub_rids[record]
                continue

            # if record-tuple is deactivated
            for rec_tuple in rec_tuples.copy():
                if not rec_tuple.is_active():
                    self.sub_rids[record].remove(rec_tuple)
                    if not self.sub_rids[record]:
                        del self.sub_rids[record]

        return self.sub_rids

    def accept_this_mapping(self):

        # update all record-tuples, that have now one (or more) less cells to fill
        remaining_records = set()
        finished_record_tuples = dict()
        for mapped_record in self.sub_rids.keys():
            is_finished = mapped_record.mark_filled_cols(self)
            remaining_records.add(mapped_record)
            if is_finished:
                ind1 = list()
                ind2 = list()
                rec_tuples = self.sub_rids[mapped_record]
                # TODO: it happens that with a finished record more than 1 record-tuple is considered finshed
                for rec_tuple in rec_tuples:
                    l1 = rec_tuple.record1.rid
                    l2 = rec_tuple.record2.rid
                    if l1 in ind1 or l2 in ind2:
                        raise ValueError(f"this record is marked already for fulfillment {mapped_record.file_name,l1,l2}")
                    else:
                        ind1.append(l1)
                        ind2.append(l2)
                    if Setup.DEBUG or rec_tuple in Setup.debug_set:
                        print(f"marked record for fill: {mapped_record.file_name}{l1,l2}")
                    finished_record_tuples.setdefault(mapped_record.file_name,set()).add((rec_tuple.record1.rid,rec_tuple.record2.rid))



        # find all mappings that are invalid now
        # in the expansion, they will be removed from prio-dict, otherwise we dont need to do anything to them, bc. either term1, or term2 is now mapped
        delete_term_tuples = set()
        delete_term_tuples |= self.term1.deactivate_term_and_all_tt()
        delete_term_tuples |= self.term2.deactivate_term_and_all_tt()
        delete_term_tuples.remove(self)  # we don't want to delete the current from the prio-dict

        # calculate possible record_objs (not tuples!) that are now invalid (they can never be mapped anymore)
        # and make them inactive

        # gather all records where the two terms are involved individually
        all_records = set()
        for records in itertools.chain(self.term1.occurrences.values(), self.term2.occurrences.values()):
            all_records |= records

        # calculate possible record_objs (not tuples!) that are now invalid (they can never be mapped anymore)
        destroy_record_objs = all_records - remaining_records

        #  deactivate  records and return subscribed term-tuples that need to be updated (since they lost a record-tuple)
        altered_term_tuples = set()
        for record in destroy_record_objs:

            if record.is_active():
                if Setup.DEBUG or self in Setup.debug_set:
                    print(f"deactivate  record: {record.db}{record.file_name, record.rid}")
                altered_term_tuples |= record.deactivate_self_and_all_rt()
        return delete_term_tuples, altered_term_tuples,finished_record_tuples

    def get_records(self):
        return self.sub_rids