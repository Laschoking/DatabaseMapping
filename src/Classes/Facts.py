from src.Config_Files.Debug_Flags import DEBUG_TERMS, DEBUG_RECORDS,debug_set, debug_element_names1,debug_element_names2


class Fact:
    def __init__(self, index, file, db, col_len):
        self.index = index
        self.file = file
        self.col_len = col_len
        self.db = db  # denotes, if record belongs to db1 or db2
        self.vacant_cols = list(range(col_len))
        self.fact_pairs = set()
        self.in_process = False
        self.gen_active = True

        self.active_fact_pairs = set()  # of all record-tuple-objects that are active atm and contain (self)
        self.elements = [None] * col_len  # we note all elements that contribute to the record

    def add_element(self, element, cols):
        # if cols contains more than one col, we need to insert the element at the correct place
        for col in cols:
            # overwrite shallow elements list
            self.elements[col] = element

    def get_active_fact_pairs(self):
        for fact_pair in self.active_fact_pairs.copy():
            if not fact_pair.is_active():
                self.active_fact_pairs.remove(fact_pair)
        return self.active_fact_pairs

    def get_all_fact_pairs(self):
        for fp in self.fact_pairs.copy():
            if not fp.is_active():
                self.fact_pairs.remove(fp)
        return self.fact_pairs

    def mark_filled_cols(self, mapping):  # this may be called several times
        if self.db == "facts-db1":
            mapped_elem = mapping.element1
        else:
            mapped_elem = mapping.element2
        if mapped_elem not in self.elements:
            raise KeyError(f"{mapped_elem.name} not elements {self.elements}")

        # reduce vacant cols by checking in which coll the mapped element appears
        self.vacant_cols = [i for i in self.vacant_cols if self.elements[i] != mapped_elem]

        # check if the record is finised (matched with another one), then we can deactivate it
        if not self.vacant_cols:
            # if debug: print(f"deactivate Record: {self.db,self.index}")
            self.gen_active = False
            return True
        else:
            return False

    # if a record can not be matched anymore due to a recent mapping_func, we want to delete the occurrences of the elements within
    def deactivate_self_and_all_rt(self):
        self.gen_active = False
        self.in_process = False
        altered_mappings = set()
        # if debug: print(f"delete Record ({self.db,self.index})")
        # make all connected record-tuples inactive and save the element-tuples that were subscribed to them
        for fact_pair in self.fact_pairs:
            altered_mappings |= fact_pair.make_inactive()  # only sets bool (so element-tuple still may hold inactive record-tuples)
        element_cols = dict()

        i = 0

        for element in self.elements:
            # makes sure we only update elements that are free (not mapped already)
            if not element.is_active():
                element_cols.setdefault(element, list()).append(i)
            i += 1

        # denotes all record_objs where the element is subscribed
        # we want to remove this (self) record from the occurrences
        for element, cols in element_cols.items():
            if element.is_active():
                # remove this record_obj from the occurrences of the element, because it is now obsolete
                if DEBUG_RECORDS or element in debug_element_names1 or element in debug_element_names2:
                    print(f"delete occurrence ({self.file},{self.index}) from {element.name} at col {cols}")
                element.remove_occurrence(self.file, tuple(cols), self)

        return altered_mappings

    def is_active(self):
        return self.gen_active

    def is_in_process(self):
        return self.in_process


class FactPair:
    def __init__(self, fact1, fact2):
        self.fact1 = fact1
        self.fact2 = fact2
        self.subscribers = set()
        self.gen_active = True  # means it is not dead

    def __lt__(self, other):
        if self.fact1.file < other.fact1.file:
            return True
        elif self.fact1.file > other.fact1.file:
            return False
        elif self.fact1.index < other.fact1.index:
            return True
        elif self.fact1.index > other.fact1.index:
            return False
        elif self.fact2.index < other.fact2.index:
            return True
        elif self.fact2.index > other.fact2.index:
            return False
    def is_active(self):
        return self.gen_active

    def add_subscriber(self, mapping, mapped_col):
        self.subscribers.add(mapping)

    def get_subscribers(self):
        for mapping in self.subscribers.copy():
            if not mapping.gen_active:
                self.subscribers.remove(mapping)
        return self.subscribers

    def make_inactive(self):
        self.gen_active = False
        if DEBUG_RECORDS or self in debug_set:
            print(f"deactivate Record Tuple: {self.fact1.file}({self.fact1.index},{self.fact2.index})")
        return self.get_subscribers()