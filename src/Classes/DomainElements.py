import itertools
from src.Config_Files.Debug_Flags import DEBUG_TERMS,DEBUG_RECORDS, debug_set, debug_element_names1,debug_element_names2
from src.Classes.Facts import FactPair


class Element:
    def __init__(self, name, file_name, col_ind, fact):
        self.name = name
        # occurrences is a representation where each element appears
        # occurrences are of type {file_name1,cols1 : [fact1, fact2,...], file_name2,cols2 : [fact3, fact4,...]}
        self.occurrences = dict()
        self.attached_mappings = set()
        self.type = "int" if type(name) is int else "string"
        self.degree = 0
        self.db = fact.db  # its good for debugging, to know to which side the element belongs
        self.gen_active = True

        self.update(file_name, col_ind, fact)

    def __lt__(self, other):
        return self.degree < other.degree

    def deactivate_element_and_all_tt(self):
        self.gen_active = False
        if DEBUG_TERMS or self in debug_set:
            print(f"deactivate Element of {self.db}: ({self.name})")

        for mapping in self.attached_mappings.copy():
            # mapping_func was deactivated before i.e. bc it has a similarity of 0
            if not mapping.is_active():
                self.attached_mappings.remove(mapping)
            else:  # otherwise deactivate now. we cannot destroy it yet bc. it needs to be removed from prio-dict first
                mapping.gen_active = False
                if DEBUG_TERMS or mapping in debug_set:
                    print(f"deactivate Element Tuple: ({mapping.element1.name},{mapping.element2.name})")

        return self.attached_mappings

    def is_active(self):
        return self.gen_active

    def update(self, file_name, cols, fact):
        # cols needs to be of type tuple bc. list is mutable object
        if type(cols) is list:
            cols = tuple(cols)
        key = (file_name, cols)
        self.occurrences.setdefault(key, set()).add(fact)
        self.degree += 1

    def remove_occurrence(self, file_name, cols, fact_pair):
        self.occurrences[file_name, cols].remove(fact_pair)
        if not self.occurrences[file_name, cols]:
            del self.occurrences[file_name, cols]
        self.degree -= 1


# this will be 1 potential mapping_func consisting of two element-objects


class Mapping:
    def __init__(self, element1, element2, expanded_fact_pairs, similarity_metric):
        self.element1 = element1
        self.element2 = element2
        self.element1.attached_mappings.add(self)
        self.element2.attached_mappings.add(self)
        self.gen_active = True
        self.sub_fact_pairs = dict()  # {fact : set(fact_pair1,fact_pair2,...) }
        self.similarity_metric = similarity_metric
        self.sim = 0

        #
        if element1.name in debug_element_names1:
            debug_set.add(element1)
            debug_set.add(self)
        if element2.name in debug_element_names2:
            debug_set.add(self)
            debug_set.add(element2)

        self.calc_initial_fact_pairs(expanded_fact_pairs)


    def __lt__(self, other):
        if self.sim is None or other.sim is None:
            raise ValueError(f"sim does not exist {self.element1.name,self.element2.name,self.sim}, "
                             f"{other.element1.name, other.element2.name,other.sim}")
        if self.sim < other.sim:
            return True
        elif self.sim > other.sim:
            return False
        return id(self) < id(other)

    def eq_values(self,other):
        return self.sim == other.sim #and min(self.element1.degree,self.element2.degree) == min(other.element1.degree,other.element2.degree)


    def is_active(self):
        return self.gen_active

    def calc_initial_fact_pairs(self, expanded_fact_pairs):
        """ Calculates all fact pairs where element1 -> element2  helps the matching of those facts
        :param expanded_fact_pairs: fact tuples that have been unveiled already (they can need to be active or inactive)"""

        # intersection saves the key (file,cols):  which is the minimum of occurrences for this key
        intersection = set(self.element1.occurrences.keys()) & set(self.element2.occurrences.keys())

        for file_name, mapped_cols in intersection:

            facts_db1 = self.element1.occurrences[(file_name, mapped_cols)]
            facts_db2 = self.element2.occurrences[(file_name, mapped_cols)]

            for fact1, fact2 in itertools.product(facts_db1, facts_db2):
                # in this case we would expand a side, where one fact is inactive already
                if not fact1.is_active() or not fact2.is_active():
                    if DEBUG_RECORDS:
                        print(f"discarded fact obj bc. it is inactive {fact1.file}{fact1.index, fact2.index}")
                    continue

                if fact1.is_in_process() and fact2.is_in_process():
                    # if both facts are individually in_process, but no active fact tuple exist, skip
                    if (fact1, fact2) not in expanded_fact_pairs.keys():
                        continue
                    else:
                        fact_pair = expanded_fact_pairs[(fact1, fact2)]
                        # check if the existing fact_pair is still active
                        if not fact_pair.is_active():
                            continue

                elif not fact1.is_in_process() and not fact2.is_in_process():
                    if (fact1, fact2) not in expanded_fact_pairs.keys():
                        fact_pair = FactPair(fact1, fact2)
                        expanded_fact_pairs[fact1,fact2] = fact_pair
                    else:
                        fact_pair = expanded_fact_pairs[(fact1, fact2)]

                # Avoid fact-tuples, where one side is in_process, when the other side is not
                else:
                    continue

                fact1.fact_pairs.add(fact_pair)
                fact2.fact_pairs.add(fact_pair)
                fact_pair.add_subscriber(self, mapped_cols)
                if DEBUG_RECORDS or self.element1 in debug_set or self.element2 in debug_set:
                    print(
                        f"{self.element1.name},{self.element2.name}  subscribes to {fact1.file}({fact_pair.fact1.index},{fact_pair.fact2.index}),(in_process1={fact_pair.fact1.is_in_process()}),(in_process2={fact_pair.fact2.is_in_process()})")
                    debug_set.add(fact_pair)

                if fact1.is_active() == fact2.is_active():
                    fact1.active_fact_pairs.add(fact_pair)
                    fact2.active_fact_pairs.add(fact_pair)
                self.sub_fact_pairs.setdefault(fact1, set()).add(fact_pair)
                self.sub_fact_pairs.setdefault(fact2, set()).add(fact_pair)

    def recompute_similarity(self):
        self.get_clean_fact_pairs()  # make sure, that all facts & fact_objects are still valid
        sim = self.similarity_metric.recompute_similarity(self.sim,self.element1, self.element2, self.sub_fact_pairs)
        self.sim = sim
        return self.sim

    def compute_similarity(self):
        sim = self.similarity_metric.compute_similarity(self.element1, self.element2, self.sub_fact_pairs)
        self.sim = sim
        return self.sim

    def get_similarity(self):
        return self.sim

    def get_clean_fact_pairs(self):
        for fact, fact_pairs in self.sub_fact_pairs.copy().items():
            # if fact is deactivated, delete it
            if not fact.is_active():
                del self.sub_fact_pairs[fact]
                continue

            # if fact-tuple is deactivated
            for fact_pair in fact_pairs.copy():
                if not fact_pair.is_active():
                    self.sub_fact_pairs[fact].remove(fact_pair)
                    if not self.sub_fact_pairs[fact]:
                        del self.sub_fact_pairs[fact]

        return self.sub_fact_pairs


    def accept_this_mapping(self,DYNAMIC):

        # Update all fact-tuples, that one or more cells are filled now by the current mapping_func
        remaining_facts = set()
        finished_fact_pairs = dict()
        for mapped_fact in self.sub_fact_pairs.keys():
            is_finished = mapped_fact.mark_filled_cols(self)
            remaining_facts.add(mapped_fact)
            if is_finished:
                ind1 = list()
                ind2 = list()
                fact_pairs = self.sub_fact_pairs[mapped_fact]

                for fact_pair in fact_pairs:
                    l1 = fact_pair.fact1.index
                    l2 = fact_pair.fact2.index
                    if l1 in ind1 or l2 in ind2:
                        raise ValueError(f"this fact is marked already for fulfillment {mapped_fact.file,l1,l2}")
                    else:
                        ind1.append(l1)
                        ind2.append(l2)
                    if DEBUG_RECORDS or fact_pair in debug_set:
                        print(f"marked fact for fill: {mapped_fact.file}{l1,l2}")
                    finished_fact_pairs.setdefault(mapped_fact.file, set()).add((fact_pair.fact1.index, fact_pair.fact2.index))



        # Find all mappings that are invalid now, because one part consisted of mapping_func.element1 or mapping_func.element2
        related_mappings = set()
        related_mappings |= self.element1.deactivate_element_and_all_tt()
        related_mappings |= self.element2.deactivate_element_and_all_tt()
        related_mappings.remove(self)  # We don't want to delete the current mapping_func from the prio-dict

        altered_mappings = set()
        if DYNAMIC:
            # Gather all facts where the two elements are involved individually
            all_facts = set()
            for facts in itertools.chain(self.element1.occurrences.values(), self.element2.occurrences.values()):
                all_facts |= facts

            # Find facts that can never be matched after the current mapping_func
            # For example, consider DB1: foo(a1,b1). DB2: foo(c2,d2). foo(e2,c2).
            # When accepting  "a1 -> c2", the fact "foo(e2,c2)" can never be matched throughout the whole mapping_func process
            destroy_facts = all_facts - remaining_facts

            # Deactivate  facts and return subscribed element-tuples that need to be updated (since they lost a fact-tuple)
            for fact in destroy_facts:

                if fact.is_active():
                    if DEBUG_RECORDS or self in debug_set:
                        print(f"deactivate  fact: {fact.db}{fact.file, fact.index}")
                    altered_mappings |= fact.deactivate_self_and_all_rt()
        return related_mappings, altered_mappings,finished_fact_pairs

    def get_facts(self):
        return self.sub_fact_pairs

