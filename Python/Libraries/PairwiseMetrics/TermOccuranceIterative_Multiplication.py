from collections import Counter
from Python.Libraries import Classes
from Python.Libraries.MappingStrategies.Iterative_Anchor_Mapping import *


class TermOccuranceIterative_Multiplication(Iterative_Anchor_Mapping):
    def __init__(self,paths):
        super().__init__(paths,"TermOccuranceIterative_Multiplication")


    def similarity(self,term1,term2,term1_occ,term2_occ):
        counter1 = Counter(term1_occ.keys())
        counter2 = Counter(term2_occ.keys())
        intersection = counter1 & counter2
        sum = 0
        for common in  intersection:
            sum += counter1[common] * counter2[common]

        # eventually integrate lexical similarity
        # TODO: currently priority queue works with minimal value first
        join_atoms = []
        for overlap in intersection:
            join_atoms.append(overlap + (term1_occ[overlap],term2_occ[overlap]))
        return sum,join_atoms