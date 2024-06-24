from Python.Libraries.MappingStrategies.Crossproduct_Mapping_Queue import *

'''
Nutzung der Built-in Funktion aus Python für Score [0,1]
Ausserdem wird gezählt, wie oft die Terme in der gleichen Spalte einer Relation vorkommen
score = sim_score(0-1) + countPair(1-...)
'''

class ISUBStringMetric(Crossproduct_Mapping_Queue):
    def __init__(self,paths):
        super().__init__(paths,"ISUBStringMetric")

    def similarity(self,term1,term2,occ1,occ2):
        # based on the path to the first relation, determine path to second relation
        if term1.lstrip("-").isdigit() and term2.lstrip("-").isdigit():
            max_int = max(int(term1), int(term2))
            if max_int > 0:
                return 1 - abs(int(term1) - int(term2)) / max_int
            else:
                return 1
        else:
            return isub(term1,term2)

def normalize_string(s, chars_to_remove):
    for char in chars_to_remove:
        s = s.replace(char, '')
    return s

def winkler_improvement(s1, s2, commonality):
    common_prefix_length = min(len(s1), len(s2), 4)
    for i in range(common_prefix_length):
        if s1[i] != s2[i]:
            common_prefix_length = i
            break
    return common_prefix_length * 0.1 * (1 - commonality)

def isub(st1, st2):
    if st1 is None or st2 is None:
        return -1

    chars_to_remove = '._ '
    s1 = normalize_string(st1.lower(), chars_to_remove)
    s2 = normalize_string(st2.lower(), chars_to_remove)

    L1, L2 = len(s1), len(s2)

    if L1 == 0 and L2 == 0:
        return 1
    if L1 == 0 or L2 == 0:
        return -1

    common = 0
    best = 2

    while s1 and s2 and best != 0:
        best = 0
        start_s1, end_s1, start_s2, end_s2 = 0, 0, 0, 0

        for i in range(len(s1)):
            for j in range(len(s2)):
                k = 0
                while i + k < len(s1) and j + k < len(s2) and s1[i + k] == s2[j + k]:
                    k += 1
                if k > best:
                    best = k
                    start_s1, end_s1 = i, i + k
                    start_s2, end_s2 = j, j + k

        if best > 0:
            s1 = s1[:start_s1] + s1[end_s1:]
            s2 = s2[:start_s2] + s2[end_s2:]

        if best > 2:
            common += best
        else:
            best = 0

    scaled_common = 2 * common / (L1 + L2)
    commonality = scaled_common

    winkler_improv = winkler_improvement(st1, st2, commonality)
    rest1, rest2 = L1 - common, L2 - common
    unmatched_s1, unmatched_s2 = rest1 / L1, rest2 / L2

    suma = unmatched_s1 + unmatched_s2
    product = unmatched_s1 * unmatched_s2
    p = 0.6

    if suma - product == 0:
        dissimilarity = 0
    else:
        dissimilarity = product / (p + (1 - p) * (suma - product))

    return commonality - dissimilarity + winkler_improv