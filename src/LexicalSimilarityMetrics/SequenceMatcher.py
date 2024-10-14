import difflib


def sequence_matcher(element_name1, element_name2, element1, element2, occ_overlap):
    # similarity was already calculated (just increase by one then)
    # based on the path to the first relation, deelementine path to second relation
    if element1.type == "int" and element2.type == "int":
        max_int = max(int(element_name1), int(element_name2))
        if max_int > 0:
            return 1 - abs(int(element_name1) - int(element_name2)) / max_int
        else:
            return 1
    else:
        return difflib.SequenceMatcher(None, element_name1, element_name2).ratio()