DYNAMIC_EXPANSION = True
HUB_RECOMPUTE = False
DEBUG_TERMS = False
DEBUG_RECORDS = False
debug_term_names1 = set()  # set(["gocd"])
debug_term_names2 = set()  # set(["gocd"])
debug_set = set()  # set(["A","B","C","D","E","F","G","H"])
PLOT_STATISTICS = False

# Dynamic Expansion: considers Record Tuples and Term Tuples (mapping based on optimal record matching)
# Static Expansion: considers only Term Tuples (mapping based on similarity) -> only delete Term Tuples


# static Lexical Approaches only need String Name
# static Jaccard Index needs Occurrences, but no Records
# for expansion Occurrences are anyway needed !

# Dynamic Metrics needs Occurrences and Record Tuples