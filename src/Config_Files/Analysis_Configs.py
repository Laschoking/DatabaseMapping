# from src.Classes.Classes import *
from src.Libraries import PathLib


class DbConfig:
    def __init__(self, db_type, dir_name, db1_dir_name, db2_dir_name, db1_file_name=None, db2_file_name=None):
        self.db_type = db_type
        self.dir_name = dir_name
        self.full_name = dir_name + "_" + db1_dir_name + "_" + db2_dir_name
        self.base_output_path = PathLib.base_out_path.joinpath(db_type).joinpath(dir_name).joinpath(
            db1_dir_name + "_" + db2_dir_name)
        self.db1_dir_name = db1_dir_name
        self.db2_dir_name = db2_dir_name
        self.db1_path = self.base_output_path.joinpath(db1_dir_name)
        self.db2_path = self.base_output_path.joinpath(db2_dir_name)
        self.db1_file_name = db1_file_name if db1_file_name else dir_name
        self.db2_file_name = db2_file_name if db2_file_name else dir_name


class DatalogProgram:
    def __init__(self, program_type, class_name, sep_dl, merge_dl, blocked_terms=None):
        self.program_type = program_type
        self.class_name = class_name
        self.execution_path = PathLib.datalog_programs_path.joinpath(program_type).joinpath(class_name)
        self.sep_dl = self.execution_path.joinpath(sep_dl)
        self.merge_dl = self.execution_path.joinpath(merge_dl)
        if not blocked_terms:
            blocked_terms = {}
        self.blocked_terms = blocked_terms


DOOP_Unit_Test_Renamed_Literals = DbConfig("DoopProgramAnalysis", "Unit-Tests", "Basic", "Literal-Renaming",
                                            "BasicMethod", "BasicMethod")
DOOP_Unit_Test_Renamed_Method = DbConfig("DoopProgramAnalysis", "Unit-Tests", "Basic", "Method-Renaming",
                                          "BasicMethod", "Renamed")
Unit_Test_Renamed_Method = DbConfig("Strukturelle_Unit_Tests", "Unit-Tests", "Basic", "Method-Renaming", "BasicMethod",
                                     "Renamed")

Doop_Constants = DbConfig("DoopProgramAnalysis", "Constants", "v1", "v2")
Doop_Simple_Pointer = DbConfig("DoopProgramAnalysis", "Simple_Pointer", "v1", "v2")
Doop_Simple_Java_Calculator = DbConfig("DoopProgramAnalysis", "Simple_Java_Calculator", "v3_0", "v3_1_1")
Doop_Gocd_Websocket_Notifier_v1_v4 = DbConfig("DoopProgramAnalysis", "Gocd_Websocket_Notifier", "v1", "v4")
Doop_Gocd_Websocket_Notifier_v4_v1 = DbConfig("DoopProgramAnalysis", "Gocd_Websocket_Notifier", "v4", "v1")

Doop_Gocd_Websocket_Notifier_v1_v1_copy = DbConfig("DoopProgramAnalysis", "Gocd_Websocket_Notifier", "v1", "v1_copy")

Doop_Gocd_Websocket_Notifier_v3_v4 = DbConfig("DoopProgramAnalysis", "Gocd_Websocket_Notifier", "v3", "v4")

Doop_CFG = DatalogProgram("DoopProgramAnalysis", "CFG", "CFG_separate.rls", "CFG_merge.rls",
                          {'', ' ', "abstract", "<sun.misc.ProxyGenerator: byte[] generateClassFile()>"})
Doop_PointerAnalysis = DatalogProgram("DoopProgramAnalysis", "PointerAnalysis", "PointerAnalyse_separate.rls",
                                      "PointerAnalyse_merge_no_fold.rls",
                                      {'', ' ', "<clinit>", "void()", "public", "static", "main",
                                       "void(java.lang.String[])", "java.io.Serializable", "java.lang.Cloneable",
                                       "java.lang.Object", "abstract"})

Unit_Tests_Doublette = DbConfig("Strukturelle_Unit_Tests", "Doublette_Paradox", "v1", "v2")
Unit_Test_Finish_Records = DbConfig("Strukturelle_Unit_Tests", "Finish_Records", "v1", "v2")
Unit_Test_Del_Rec1 = DbConfig("Strukturelle_Unit_Tests", "Test_Del_Record_Tuple1", "v1", "v2")
Unit_Test_Del_Rec2 = DbConfig("Strukturelle_Unit_Tests", "Test_Del_Record_Tuple2", "v1", "v2")
Unit_Test_Del_Record1 = DbConfig("Strukturelle_Unit_Tests", "Test_Del_Record1", "v1", "v2")
Unit_Test_Del_Record2 = DbConfig("Strukturelle_Unit_Tests", "Test_Del_Record2", "v1", "v2")
Unit_Test_Del_Record3 = DbConfig("Strukturelle_Unit_Tests", "Test_Del_Record3", "v1", "v2")
Unit_Test_Dyn_Max_Cardinality = DbConfig("Strukturelle_Unit_Tests", "Test_Dyn_Max_Cardinality", "v1", "v2")
Unit_Test_Double_Expansion = DbConfig("Strukturelle_Unit_Tests", "Test_Double_Expansion", "v1", "v2")
Unit_Test_Expanding_Bad_Record_Tuples = DbConfig("Strukturelle_Unit_Tests", "Test_Expanding_Bad_Record_Tuples", "v1",
                                                  "v2")
Unit_Test_Expanding_Bad_Record_Tuples = DbConfig("Strukturelle_Unit_Tests", "Test_Expanding_Bad_Record_Tuples", "v1", "v2")

DOOP_GH1_alibaba = DbConfig("DoopProgramAnalysis", "alibaba_arthas", "release_1", "release_2")
DOOP_GH1_halo = DbConfig("DoopProgramAnalysis", "halo-dev_halo", "release_1", "release_2")
DOOP_GH1_proxee = DbConfig("DoopProgramAnalysis", "proxyee-down-org_proxyee-down", "release_1", "release_2")
DOOP_GH1_selenium = DbConfig("DoopProgramAnalysis", "SeleniumHQ_selenium", "release_1", "release_2")
DOOP_GH1_stirling = DbConfig("DoopProgramAnalysis", "Stirling-Tools_Stirling-PDF", "release_1", "release_2")

Syn_Family_Fold_DL = DatalogProgram("SouffleSynthetic", "Family", "Family_separate.rls", "Family_merge_fold.rls", {})

Syn_Family_DL = DatalogProgram("SouffleSynthetic", "Family", "Family_separate.rls", "Family_merge.rls", {})
Syn_Orbits_DL = DatalogProgram("SouffleSynthetic", "Orbits", "Orbits_separate.rls", "Orbits_merge.rls", {})
Syn_Po1_DL = DatalogProgram("SouffleSynthetic", "Po1", "Po1_separate.rls", "Po1_merge.rls", {})

Syn_Family_db = DbConfig("SouffleSynthetic", "Family", "v1", "v2")
Syn_Orbits_db = DbConfig("SouffleSynthetic", "Orbits", "v1", "v2")
Syn_Po1_db = DbConfig("SouffleSynthetic", "Po1", "v1", "v2")

Family_db = DbConfig("SouffleSynthetic", "Family", "Unit", "Example")
Family_Renaming_db = DbConfig("SouffleSynthetic", "Family", "Unit", "Renaming")