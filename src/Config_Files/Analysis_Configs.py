# from src.Classes.Classes import *
from src.Libraries import PathLib


class DbConfig:
    def __init__(self, use, type, file_name, db1_name, db2_name):
        self.use = use
        self.db_type = type
        self.file_name = file_name
        self.full_name = file_name + "_" + db1_name + "_" + db2_name
        self.base_output_path = PathLib.base_out_path.joinpath(type).joinpath(file_name).joinpath(
            db1_name + "_" + db2_name)
        self.db1_name = db1_name
        self.db2_name = db2_name
        self.db1_path = self.base_output_path.joinpath(db1_name)
        self.db2_path = self.base_output_path.joinpath(db2_name)

    def get_finger_print(self):
        return {'db_config_id': self.full_name, 'use': self.use, 'type': self.db_type,
                'file': self.file_name, 'db1': self.db1_name, 'db2': self.db2_name}

class DatalogProgram:
    def __init__(self, program_type, name, sep_dl, merge_dl, blocked_elements=None):
        self.program_type = program_type
        self.name = name
        self.execution_path = PathLib.datalog_programs_path.joinpath(program_type).joinpath(name)
        self.sep_dl = self.execution_path.joinpath(sep_dl)
        self.merge_dl = self.execution_path.joinpath(merge_dl)
        if not blocked_elements:
            blocked_elements = {}
        self.blocked_elements = blocked_elements



Syn_Family_Fold_DL = DatalogProgram("SouffleSynthetic", "Family", "Family_separate.rls", "Family_merge_fold.rls", {})
Syn_Family_DL = DatalogProgram("SouffleSynthetic", "Family", "Family_separate.rls", "Family_merge.rls", {})
Syn_Orbits_DL = DatalogProgram("SouffleSynthetic", "Orbits", "Orbits_separate.rls", "Orbits_merge.rls", {})
Syn_Po1_DL = DatalogProgram("SouffleSynthetic", "Po1", "Po1_separate.rls", "Po1_merge.rls", {})


Doop_CFG = DatalogProgram("DoopProgramAnalysis", "CFG", "CFG_separate.rls", "CFG_merge.rls",
                          {'', ' ', "abstract", "<sun.misc.ProxyGenerator: byte[] generateClassFile()>"})
Doop_PointerAnalysis = DatalogProgram("DoopProgramAnalysis", "PointerAnalysis", "PointerAnalyse_separate.rls",
                                      "PointerAnalyse_merge_no_fold.rls",
                                      {'', ' ', "<clinit>", "void()", "public", "static", "main",
                                       "void(java.lang.String[])", "java.io.Serializable", "java.lang.Cloneable",
                                       "java.lang.Object", "abstract"})
