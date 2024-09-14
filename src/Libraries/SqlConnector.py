import re
import os
from pathlib import Path
from src.Libraries import PathLib
import shutil
import subprocess
from prettytable import PrettyTable
import pandas as pd
from sqlalchemy import create_engine,text

class SqlConnector:
    def __init__(self, db_path):
        self.engine = create_engine(f"sqlite:///{db_path}")

    def insert_records(self, df_name, df,write_index):
        df.to_sql(name=df_name,con=self.engine,if_exists='append',index=write_index)


    def query_table(self,query,ind_col=None):
        #with self.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(query,con=self.engine,index_col=ind_col)
        return df

    def get_table(self,table):
        df = pd.read_sql_table(table,self.engine)
        return df
