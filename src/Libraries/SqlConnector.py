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

    def insert_df(self, table, df):
        df.to_sql(name=table, con=self.engine, if_exists='append',index=False)

    def insert_series(self, table, series):
        df = series.to_frame().T
        self.insert_df(table, df)


    def query_table(self,query,ind_col=None):
        with self.engine.connect() as conn, conn.begin():
            df = pd.read_sql_query(query,con=conn,index_col=ind_col)
        return df

    def get_table(self,table):
        with self.engine.connect() as conn, conn.begin():
            df = pd.read_sql_table(table,con=conn)
        return df
