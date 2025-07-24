import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class CleaningOperator(BaseOperator):
    def __init__(self, engine, schema,table, col,**kwargs) -> None:
        super().__init__(**kwargs)
        self.engine=engine
        self.schema=schema
        self.table=table
        self.col=col


    def execute(self, context):
        print("in cleaning operator")
        sql_alchemy_conn_url = "postgresql+psycopg2://postgres:postgres@host.docker.internal:5436/postgres"

        df = pd.read_sql_table(table_name=self.table, schema=self.schema,con=sql_alchemy_conn_url)
        df_2 = df[df[self.col] > 5.0]
        df_2.to_sql(name=f'cleaned_{self.table}',schema=self.schema,con=sql_alchemy_conn_url,if_exists='replace', index=False)

        print(df_2)