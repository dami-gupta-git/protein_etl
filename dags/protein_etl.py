import json

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text
from CleaningOperator import CleaningOperator

default_args = {
    'owner': 'dgupta'
}

with DAG(
        dag_id='protein_etl',
        description='DAG for processing protein data',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=None
) as dag:
    @task
    def run_data_checks():
        """
        Check data format - including types, values, foreign keys etc.
        Errors could be recorded in a data_checks table.
        TBD: An error handling workflow
        e.g. The workflow could copy error files to a retry bucket, and schedule a retry run.

        There will be separate methods for each table.
        """
        pass


    @task
    def reshape_data():
        """
        The raw data needs to be reworked to a version appropriate for the end-user
        There will be separate methods for each table
        """
        pass


    @task
    def post_process():
        """
        Post-process data as per business needs
        There will be separate methods for each table
        """
        pass


    @task
    def read_data_protein_binding(engine, latest_run_id):
        """
        Read protein_binding file, and populate protein_binding table
        """
        # TODO fix hardcoding

        chunk_size = 10000
        fname = './data/mock_binding_data.csv'

        # Start with fresh tables to accomodate changes in schema
        sql = text('DROP TABLE IF EXISTS protein_etl.protein_binding;')
        engine.execute(sql)

        # Read in chunks
        # TODO error handling
        for chunk in pd.read_csv(fname, chunksize=chunk_size):
            chunk['run_id'] = latest_run_id
            print(chunk)
            # persist using pandas
            chunk.to_sql(name='protein_binding', con=engine, schema='protein_etl', if_exists='append')


    @task
    def read_data_protein_info(engine, latest_run_id):
        """
        Read protein_info file, and populate protein_info, and protein_dev_metrics tables
        """
        # TODO fix hardcoding

        # Start with fresh tables to accomodate changes in schema
        sql = text('DROP TABLE IF EXISTS protein_etl.protein_info;')
        engine.execute(sql)
        sql2 = text('DROP TABLE IF EXISTS protein_etl.protein_developability_metrics;')
        engine.execute(sql2)

        # TODO Need to do this in chunked fashion
        # TODO error handling
        with open('./data/mock_protein_info.json', 'r') as file:
            protein_info = json.load(file)["proteins"]
            df = pd.DataFrame(protein_info)
            dm = df['developability_metrics']

            df.drop('developability_metrics', axis=1, inplace=True)
            df['run_id'] = latest_run_id
            df.to_sql(name='protein_info', con=engine, schema='protein_etl', if_exists='append')

            # handle nested object
            list_dm = [x[1] for x in dm.items()]
            dm_df = pd.DataFrame(list_dm)
            df_concat = pd.concat([df["protein_id"], dm_df], axis=1)
            df_concat['run_id'] = latest_run_id
            df_concat.to_sql(name='protein_developability_metrics', con=engine, schema='protein_etl', if_exists='append')


    @task
    def read_data_in_vivo_measurements(latest_run_id):
        data = pd.read_parquet('./data/mock_in_vivo_measurements.parquet')
        # TODO process data


    @task
    def update_final_table():
        """
        Invoked when run is complete.
        Will contain information like paths to output files etc
        """


    @task
    def update_start_info(engine):
        """
        Invoked when dag run is started. Updates table start_info with details of the run.
        The run_id that will be used as the unique identifier for this run is generated.
        """

        # TODO these will be fetched from the environment
        de_version = "1.6.1"
        git_hash = "vr3gs4"

        # TODO use airflow operators and jinjified .sql files instead of hardcoded SQL statements
        # TODO batch updates instead of single line operations
        # TODO error handling
        insert_sql = f"""
            INSERT INTO protein_etl.start_info (de_version, git_commit_hash, start_date_time) 
            VALUES ('{de_version}','{git_hash}', NOW())
            RETURNING id;
            """

        with engine.connect() as con:
            latest_run_id = con.execute(insert_sql).fetchone()[0]

        return latest_run_id


    ######################  Main pipeline code ######################
    sql_alchemy_conn_url = "postgresql+psycopg2://postgres:postgres@host.docker.internal:5436/postgres"
    engine = create_engine(sql_alchemy_conn_url)

    # Get the generated run_id for this run
    latest_run_id = update_start_info(engine)

    # These methods will be generated dynamically because files and file types will change.
    # Data in the first set of tables is stored unaltered as text data.
    # Formatting will take place at later stages.
    with TaskGroup(group_id="read_raw_data") as read_raw_data:
        read_data_protein_info_task = read_data_protein_info(engine, latest_run_id)
        read_data_in_vivo_measurements_task = read_data_in_vivo_measurements(latest_run_id)
        read_data_protein_binding_task = read_data_protein_binding(engine, latest_run_id)

    with TaskGroup(group_id="run_checks") as run_data_checks_task:
        run_data_checks_protein_info_task = run_data_checks()
        run_data_checks_in_vivo_measurements_task = run_data_checks()
        run_data_checks_protein_binding_task = run_data_checks()
        run_data_checks_protein_developability_metrics_task = run_data_checks()

    # The data looks normalized to me, any reshaping and postprocessing will
    # depend on business needs

    #run_data_checks_task = run_data_checks()

    clean_protein_binding_data_task = CleaningOperator(
        task_id='clean_protein_binding_data',
        engine=engine,
        schema='protein_etl',
        table='protein_binding',
        col='affinity'
    )

    post_process_task = post_process()
    update_final_table_task = update_final_table()


    latest_run_id  >> read_raw_data>> run_data_checks_task >> clean_protein_binding_data_task >> post_process_task >> update_final_table_task
