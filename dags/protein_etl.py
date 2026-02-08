import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, text
import pyspark


# Spark and JDBC configuration
SPARK_APP_NAME = "ProteinETL"
JDBC_URL = "jdbc:postgresql://host.docker.internal:5436/postgres"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def get_spark_session():
    """Create or get existing Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

default_args = {
    'owner': 'dgupta'
}

with DAG(
        dag_id='protein_etl',
        description='DAG for processing data',
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
    def reshape_data(engine, latest_run_id):
        """
        Use Spark to perform heavy data transformations across tables.
        Creates aggregated and joined views for downstream analysis.
        """
        spark = None
        try:
            spark = get_spark_session()

            # Read raw tables from PostgreSQL
            protein_info_df = spark.read.jdbc(
                url=JDBC_URL,
                table="protein_etl.protein_info",
                properties=JDBC_PROPERTIES
            ).filter(F.col("run_id") == latest_run_id)

            in_vivo_df = spark.read.jdbc(
                url=JDBC_URL,
                table="protein_etl.in_vivo_measurements",
                properties=JDBC_PROPERTIES
            ).filter(F.col("run_id") == latest_run_id)

            dev_metrics_df = spark.read.jdbc(
                url=JDBC_URL,
                table="protein_etl.protein_developability_metrics",
                properties=JDBC_PROPERTIES
            ).filter(F.col("run_id") == latest_run_id)

            # === Transformation 1: Protein PK Summary ===
            # Aggregate in_vivo measurements per protein/tissue/timepoint
            pk_summary_df = in_vivo_df.groupBy("protein_id", "tissue", "timepoint") \
                .agg(
                    F.count("*").alias("n_observations"),
                    F.mean("concentration_ug_ml").alias("mean_concentration"),
                    F.stddev("concentration_ug_ml").alias("std_concentration"),
                    F.min("concentration_ug_ml").alias("min_concentration"),
                    F.max("concentration_ug_ml").alias("max_concentration")
                ) \
                .withColumn("run_id", F.lit(latest_run_id).cast(IntegerType()))

            # Drop and recreate table
            sql = text('DROP TABLE IF EXISTS protein_etl.pk_summary;')
            engine.execute(sql)

            pk_summary_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table="protein_etl.pk_summary", properties=JDBC_PROPERTIES)

            print(f"Created pk_summary with {pk_summary_df.count()} records")

            # === Transformation 2: Protein Master View ===
            # Join protein_info with developability metrics
            protein_master_df = protein_info_df.join(
                dev_metrics_df.select(
                    "protein_id",
                    F.col("aggregation_score").alias("agg_score"),
                    F.col("stability_score").alias("stab_score"),
                    F.col("expression_level_mg_L").alias("expression_level")
                ),
                on="protein_id",
                how="left"
            )

            # Add in_vivo summary stats per protein
            protein_invivo_stats = in_vivo_df.groupBy("protein_id") \
                .agg(
                    F.countDistinct("mouse_id").alias("n_mice_tested"),
                    F.countDistinct("tissue").alias("n_tissues_tested"),
                    F.mean("concentration_ug_ml").alias("overall_mean_conc")
                )

            protein_master_df = protein_master_df.join(
                protein_invivo_stats,
                on="protein_id",
                how="left"
            ).withColumn("run_id", F.lit(latest_run_id).cast(IntegerType()))

            # Drop and recreate table
            sql = text('DROP TABLE IF EXISTS protein_etl.protein_master;')
            engine.execute(sql)

            protein_master_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table="protein_etl.protein_master", properties=JDBC_PROPERTIES)

            print(f"Created protein_master with {protein_master_df.count()} records")

            # === Transformation 3: Tissue Exposure Summary ===
            # Summary of drug exposure by tissue across all proteins
            tissue_summary_df = in_vivo_df.groupBy("tissue", "payload") \
                .agg(
                    F.countDistinct("protein_id").alias("n_proteins"),
                    F.countDistinct("mouse_id").alias("n_mice"),
                    F.mean("concentration_ug_ml").alias("mean_concentration"),
                    F.expr("percentile_approx(concentration_ug_ml, 0.5)").alias("median_concentration")
                ) \
                .withColumn("run_id", F.lit(latest_run_id).cast(IntegerType()))

            sql = text('DROP TABLE IF EXISTS protein_etl.tissue_exposure_summary;')
            engine.execute(sql)

            tissue_summary_df.write \
                .mode("append") \
                .jdbc(url=JDBC_URL, table="protein_etl.tissue_exposure_summary", properties=JDBC_PROPERTIES)

            print(f"Created tissue_exposure_summary with {tissue_summary_df.count()} records")

        except Exception as e:
            raise RuntimeError(f"Failed to reshape data: {e}") from e
        finally:
            if spark:
                spark.stop()


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

        try:
            # Start with fresh tables to accomodate changes in schema
            sql = text('DROP TABLE IF EXISTS protein_etl.protein_binding;')
            engine.execute(sql)

            # Read in chunks
            chunks_processed = 0
            for chunk in pd.read_csv(fname, chunksize=chunk_size):
                chunk['run_id'] = latest_run_id
                print(chunk)
                # persist using pandas
                chunk.to_sql(name='protein_binding', con=engine, schema='protein_etl', if_exists='append')
                chunks_processed += 1

            if chunks_processed == 0:
                raise ValueError(f"CSV file {fname} is empty")

        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {fname}")
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file {fname} contains no data")
        except Exception as e:
            raise RuntimeError(f"Failed to process protein_binding: {e}") from e


    @task
    def read_data_protein_info(engine, latest_run_id):
        """
        Read protein_info file, and populate protein_info, and protein_dev_metrics tables.
        Processes data in chunks to handle large files efficiently.
        """
        # TODO fix hardcoding
        fname = './data/mock_protein_info.json'
        chunk_size = 50

        try:
            # Start with fresh tables to accomodate changes in schema
            sql = text('DROP TABLE IF EXISTS protein_etl.protein_info;')
            engine.execute(sql)
            sql2 = text('DROP TABLE IF EXISTS protein_etl.protein_developability_metrics;')
            engine.execute(sql2)

            with open(fname, 'r') as file:
                data = json.load(file)

                if "proteins" not in data:
                    raise KeyError(f"JSON file {fname} missing required 'proteins' key")

                protein_list = data["proteins"]
                if not protein_list:
                    raise ValueError(f"JSON file {fname} contains empty proteins array")

                # Process in chunks
                total_proteins = len(protein_list)
                chunks_processed = 0

                for i in range(0, total_proteins, chunk_size):
                    chunk = protein_list[i:i + chunk_size]
                    df = pd.DataFrame(chunk)

                    if 'developability_metrics' not in df.columns:
                        raise KeyError(f"JSON data missing required 'developability_metrics' field")

                    # Extract developability_metrics before dropping
                    dm = df['developability_metrics']

                    # Process protein_info
                    df.drop('developability_metrics', axis=1, inplace=True)
                    df['run_id'] = latest_run_id
                    df.to_sql(name='protein_info', con=engine, schema='protein_etl', if_exists='append', index=False)

                    # Process developability_metrics with protein_id linkage
                    dm_records = []
                    for idx, metrics in dm.items():
                        record = metrics.copy()
                        record['protein_id'] = df.iloc[idx - df.index[0]]['protein_id']
                        dm_records.append(record)

                    dm_df = pd.DataFrame(dm_records)
                    dm_df['run_id'] = latest_run_id
                    dm_df.to_sql(name='protein_developability_metrics', con=engine, schema='protein_etl', if_exists='append', index=False)

                    chunks_processed += 1
                    print(f"Processed chunk {chunks_processed}: proteins {i+1} to {min(i+chunk_size, total_proteins)}")

                print(f"Completed processing {total_proteins} proteins in {chunks_processed} chunks")

        except FileNotFoundError:
            raise FileNotFoundError(f"JSON file not found: {fname}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {fname}: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Failed to process protein_info: {e}") from e


    @task
    def read_data_in_vivo_measurements(engine, latest_run_id):
        """
        Read in_vivo_measurements parquet file using Spark and populate in_vivo_measurements table.
        Uses Spark for efficient processing of large parquet files.
        """
        # TODO fix hardcoding
        fname = './data/mock_in_vivo_measurements.parquet'

        spark = None
        try:
            # Start with fresh table to accommodate changes in schema
            sql = text('DROP TABLE IF EXISTS protein_etl.in_vivo_measurements;')
            engine.execute(sql)

            # Initialize Spark session
            spark = get_spark_session()

            # Read parquet with Spark (native format, very efficient)
            df = spark.read.parquet(fname)

            if df.count() == 0:
                raise ValueError(f"Parquet file {fname} is empty")

            # Add run_id column
            df = df.withColumn("run_id", F.lit(latest_run_id).cast(IntegerType()))

            # Write to PostgreSQL using JDBC
            df.write \
                .mode("append") \
                .jdbc(
                    url=JDBC_URL,
                    table="protein_etl.in_vivo_measurements",
                    properties=JDBC_PROPERTIES
                )

            record_count = df.count()
            print(f"Successfully loaded {record_count} in_vivo_measurements records using Spark")

        except Exception as e:
            raise RuntimeError(f"Failed to process in_vivo_measurements: {e}") from e
        finally:
            if spark:
                spark.stop()


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
        insert_sql = f"""
            INSERT INTO protein_etl.start_info (de_version, git_commit_hash, start_date_time)
            VALUES ('{de_version}','{git_hash}', NOW())
            RETURNING id;
            """

        try:
            with engine.connect() as con:
                result = con.execute(insert_sql).fetchone()
                if result is None:
                    raise RuntimeError("Failed to generate run_id: INSERT did not return a value")
                latest_run_id = result[0]

            return latest_run_id

        except Exception as e:
            raise RuntimeError(f"Failed to update start_info: {e}") from e


    ######################  Main pipeline code ######################
    sql_alchemy_conn_url = "postgresql+psycopg2://postgres:postgres@host.docker.internal:5436/postgres"
    engine = create_engine(sql_alchemy_conn_url)

    # Get the generated run_id for this run
    latest_run_id = update_start_info(engine)

    # These methods will be generated dynamically because files and file types will change.
    # Data in the first set of tables is stored unaltered as text data.
    # Formatting will take place at later stages.
    read_data_protein_info_task = read_data_protein_info(engine, latest_run_id)
    read_data_in_vivo_measurements_task = read_data_in_vivo_measurements(engine, latest_run_id)
    read_data_protein_binding_task = read_data_protein_binding(engine, latest_run_id)

    # The data looks normalized to me, any reshaping and postprocessing will
    # depend on business needs

    run_data_checks_task = run_data_checks()
    reshape_data_task = reshape_data(engine, latest_run_id)
    post_process_task = post_process()
    update_final_table_task = update_final_table()

    latest_run_id >> [
        read_data_protein_info_task, read_data_in_vivo_measurements_task, read_data_protein_binding_task
    ] >> run_data_checks_task >> reshape_data_task >> post_process_task >> update_final_table_task
