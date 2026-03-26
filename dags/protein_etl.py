import json
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from airflow import DAG
from airflow.decorators import task
from sqlalchemy import create_engine, text


# Spark and JDBC configuration
SPARK_APP_NAME = "ProteinETL"
JDBC_URL = "jdbc:postgresql://host.docker.internal:5436/postgres"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
SQL_ALCHEMY_CONN_URL = "postgresql+psycopg2://postgres:postgres@host.docker.internal:5436/postgres"


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
        start_date=datetime(2024, 1, 1),
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
    def reshape_data(latest_run_id):
        """
        Use Spark to perform heavy data transformations across tables.
        Creates aggregated and joined views for downstream analysis.
        """
        engine = create_engine(SQL_ALCHEMY_CONN_URL)
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

            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.pk_summary;'))

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

            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.protein_master;'))

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

            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.tissue_exposure_summary;'))

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
    def post_process(latest_run_id):
        """
        Post-process data:
        - Flag candidate proteins in protein_master
        - Flag outlier binding records in protein_binding
        - Cross-reference protein_ids across tables and write issues to data_quality
        """
        engine = create_engine(SQL_ALCHEMY_CONN_URL)

        try:
            with engine.connect() as con:
                # === 1: Flag candidate proteins ===
                # A candidate must have low aggregation risk, high stability, and good expression
                con.execute(text("""
                    ALTER TABLE protein_etl.protein_master
                    ADD COLUMN IF NOT EXISTS is_candidate BOOLEAN DEFAULT FALSE;
                """))
                con.execute(text("""
                    UPDATE protein_etl.protein_master
                    SET is_candidate = (
                        agg_score < 0.4
                        AND stab_score > 0.7
                        AND expression_level > 200
                    )
                    WHERE run_id = :run_id;
                """), {"run_id": latest_run_id})

                # === 2: Flag outlier binding records ===
                # Outlier = kd_nm more than 2 standard deviations from the mean for this run
                con.execute(text("""
                    ALTER TABLE protein_etl.protein_binding
                    ADD COLUMN IF NOT EXISTS is_outlier BOOLEAN DEFAULT FALSE;
                """))
                con.execute(text("""
                    UPDATE protein_etl.protein_binding
                    SET is_outlier = TRUE
                    WHERE run_id = :run_id
                      AND ABS(affinity - (
                            SELECT AVG(affinity) FROM protein_etl.protein_binding WHERE run_id = :run_id
                          )) > 2 * (
                            SELECT STDDEV(affinity) FROM protein_etl.protein_binding WHERE run_id = :run_id
                          );
                """), {"run_id": latest_run_id})

                # === 3: Cross-reference check ===
                # Find protein_ids in binding or in_vivo tables that don't exist in protein_info
                con.execute(text("""
                    CREATE TABLE IF NOT EXISTS protein_etl.data_quality (
                        ID SERIAL PRIMARY KEY,
                        run_id INTEGER,
                        source_table VARCHAR(255),
                        protein_id VARCHAR(255),
                        issue VARCHAR(255)
                    );
                """))
                con.execute(text("""
                    INSERT INTO protein_etl.data_quality (run_id, source_table, protein_id, issue)
                    SELECT DISTINCT :run_id, 'protein_binding', protein_id, 'protein_id not found in protein_info'
                    FROM protein_etl.protein_binding
                    WHERE run_id = :run_id
                      AND protein_id NOT IN (
                            SELECT protein_id FROM protein_etl.protein_info WHERE run_id = :run_id
                          );
                """), {"run_id": latest_run_id})

                con.execute(text("""
                    INSERT INTO protein_etl.data_quality (run_id, source_table, protein_id, issue)
                    SELECT DISTINCT :run_id, 'in_vivo_measurements', protein_id, 'protein_id not found in protein_info'
                    FROM protein_etl.in_vivo_measurements
                    WHERE run_id = :run_id
                      AND protein_id NOT IN (
                            SELECT protein_id FROM protein_etl.protein_info WHERE run_id = :run_id
                          );
                """), {"run_id": latest_run_id})

        except Exception as e:
            raise RuntimeError(f"Failed to post_process: {e}") from e


    @task
    def read_data_protein_binding(latest_run_id):
        """
        Read protein_binding file, and populate protein_binding table
        """
        # TODO fix hardcoding

        engine = create_engine(SQL_ALCHEMY_CONN_URL)
        chunk_size = 10000
        fname = './data/mock_binding_data.csv'

        try:
            # Start with fresh tables to accomodate changes in schema
            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.protein_binding;'))

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
    def read_data_protein_info(latest_run_id):
        """
        Read protein_info file, and populate protein_info, and protein_dev_metrics tables.
        Processes data in chunks to handle large files efficiently.
        """
        # TODO fix hardcoding
        engine = create_engine(SQL_ALCHEMY_CONN_URL)
        fname = './data/mock_protein_info.json'
        chunk_size = 50

        try:
            # Start with fresh tables to accomodate changes in schema
            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.protein_info;'))
                con.execute(text('DROP TABLE IF EXISTS protein_etl.protein_developability_metrics;'))

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
    def read_data_in_vivo_measurements(latest_run_id):
        """
        Read in_vivo_measurements parquet file using Spark and populate in_vivo_measurements table.
        Uses Spark for efficient processing of large parquet files.
        """
        # TODO fix hardcoding
        engine = create_engine(SQL_ALCHEMY_CONN_URL)
        fname = './data/mock_in_vivo_measurements.parquet'

        spark = None
        try:
            # Start with fresh table to accommodate changes in schema
            with engine.connect() as con:
                con.execute(text('DROP TABLE IF EXISTS protein_etl.in_vivo_measurements;'))

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
    def update_final_table(latest_run_id):
        """
        Invoked when run is complete.
        Writes a summary of the run to run_summary including record counts and status.
        """
        engine = create_engine(SQL_ALCHEMY_CONN_URL)

        try:
            with engine.connect() as con:
                con.execute(text("""
                    CREATE TABLE IF NOT EXISTS protein_etl.run_summary (
                        ID SERIAL PRIMARY KEY,
                        run_id INTEGER,
                        end_date_time TIMESTAMP,
                        n_proteins INTEGER,
                        n_binding_records INTEGER,
                        n_in_vivo_records INTEGER,
                        n_candidate_proteins INTEGER,
                        n_outlier_binding_records INTEGER,
                        n_data_quality_issues INTEGER,
                        status VARCHAR(50)
                    );
                """))

                n_proteins = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.protein_info WHERE run_id = :run_id"
                ), {"run_id": latest_run_id}).scalar()

                n_binding_records = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.protein_binding WHERE run_id = :run_id"
                ), {"run_id": latest_run_id}).scalar()

                n_in_vivo_records = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.in_vivo_measurements WHERE run_id = :run_id"
                ), {"run_id": latest_run_id}).scalar()

                n_candidate_proteins = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.protein_master WHERE run_id = :run_id AND is_candidate = TRUE"
                ), {"run_id": latest_run_id}).scalar()

                n_outlier_binding_records = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.protein_binding WHERE run_id = :run_id AND is_outlier = TRUE"
                ), {"run_id": latest_run_id}).scalar()

                n_data_quality_issues = con.execute(text(
                    "SELECT COUNT(*) FROM protein_etl.data_quality WHERE run_id = :run_id"
                ), {"run_id": latest_run_id}).scalar()

                con.execute(text("""
                    INSERT INTO protein_etl.run_summary (
                        run_id, end_date_time, n_proteins, n_binding_records,
                        n_in_vivo_records, n_candidate_proteins, n_outlier_binding_records,
                        n_data_quality_issues, status
                    ) VALUES (
                        :run_id, NOW(), :n_proteins, :n_binding_records,
                        :n_in_vivo_records, :n_candidate_proteins, :n_outlier_binding_records,
                        :n_data_quality_issues, 'success'
                    );
                """), {
                    "run_id": latest_run_id,
                    "n_proteins": n_proteins,
                    "n_binding_records": n_binding_records,
                    "n_in_vivo_records": n_in_vivo_records,
                    "n_candidate_proteins": n_candidate_proteins,
                    "n_outlier_binding_records": n_outlier_binding_records,
                    "n_data_quality_issues": n_data_quality_issues,
                })

        except Exception as e:
            raise RuntimeError(f"Failed to update_final_table: {e}") from e


    @task
    def update_start_info():
        """
        Invoked when dag run is started. Updates table start_info with details of the run.
        The run_id that will be used as the unique identifier for this run is generated.
        """
        engine = create_engine(SQL_ALCHEMY_CONN_URL)

        # TODO these will be fetched from the environment
        de_version = "1.6.1"
        git_hash = "vr3gs4"

        # TODO use airflow operators and jinjified .sql files instead of hardcoded SQL statements
        # TODO batch updates instead of single line operations
        insert_sql = text("""
            INSERT INTO protein_etl.start_info (de_version, git_commit_hash, start_date_time)
            VALUES (:de_version, :git_hash, NOW())
            RETURNING id;
            """)

        try:
            with engine.connect() as con:
                result = con.execute(insert_sql, {"de_version": de_version, "git_hash": git_hash}).fetchone()
                if result is None:
                    raise RuntimeError("Failed to generate run_id: INSERT did not return a value")
                latest_run_id = result[0]

            return latest_run_id

        except Exception as e:
            raise RuntimeError(f"Failed to update start_info: {e}") from e


    ######################  Main pipeline code ######################

    # Get the generated run_id for this run
    latest_run_id = update_start_info()

    # These methods will be generated dynamically because files and file types will change.
    # Data in the first set of tables is stored unaltered as text data.
    # Formatting will take place at later stages.
    read_data_protein_info_task = read_data_protein_info(latest_run_id)
    read_data_in_vivo_measurements_task = read_data_in_vivo_measurements(latest_run_id)
    read_data_protein_binding_task = read_data_protein_binding(latest_run_id)

    # The data looks normalized to me, any reshaping and postprocessing will
    # depend on business needs

    run_data_checks_task = run_data_checks()
    reshape_data_task = reshape_data(latest_run_id)
    post_process_task = post_process(latest_run_id)
    update_final_table_task = update_final_table(latest_run_id)

    latest_run_id >> [
        read_data_protein_info_task, read_data_in_vivo_measurements_task, read_data_protein_binding_task
    ] >> run_data_checks_task >> reshape_data_task >> post_process_task >> update_final_table_task
