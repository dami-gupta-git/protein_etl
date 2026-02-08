"""
Data transformation utilities for ProteinETL.
Pure functions for Spark DataFrame transformations, extracted for testability.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def create_pk_summary(in_vivo_df: DataFrame, run_id: int) -> DataFrame:
    """
    Create PK (pharmacokinetic) summary from in_vivo measurements.
    Aggregates measurements per protein/tissue/timepoint.

    Args:
        in_vivo_df: Spark DataFrame with in_vivo measurements
        run_id: Run ID to add to output

    Returns:
        Aggregated DataFrame with PK statistics
    """
    return in_vivo_df.groupBy("protein_id", "tissue", "timepoint") \
        .agg(
            F.count("*").alias("n_observations"),
            F.mean("concentration_ug_ml").alias("mean_concentration"),
            F.stddev("concentration_ug_ml").alias("std_concentration"),
            F.min("concentration_ug_ml").alias("min_concentration"),
            F.max("concentration_ug_ml").alias("max_concentration")
        ) \
        .withColumn("run_id", F.lit(run_id).cast(IntegerType()))


def create_protein_master(
    protein_info_df: DataFrame,
    dev_metrics_df: DataFrame,
    in_vivo_df: DataFrame,
    run_id: int
) -> DataFrame:
    """
    Create master protein view by joining protein info with metrics and in_vivo stats.

    Args:
        protein_info_df: Spark DataFrame with protein information
        dev_metrics_df: Spark DataFrame with developability metrics
        in_vivo_df: Spark DataFrame with in_vivo measurements
        run_id: Run ID to add to output

    Returns:
        Joined and enriched DataFrame
    """
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

    return protein_master_df.join(
        protein_invivo_stats,
        on="protein_id",
        how="left"
    ).withColumn("run_id", F.lit(run_id).cast(IntegerType()))


def create_tissue_exposure_summary(in_vivo_df: DataFrame, run_id: int) -> DataFrame:
    """
    Create tissue exposure summary aggregating by tissue and payload.

    Args:
        in_vivo_df: Spark DataFrame with in_vivo measurements
        run_id: Run ID to add to output

    Returns:
        Aggregated DataFrame with tissue exposure statistics
    """
    return in_vivo_df.groupBy("tissue", "payload") \
        .agg(
            F.countDistinct("protein_id").alias("n_proteins"),
            F.countDistinct("mouse_id").alias("n_mice"),
            F.mean("concentration_ug_ml").alias("mean_concentration"),
            F.expr("percentile_approx(concentration_ug_ml, 0.5)").alias("median_concentration")
        ) \
        .withColumn("run_id", F.lit(run_id).cast(IntegerType()))


def add_run_id_column(df: DataFrame, run_id: int) -> DataFrame:
    """
    Add run_id column to a DataFrame.

    Args:
        df: Spark DataFrame
        run_id: Run ID value to add

    Returns:
        DataFrame with run_id column added
    """
    return df.withColumn("run_id", F.lit(run_id).cast(IntegerType()))
