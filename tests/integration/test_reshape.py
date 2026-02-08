"""
Integration tests for reshape_data transformations.
Tests the full Spark pipeline with mock data.
"""
import pytest
from pyspark.sql import functions as F

from utils.transformations import (
    create_pk_summary,
    create_protein_master,
    create_tissue_exposure_summary
)


@pytest.mark.integration
@pytest.mark.spark
class TestReshapeDataIntegration:
    """Integration tests for the reshape_data transformation pipeline."""

    def test_full_reshape_pipeline(
        self,
        spark_session,
        spark_protein_info_df,
        spark_dev_metrics_df,
        spark_in_vivo_df
    ):
        """
        Test that all reshape transformations work together.
        Simulates what reshape_data task does.
        """
        run_id = 1

        # Filter by run_id (simulating production behavior)
        # Add run_id to source data first
        protein_info = spark_protein_info_df.withColumn("run_id", F.lit(run_id))
        dev_metrics = spark_dev_metrics_df.withColumn("run_id", F.lit(run_id))
        in_vivo = spark_in_vivo_df.withColumn("run_id", F.lit(run_id))

        # Run all transformations
        pk_summary = create_pk_summary(in_vivo, run_id)
        protein_master = create_protein_master(protein_info, dev_metrics, in_vivo, run_id)
        tissue_summary = create_tissue_exposure_summary(in_vivo, run_id)

        # Verify outputs
        assert pk_summary.count() > 0
        assert protein_master.count() > 0
        assert tissue_summary.count() > 0

        # Verify all have run_id
        assert pk_summary.filter(F.col("run_id") == run_id).count() == pk_summary.count()
        assert protein_master.filter(F.col("run_id") == run_id).count() == protein_master.count()
        assert tissue_summary.filter(F.col("run_id") == run_id).count() == tissue_summary.count()

    def test_pk_summary_statistics_accuracy(self, spark_session):
        """Test that PK summary calculates correct statistics."""
        # Create controlled test data
        test_data = [
            ("PRT-001", "liver", 24, 10.0),
            ("PRT-001", "liver", 24, 20.0),
            ("PRT-001", "liver", 24, 30.0),
        ]
        df = spark_session.createDataFrame(
            test_data,
            ["protein_id", "tissue", "timepoint", "concentration_ug_ml"]
        )

        result = create_pk_summary(df, run_id=1)
        row = result.collect()[0]

        # Check calculations
        assert row["n_observations"] == 3
        assert abs(row["mean_concentration"] - 20.0) < 0.001  # Mean of 10, 20, 30
        assert abs(row["min_concentration"] - 10.0) < 0.001
        assert abs(row["max_concentration"] - 30.0) < 0.001

    def test_protein_master_left_join_behavior(self, spark_session):
        """Test that protein_master correctly handles proteins without in_vivo data."""
        # Protein info with 2 proteins
        protein_info = spark_session.createDataFrame([
            ("PRT-001", "ACDEF", "Fab"),
            ("PRT-002", "FGHIK", "scFv"),
        ], ["protein_id", "sequence", "structure_type"])

        # Metrics for both proteins
        dev_metrics = spark_session.createDataFrame([
            ("PRT-001", 0.3, 0.8, 250.0),
            ("PRT-002", 0.5, 0.6, 180.0),
        ], ["protein_id", "aggregation_score", "stability_score", "expression_level_mg_L"])

        # In vivo data only for PRT-001
        in_vivo = spark_session.createDataFrame([
            ("PRT-001", "M001", "liver", 10.0),
        ], ["protein_id", "mouse_id", "tissue", "concentration_ug_ml"])

        result = create_protein_master(protein_info, dev_metrics, in_vivo, run_id=1)
        result_pd = result.toPandas()

        # Both proteins should be in result
        assert len(result_pd) == 2

        # PRT-001 should have in_vivo stats
        prt001 = result_pd[result_pd["protein_id"] == "PRT-001"].iloc[0]
        assert prt001["n_mice_tested"] == 1

        # PRT-002 should have null in_vivo stats
        prt002 = result_pd[result_pd["protein_id"] == "PRT-002"].iloc[0]
        assert prt002["n_mice_tested"] is None

    def test_tissue_summary_handles_multiple_payloads(self, spark_session):
        """Test tissue summary correctly groups by payload."""
        test_data = [
            ("liver", "DM1", "PRT-001", "M001", 10.0),
            ("liver", "DM1", "PRT-001", "M002", 15.0),
            ("liver", "MMAE", "PRT-002", "M001", 20.0),
            ("tumor", "DM1", "PRT-001", "M001", 5.0),
        ]
        df = spark_session.createDataFrame(
            test_data,
            ["tissue", "payload", "protein_id", "mouse_id", "concentration_ug_ml"]
        )

        result = create_tissue_exposure_summary(df, run_id=1)
        result_pd = result.toPandas()

        # Should have 3 groups: liver/DM1, liver/MMAE, tumor/DM1
        assert len(result_pd) == 3

        # Check liver/DM1 group
        liver_dm1 = result_pd[(result_pd["tissue"] == "liver") & (result_pd["payload"] == "DM1")].iloc[0]
        assert liver_dm1["n_proteins"] == 1
        assert liver_dm1["n_mice"] == 2


@pytest.mark.integration
@pytest.mark.spark
class TestSparkParquetReading:
    """Integration tests for Spark parquet reading."""

    def test_read_parquet_file(self, spark_session, sample_parquet_file):
        """Test that Spark can read the parquet file correctly."""
        df = spark_session.read.parquet(sample_parquet_file)

        assert df.count() == 4
        assert "protein_id" in df.columns
        assert "concentration_ug_ml" in df.columns

    def test_parquet_schema_matches_expected(self, spark_session, sample_parquet_file):
        """Test that parquet schema matches expected columns."""
        df = spark_session.read.parquet(sample_parquet_file)

        expected_columns = {
            "molecule", "mouse_id", "date", "tissue",
            "timepoint", "protein_id", "payload", "concentration_ug_ml"
        }
        assert set(df.columns) == expected_columns

    def test_parquet_data_types(self, spark_session, sample_parquet_file):
        """Test that parquet data types are correct."""
        df = spark_session.read.parquet(sample_parquet_file)
        schema = {field.name: field.dataType.simpleString() for field in df.schema}

        assert schema["timepoint"] == "bigint"
        assert schema["concentration_ug_ml"] == "double"
