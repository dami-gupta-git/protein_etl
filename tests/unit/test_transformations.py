"""
Unit tests for Spark transformation utilities.
These tests use a local Spark session.
"""
import pytest

from utils.transformations import (
    create_pk_summary,
    create_protein_master,
    create_tissue_exposure_summary,
    add_run_id_column
)


@pytest.mark.spark
class TestCreatePkSummary:
    """Tests for create_pk_summary transformation."""

    def test_pk_summary_aggregates_correctly(self, spark_in_vivo_df):
        """Test that PK summary aggregates by protein/tissue/timepoint."""
        result = create_pk_summary(spark_in_vivo_df, run_id=1)
        result_pd = result.toPandas()

        # Should have 2 groups: PRT-001/liver/24 and PRT-002/tumor/48
        assert len(result_pd) == 2

        # Check aggregation for PRT-001
        prt001_row = result_pd[result_pd["protein_id"] == "PRT-001"].iloc[0]
        assert prt001_row["tissue"] == "liver"
        assert prt001_row["timepoint"] == 24
        assert prt001_row["n_observations"] == 2
        # Mean of 15.5 and 18.2
        assert abs(prt001_row["mean_concentration"] - 16.85) < 0.01

    def test_pk_summary_includes_run_id(self, spark_in_vivo_df):
        """Test that run_id is added to output."""
        result = create_pk_summary(spark_in_vivo_df, run_id=42)
        result_pd = result.toPandas()

        assert "run_id" in result_pd.columns
        assert all(result_pd["run_id"] == 42)

    def test_pk_summary_has_expected_columns(self, spark_in_vivo_df):
        """Test that output has all expected columns."""
        result = create_pk_summary(spark_in_vivo_df, run_id=1)

        expected_columns = {
            "protein_id", "tissue", "timepoint",
            "n_observations", "mean_concentration", "std_concentration",
            "min_concentration", "max_concentration", "run_id"
        }
        assert set(result.columns) == expected_columns


@pytest.mark.spark
class TestCreateProteinMaster:
    """Tests for create_protein_master transformation."""

    def test_protein_master_joins_correctly(
        self, spark_protein_info_df, spark_dev_metrics_df, spark_in_vivo_df
    ):
        """Test that protein master joins all data sources."""
        result = create_protein_master(
            spark_protein_info_df,
            spark_dev_metrics_df,
            spark_in_vivo_df,
            run_id=1
        )
        result_pd = result.toPandas()

        # Should have 3 proteins
        assert len(result_pd) == 3

        # Check that metrics are joined
        prt001 = result_pd[result_pd["protein_id"] == "PRT-001"].iloc[0]
        assert prt001["agg_score"] == 0.3
        assert prt001["stab_score"] == 0.8

    def test_protein_master_includes_invivo_stats(
        self, spark_protein_info_df, spark_dev_metrics_df, spark_in_vivo_df
    ):
        """Test that in_vivo summary stats are included."""
        result = create_protein_master(
            spark_protein_info_df,
            spark_dev_metrics_df,
            spark_in_vivo_df,
            run_id=1
        )
        result_pd = result.toPandas()

        # PRT-001 has 2 mice tested
        prt001 = result_pd[result_pd["protein_id"] == "PRT-001"].iloc[0]
        assert prt001["n_mice_tested"] == 2

        # PRT-003 has no in_vivo data, should be null
        prt003 = result_pd[result_pd["protein_id"] == "PRT-003"].iloc[0]
        assert prt003["n_mice_tested"] is None or pd.isna(prt003["n_mice_tested"])

    def test_protein_master_has_expected_columns(
        self, spark_protein_info_df, spark_dev_metrics_df, spark_in_vivo_df
    ):
        """Test that output has expected columns from all sources."""
        result = create_protein_master(
            spark_protein_info_df,
            spark_dev_metrics_df,
            spark_in_vivo_df,
            run_id=1
        )

        # Should have protein info + metrics + in_vivo stats
        assert "protein_id" in result.columns
        assert "sequence" in result.columns  # from protein_info
        assert "agg_score" in result.columns  # from metrics
        assert "n_mice_tested" in result.columns  # from in_vivo


@pytest.mark.spark
class TestCreateTissueExposureSummary:
    """Tests for create_tissue_exposure_summary transformation."""

    def test_tissue_summary_aggregates_correctly(self, spark_in_vivo_df):
        """Test that tissue summary aggregates by tissue and payload."""
        result = create_tissue_exposure_summary(spark_in_vivo_df, run_id=1)
        result_pd = result.toPandas()

        # Should have 2 groups: liver/DM1 and tumor/MMAE
        assert len(result_pd) == 2

        # Check liver/DM1 aggregation
        liver_row = result_pd[result_pd["tissue"] == "liver"].iloc[0]
        assert liver_row["payload"] == "DM1"
        assert liver_row["n_proteins"] == 1
        assert liver_row["n_mice"] == 2

    def test_tissue_summary_includes_median(self, spark_in_vivo_df):
        """Test that median concentration is calculated."""
        result = create_tissue_exposure_summary(spark_in_vivo_df, run_id=1)
        result_pd = result.toPandas()

        assert "median_concentration" in result_pd.columns
        # All values should be non-null
        assert not result_pd["median_concentration"].isna().any()


@pytest.mark.spark
class TestAddRunIdColumn:
    """Tests for add_run_id_column utility."""

    def test_add_run_id(self, spark_in_vivo_df):
        """Test that run_id column is added correctly."""
        result = add_run_id_column(spark_in_vivo_df, run_id=99)
        result_pd = result.toPandas()

        assert "run_id" in result_pd.columns
        assert all(result_pd["run_id"] == 99)

    def test_add_run_id_preserves_data(self, spark_in_vivo_df):
        """Test that adding run_id doesn't modify existing data."""
        original_columns = set(spark_in_vivo_df.columns)
        original_count = spark_in_vivo_df.count()

        result = add_run_id_column(spark_in_vivo_df, run_id=1)

        # Should have same count
        assert result.count() == original_count
        # Should have all original columns plus run_id
        assert original_columns.issubset(set(result.columns))


# Import pandas for null check
import pandas as pd
