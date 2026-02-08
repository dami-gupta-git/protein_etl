"""
Unit tests for data reading and parsing utilities.
"""
import pytest
import pandas as pd

from utils.readers import (
    parse_protein_info_json,
    extract_developability_metrics,
    validate_csv_chunk,
    validate_parquet_schema
)


class TestParseProteinInfoJson:
    """Tests for parse_protein_info_json function."""

    def test_parse_valid_json(self, sample_json_file, sample_protein_data):
        """Test parsing a valid JSON file returns expected proteins."""
        result = parse_protein_info_json(sample_json_file)

        assert len(result) == 3
        assert result[0]["protein_id"] == "PRT-001"
        assert result[1]["protein_id"] == "PRT-002"
        assert "developability_metrics" in result[0]

    def test_parse_empty_proteins_raises(self, empty_json_file):
        """Test that empty proteins array raises ValueError."""
        with pytest.raises(ValueError, match="empty proteins array"):
            parse_protein_info_json(empty_json_file)

    def test_parse_invalid_json_raises(self, invalid_json_file):
        """Test that invalid JSON raises ValueError."""
        with pytest.raises(ValueError, match="Invalid JSON"):
            parse_protein_info_json(invalid_json_file)

    def test_parse_missing_key_raises(self, missing_key_json_file):
        """Test that missing 'proteins' key raises KeyError."""
        with pytest.raises(KeyError, match="missing required 'proteins' key"):
            parse_protein_info_json(missing_key_json_file)

    def test_parse_nonexistent_file_raises(self):
        """Test that nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            parse_protein_info_json("/nonexistent/path/file.json")


class TestExtractDevelopabilityMetrics:
    """Tests for extract_developability_metrics function."""

    def test_extract_metrics_success(self, sample_protein_data):
        """Test successful extraction of developability metrics."""
        proteins, metrics = extract_developability_metrics(sample_protein_data)

        # Check proteins are clean (no nested metrics)
        assert len(proteins) == 3
        for p in proteins:
            assert "developability_metrics" not in p
            assert "protein_id" in p

        # Check metrics have protein_id linkage
        assert len(metrics) == 3
        for m in metrics:
            assert "protein_id" in m
            assert "aggregation_score" in m
            assert "stability_score" in m
            assert "expression_level_mg_L" in m

    def test_extract_metrics_preserves_protein_id_link(self, sample_protein_data):
        """Test that protein_id is correctly linked to metrics."""
        proteins, metrics = extract_developability_metrics(sample_protein_data)

        # Verify each metric has correct protein_id
        assert metrics[0]["protein_id"] == "PRT-001"
        assert metrics[0]["aggregation_score"] == 0.3

        assert metrics[1]["protein_id"] == "PRT-002"
        assert metrics[1]["aggregation_score"] == 0.5

    def test_extract_metrics_missing_field_raises(self):
        """Test that missing developability_metrics raises KeyError."""
        bad_data = [{"protein_id": "PRT-001", "sequence": "ACDE"}]

        with pytest.raises(KeyError, match="developability_metrics"):
            extract_developability_metrics(bad_data)


class TestValidateCsvChunk:
    """Tests for validate_csv_chunk function."""

    def test_validate_valid_chunk(self, sample_binding_data):
        """Test validation passes for valid DataFrame."""
        assert validate_csv_chunk(sample_binding_data) is True

    def test_validate_with_required_columns(self, sample_binding_data):
        """Test validation with required columns check."""
        required = ["protein_id", "target", "kd_nm"]
        assert validate_csv_chunk(sample_binding_data, required_columns=required) is True

    def test_validate_empty_raises(self):
        """Test that empty DataFrame raises ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty"):
            validate_csv_chunk(empty_df)

    def test_validate_missing_columns_raises(self, sample_binding_data):
        """Test that missing required columns raises ValueError."""
        required = ["protein_id", "nonexistent_column"]

        with pytest.raises(ValueError, match="Missing required columns"):
            validate_csv_chunk(sample_binding_data, required_columns=required)


class TestValidateParquetSchema:
    """Tests for validate_parquet_schema function."""

    def test_validate_valid_schema(self, sample_in_vivo_data):
        """Test validation passes for DataFrame with expected columns."""
        expected = ["protein_id", "tissue", "timepoint", "concentration_ug_ml"]
        assert validate_parquet_schema(sample_in_vivo_data, expected) is True

    def test_validate_empty_raises(self):
        """Test that empty DataFrame raises ValueError."""
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="empty"):
            validate_parquet_schema(empty_df, ["col1"])

    def test_validate_missing_columns_raises(self, sample_in_vivo_data):
        """Test that missing expected columns raises ValueError."""
        expected = ["protein_id", "nonexistent_column"]

        with pytest.raises(ValueError, match="Missing expected columns"):
            validate_parquet_schema(sample_in_vivo_data, expected)
