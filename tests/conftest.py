"""
Pytest fixtures for ProteinETL tests.
"""
import json
import os
import tempfile
from typing import Dict, List

import pandas as pd
import pytest


@pytest.fixture
def sample_protein_data() -> List[Dict]:
    """Sample protein data matching production schema."""
    return [
        {
            "protein_id": "PRT-001",
            "sequence": "ACDEFGHIKLMNPQRSTVWY",
            "molecular_weight_da": 25000.0,
            "theoretical_pi": 6.5,
            "structure_type": "Fab",
            "target": "PD1",
            "developability_metrics": {
                "aggregation_score": 0.3,
                "stability_score": 0.8,
                "expression_level_mg_L": 250.0
            }
        },
        {
            "protein_id": "PRT-002",
            "sequence": "FGHIKLMNPQRSTVWYACDE",
            "molecular_weight_da": 28000.0,
            "theoretical_pi": 7.2,
            "structure_type": "scFv",
            "target": "CD19",
            "developability_metrics": {
                "aggregation_score": 0.5,
                "stability_score": 0.6,
                "expression_level_mg_L": 180.0
            }
        },
        {
            "protein_id": "PRT-003",
            "sequence": "KLMNPQRSTVWYACDEFGHI",
            "molecular_weight_da": 30000.0,
            "theoretical_pi": 5.8,
            "structure_type": "VHH",
            "target": "HER2",
            "developability_metrics": {
                "aggregation_score": 0.2,
                "stability_score": 0.9,
                "expression_level_mg_L": 320.0
            }
        }
    ]


@pytest.fixture
def sample_json_file(sample_protein_data, tmp_path) -> str:
    """Create a temporary JSON file with sample protein data."""
    file_path = tmp_path / "test_protein_info.json"
    with open(file_path, 'w') as f:
        json.dump({"proteins": sample_protein_data}, f)
    return str(file_path)


@pytest.fixture
def empty_json_file(tmp_path) -> str:
    """Create a temporary JSON file with empty proteins array."""
    file_path = tmp_path / "empty_protein_info.json"
    with open(file_path, 'w') as f:
        json.dump({"proteins": []}, f)
    return str(file_path)


@pytest.fixture
def invalid_json_file(tmp_path) -> str:
    """Create a temporary file with invalid JSON."""
    file_path = tmp_path / "invalid.json"
    with open(file_path, 'w') as f:
        f.write("{ invalid json }")
    return str(file_path)


@pytest.fixture
def missing_key_json_file(tmp_path) -> str:
    """Create a JSON file missing the 'proteins' key."""
    file_path = tmp_path / "missing_key.json"
    with open(file_path, 'w') as f:
        json.dump({"data": []}, f)
    return str(file_path)


@pytest.fixture
def sample_binding_data() -> pd.DataFrame:
    """Sample protein binding data."""
    return pd.DataFrame({
        "protein_id": ["PRT-001", "PRT-001", "PRT-002", "PRT-002"],
        "target": ["PD1", "PD1", "CD19", "CD19"],
        "kd_nm": [1.5, 1.8, 2.3, 2.1],
        "kon": [1e5, 1.1e5, 9e4, 9.5e4],
        "koff": [1e-4, 1.2e-4, 2e-4, 1.8e-4]
    })


@pytest.fixture
def sample_csv_file(sample_binding_data, tmp_path) -> str:
    """Create a temporary CSV file with sample binding data."""
    file_path = tmp_path / "test_binding_data.csv"
    sample_binding_data.to_csv(file_path, index=False)
    return str(file_path)


@pytest.fixture
def empty_csv_file(tmp_path) -> str:
    """Create an empty CSV file."""
    file_path = tmp_path / "empty.csv"
    with open(file_path, 'w') as f:
        f.write("")
    return str(file_path)


@pytest.fixture
def sample_in_vivo_data() -> pd.DataFrame:
    """Sample in_vivo measurements data."""
    return pd.DataFrame({
        "molecule": ["PRT-001-DM1", "PRT-001-DM1", "PRT-002-MMAE", "PRT-002-MMAE"],
        "mouse_id": ["M001", "M002", "M001", "M003"],
        "date": ["01/15/2025", "01/15/2025", "01/16/2025", "01/16/2025"],
        "tissue": ["liver", "liver", "tumor", "tumor"],
        "timepoint": [24, 24, 48, 48],
        "protein_id": ["PRT-001", "PRT-001", "PRT-002", "PRT-002"],
        "payload": ["DM1", "DM1", "MMAE", "MMAE"],
        "concentration_ug_ml": [15.5, 18.2, 8.3, 9.1]
    })


@pytest.fixture
def sample_parquet_file(sample_in_vivo_data, tmp_path) -> str:
    """Create a temporary parquet file with sample in_vivo data."""
    file_path = tmp_path / "test_in_vivo.parquet"
    sample_in_vivo_data.to_parquet(file_path, index=False)
    return str(file_path)


@pytest.fixture
def spark_session():
    """Create a local Spark session for testing."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("ProteinETL-Test") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture
def spark_in_vivo_df(spark_session, sample_in_vivo_data):
    """Create a Spark DataFrame from sample in_vivo data."""
    return spark_session.createDataFrame(sample_in_vivo_data)


@pytest.fixture
def spark_protein_info_df(spark_session, sample_protein_data):
    """Create a Spark DataFrame from sample protein data (without nested metrics)."""
    proteins_flat = [
        {k: v for k, v in p.items() if k != 'developability_metrics'}
        for p in sample_protein_data
    ]
    return spark_session.createDataFrame(proteins_flat)


@pytest.fixture
def spark_dev_metrics_df(spark_session, sample_protein_data):
    """Create a Spark DataFrame from sample developability metrics."""
    metrics = []
    for p in sample_protein_data:
        m = p['developability_metrics'].copy()
        m['protein_id'] = p['protein_id']
        metrics.append(m)
    return spark_session.createDataFrame(metrics)
