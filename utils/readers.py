"""
Data reading and parsing utilities for ProteinETL.
These are pure functions extracted from DAG tasks for testability.
"""
import json
from typing import Dict, List, Tuple, Any

import pandas as pd


def parse_protein_info_json(file_path: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse protein info JSON file and extract protein data and developability metrics.

    Args:
        file_path: Path to the JSON file

    Returns:
        Tuple of (protein_records, developability_metrics_records)

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If JSON is invalid or missing required fields
        KeyError: If required keys are missing
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}") from e

    if "proteins" not in data:
        raise KeyError(f"JSON file {file_path} missing required 'proteins' key")

    protein_list = data["proteins"]
    if not protein_list:
        raise ValueError(f"JSON file {file_path} contains empty proteins array")

    return protein_list


def extract_developability_metrics(protein_records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Extract and separate developability metrics from protein records.

    Args:
        protein_records: List of protein dictionaries with nested developability_metrics

    Returns:
        Tuple of (protein_records_without_metrics, developability_metrics_with_protein_id)

    Raises:
        KeyError: If developability_metrics field is missing
    """
    proteins_clean = []
    metrics_records = []

    for protein in protein_records:
        if 'developability_metrics' not in protein:
            raise KeyError("Protein data missing required 'developability_metrics' field")

        # Extract metrics with protein_id linkage
        metrics = protein['developability_metrics'].copy()
        metrics['protein_id'] = protein['protein_id']
        metrics_records.append(metrics)

        # Create protein record without nested metrics
        protein_clean = {k: v for k, v in protein.items() if k != 'developability_metrics'}
        proteins_clean.append(protein_clean)

    return proteins_clean, metrics_records


def validate_csv_chunk(df: pd.DataFrame, required_columns: List[str] = None) -> bool:
    """
    Validate a CSV chunk has expected structure.

    Args:
        df: DataFrame chunk to validate
        required_columns: Optional list of required column names

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    if df.empty:
        raise ValueError("DataFrame chunk is empty")

    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    return True


def validate_parquet_schema(df: pd.DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate parquet DataFrame has expected columns.

    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    if df.empty:
        raise ValueError("DataFrame is empty")

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    return True
