# Protein Data Engineering Pipeline

The `protein_etl` project is an Apache Airflow-based Extract, Transform, Load (ETL) pipeline designed for processing
proteomics data. It ingests raw data from various formats (CSV, JSON, and Parquet), performs data validation and
cleaning, and loads the processed data into a PostgreSQL database. The pipeline is tailored for handling
protein-related datasets, including protein binding affinities, protein metadata, and in vivo measurement data,
making it suitable for bioinformatics and drug development workflows.

The pipeline tracks each run using a unique `run_id` stored in a `start_info` table, ensuring traceability.

## Sample Pipeline
<img width="1026" height="383" alt="image" src="https://github.com/user-attachments/assets/b541c6fe-3a19-4c70-846e-307ad4755ac8" />


## Features
- Custom Airflow DAGs for ETL workflows
- **Apache Spark** integration for large-scale data processing
- SQL-based schema initialization
- Dockerized for portable development and deployment
- CI/CD pipeline with Jenkins
- Comprehensive unit and integration test suite
- Configurable via environment variables

**Data Ingestion**: Reads data from:
* `mock_binding_data.csv`: Protein binding data (e.g., association/dissociation rates, affinity).  
* `mock_protein_info.json`: Protein metadata (e.g., sequence, molecular weight, developability metrics).
* `mock_in_vivo_measurements.parquet`: In vivo measurements (e.g., concentration in tissues over time).

**Data Transformation:**
* Normalizes nested JSON data (e.g., splits `developability_metrics` into a separate `protein_developability_metrics` table).
* Flags candidate proteins based on developability thresholds (aggregation score, stability, expression level).
* Flags outlier binding records based on affinity standard deviation.
* Cross-references protein IDs across tables and writes issues to a `data_quality` table.

**Data Loading**: Stores processed data in a PostgreSQL database under the protein_etl schema, with tables:
* `protein_binding`
* `protein_info`
* `protein_developability_metrics`
* `in_vivo_measurements`
* `pk_summary` (pharmacokinetic statistics per protein/tissue/timepoint)
* `protein_master` (joined view of protein info, developability metrics, and in vivo stats)
* `tissue_exposure_summary` (drug exposure summary by tissue and payload)
* `start_info` (for run metadata)
* `data_quality` (for cross-reference issues flagged during post-processing)
* `run_summary` (for run-level counts and status)

**Spark Transformations**: Creates aggregated analytical tables:
* `pk_summary` - Pharmacokinetic statistics per protein/tissue/timepoint
* `protein_master` - Joined view of protein info, developability metrics, and in vivo stats
* `tissue_exposure_summary` - Drug exposure summary by tissue and payload

**Scalability**: Processes large CSV files in chunks and uses Apache Spark for parquet ingestion and heavy transformations.

## Building and Running

1. **Prerequisites**

- Docker and Docker Compose
- (Optional) Python 3.11+ for local script testing
- Git


2. **Clone the repository and Setup**

Clone the repo as
```
git clone https://github.com/your-username/protein_etl.git
```
Navigate to the root directory to make the scripts executable
```
cd protein_etl
chmod +rwx scripts/*
```
Make sure that Docker is installed and running.

3. **Build and run**
```
docker compose up -d
```
Give it a minute to come up. Note there will be errors thrown while it is starting up, these can be ignored.

> **Note:** If you need a clean database (e.g. first run or after schema changes), bring down the stack with the volume flag so `init.sql` re-runs on next start:
> ```
> docker compose down -v && docker compose up -d
> ```

4. **Run the DAG** 
Open a browser, and point it to `http://localhost:8080/`. This will bring you to a login screen.
Use credentials `admin` as both Username and Password to Sign In. This will bring up a list of DAGs,
click on the `protein_etl` DAG, and press on the `Trigger DAG` button. 
   
This will run the Airflow DAG. 

5. **Output**
If you click the `Graph` button, you will see the final DAG. If all tasks run successfully, it will look like the below   
<img width="1520" height="578" alt="image" src="https://github.com/user-attachments/assets/aca6b216-99af-4c67-ba4e-42d19dd118ab" />
<br>     

To view the output from the run in the postgres db, you can use the connection string  

`postgres://postgres:postgres@localhost:5436/postgres`


6. **Stopping the Application**
When you want to stop the application, do
```
docker compose down
```

## Testing

The project includes comprehensive unit and integration tests using pytest.

### Test Structure

```
tests/
├── conftest.py                 # Shared fixtures (sample data, Spark session)
├── unit/
│   ├── test_readers.py         # Tests for data parsing and validation
│   └── test_transformations.py # Tests for Spark transformations
└── integration/
    └── test_reshape.py         # End-to-end pipeline tests
```

### Running Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Run unit tests only (fast, no Spark)
pytest -m "not spark"

# Run Spark tests only
pytest -m spark

# Run with coverage report
pytest --cov=dags --cov-report=html

# Run integration tests
pytest -m integration
```

### Test Markers

- `@pytest.mark.unit` - Fast unit tests with no external dependencies
- `@pytest.mark.spark` - Tests requiring a local Spark session
- `@pytest.mark.integration` - Integration tests for full pipeline

## Architecture

### Spark Integration

The pipeline uses Apache Spark for:

1. **Parquet Ingestion** (`read_data_in_vivo_measurements`)
   - Native Spark parquet reader for efficient columnar data processing
   - Handles 50,000+ records efficiently
   - Writes to PostgreSQL via JDBC

2. **Data Transformations** (`reshape_data`)
   - Aggregations using Spark SQL functions
   - Joins across multiple tables
   - Statistical calculations (mean, stddev, percentiles)

### Data Flow

```
Raw Files                    Staging Tables              Analytical Tables
-----------                  --------------              -----------------
CSV (binding)      -->       protein_binding (is_outlier flag)
JSON (protein)     -->       protein_info          -->  protein_master (is_candidate flag)
                   -->       protein_developability_metrics
Parquet (in vivo)  -->       in_vivo_measurements  -->  pk_summary
                                                   -->  tissue_exposure_summary
                                                                                    data_quality
                                                                                    run_summary
```
