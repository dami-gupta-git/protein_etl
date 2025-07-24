# Protein Data Engineering Pipeline

The 'protein_etl' project is an Apache Airflow-based Extract, Transform, Load (ETL) pipeline designed for processing 
proteomics data. It ingests raw data from various formats (CSV, JSON, and Parquet), performs data validation and 
cleaning, and loads the processed data into a PostgreSQL database. The pipeline is tailored for handling 
protein-related datasets, including protein binding affinities, protein metadata, and in vivo measurement data, 
making it suitable for bioinformatics and drug development workflows.

The pipeline tracks each run using a unique `run_id` stored in a `start_info` table, ensuring traceability. 
It uses Airflow's `TaskGroup` to organize tasks and a custom `CleaningOperator` to filter data based on 
specific criteria (e.g., affinity thresholds). 

The project is currently under development, with some tasks (e.g., data checks, reshaping, error-handling, 
and post-processing) as placeholders for future implementation.

## Features
- Custom Airflow DAGs for ETL workflows
- SQL-based schema initialization
- Dockerized for portable development and deployment
- CI/CD pipeline with Jenkins
- Configurable via environment variables

**Data Ingestion**: Reads data from:
* `mock_binding_data.csv`: Protein binding data (e.g., association/dissociation rates, affinity).  
* `mock_protein_info.json`: Protein metadata (e.g., sequence, molecular weight, developability metrics).
* `mock_in_vivo_measurements.parquet`: In vivo measurements (e.g., concentration in tissues over time).

**Data Transformation:**
* Normalizes nested JSON data (e.g., splits developability_metrics into a separate table).
* Filters data using a custom `CleaningOperator` (e.g., removes rows with affinity <= 5.0).

**Data Loading**: Stores processed data in a PostgreSQL database under the protein_etl schema, with tables:
* `protein_binding`
* `protein_info`
* `protein_developability_metrics`
* `start_info` (for run metadata)



**Scalability**: Processes large CSV files in chunks to handle memory constraints.
**Modularity**: Uses Airflow `TaskGroup` for task organization and a custom operator for reusable cleaning logic.

## Building and Running

1. **Prerequisites**

- Docker and Docker Compose
- (Optional) Python 3.8+ for local script testing
- Git


2. **Clone the repository and Setup**

Clone the repo as
```
git clone https://github.com/your-username/protein_data_eng.git
```
Navigate to the root directory to make the scripts executable
```
cd protein_data_eng
chmod +rwx scripts/*
```
Make sure that Docker is installed and running. 

Build and run
```
docker compose up -d
```
Give it a minute to come up. Note there will be errors thrown while it is starting up, these can be ignored. 


4. **Run the DAG** 
Open a browser, and point it to `http://localhost:8080/`. This will bring you to a login screen.
Use credentials `admin` as both Username and Password to Sign In. This will bring up a list of DAGs,
click on the `protein_etl` DAG, and press on the `Trigger DAG` button. 
   
This will run the Airflow DAG. (You can click on `Graph` to see the UI)

To view the output from the run in the postgres db, you can use the connection string

`postgres://postgres:postgres@localhost:5436/postgres`

5. **Stopping the Application** 
When you want to stop the application, do
```
docker compose down
```
