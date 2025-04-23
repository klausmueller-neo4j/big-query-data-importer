# BigQuery to Neo4j Driver Data Importer

This Python project provides a simple pipeline to:
1. Export driver usage data from Google BigQuery.
2. Import the exported CSV data into a Neo4j graph database.

## Components
- **big_query_search.py**
  - `BigQueryDriverExporter`: connects to BigQuery, runs a pre-defined SQL query against
    `neo4j-cloud.query_logs.neo4j_query`, and writes the resulting rows to a CSV file.
- **import_driver_csv.py**
  - `Neo4jDriverDataImporter`: reads the CSV file, batches rows, and writes them to Neo4j.
    - Creates/MERGE `Database` nodes with `dbid` and `neo4j_version`.
    - Creates/MERGE `Driver` nodes with `name`, `type`, and `version`.
    - Establishes `(:Database)-[:USES]->(:Driver)` relationships.
- **main.py**
  - Orchestrates the pipeline:
    1. Loads configuration from environment variables (or optionally a `.env` file via python-dotenv).
    2. Validates required settings (`BQ_OUTPUT_PATH`, `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`).
    3. Executes export and import steps with a configurable batch size (`BATCH_SIZE`, default 1000).

## Requirements
- Python 3.7 or newer
- Dependencies (see `requirements.txt`):
  ```
  google-cloud-bigquery
  neo4j
  python-dotenv
  ```

## Setup & Usage
1. **Clone the repository**
   ```bash
   git clone https://github.com/klausmueller-neo4j/big-query-data-importer.git
   cd big-query-data-importer
   ```
2. **Install dependencies**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Configure environment variables**
   Create a `.env` file in the project root or set variables manually:
   ```env
   BQ_OUTPUT_PATH=/path/to/driver-version-by-dbid.csv
   NEO4J_URI=neo4j+ssc://<YOUR_HOST>:7687
   NEO4J_USER=<USERNAME>
   NEO4J_PASSWORD=<PASSWORD>
   BATCH_SIZE=1000    # optional, defaults to 1000
   ```
4. **Run the pipeline**
   ```bash
   python main.py
   ```

After running, you will have:
- A CSV file at `BQ_OUTPUT_PATH` containing the extracted data.
- Nodes and relationships loaded into your Neo4j database.