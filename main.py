# main.py
from big_query_search import BigQueryDriverExporter
from import_driver_csv import Neo4jDriverDataImporter
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class DriverDataPipeline:
    def __init__(self, bq_output_path, neo4j_uri, neo4j_auth, batch_size):
        self.bq_output_path = bq_output_path
        self.neo4j_uri = neo4j_uri
        self.neo4j_auth = neo4j_auth
        self.batch_size = batch_size

    def run(self):
        print("Fetching data from BigQuery...")
        fetcher = BigQueryDriverExporter(self.bq_output_path)
        fetcher.export_to_csv()

        print("Importing data into Neo4j...")
        importer = Neo4jDriverDataImporter(self.neo4j_uri, self.neo4j_auth, self.bq_output_path, self.batch_size)
        importer.import_data()

if __name__ == "__main__":
    # Load configuration from environment variables
    BQ_OUTPUT_PATH = os.environ.get("BQ_OUTPUT_PATH")
    NEO4J_URI = os.environ.get("NEO4J_URI")
    NEO4J_USER = os.environ.get("NEO4J_USER")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))

    # Validate required variables
    missing = [var for var in ("BQ_OUTPUT_PATH", "NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD") if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    NEO4J_AUTH = (NEO4J_USER, NEO4J_PASSWORD)

    pipeline = DriverDataPipeline(BQ_OUTPUT_PATH, NEO4J_URI, NEO4J_AUTH, BATCH_SIZE)
    pipeline.run()
