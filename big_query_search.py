import csv
import logging
from google.cloud import bigquery


class BigQueryDriverExporter:
    def __init__(self, output_file: str):
        self.output_file = output_file
        self.client = bigquery.Client()
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG)

    def run_query(self):
        self.logger.debug("Initializing BigQuery query...")
        query = """
        WITH extracted AS (
          SELECT
            jsonPayload.dbid,
            jsonPayload.neo4jversion,
            REGEXP_EXTRACT(jsonPayload.source, r'.*\t.*\t(.*)\t\t.*') AS driver
          FROM `neo4j-cloud.query_logs.neo4j_query`
          WHERE jsonPayload.event != 'start'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY)
            AND REGEXP_EXTRACT(jsonPayload.source, r'.*\t.*\t(.*)\t\t.*') IS NOT NULL
            AND jsonPayload.source NOT LIKE '%neo4j-php-client%'
        )
        SELECT
          dbid,
          neo4jversion,
          driver,
          REGEXP_EXTRACT(driver, r'^([^/]+)/') AS driver_type,
          REGEXP_EXTRACT(driver, r'/([^ ]+)') AS driver_version
        FROM extracted
        GROUP BY dbid, neo4jversion, driver;
        """
        return self.client.query(query).result()

    def export_to_csv(self):
        self.logger.debug("Fetching query results...")
        results = self.run_query()

        self.logger.debug(f"Saving results to {self.output_file}...")
        with open(self.output_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["dbid", "neo4jversion", "driver", "driver_type", "driver_version"])
            for row in results:
                writer.writerow([
                    row["dbid"],
                    row["neo4jversion"],
                    row["driver"],
                    row["driver_type"],
                    row["driver_version"]
                ])

        self.logger.info(f"Results successfully saved to {self.output_file}")
