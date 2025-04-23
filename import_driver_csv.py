import csv
import logging
import re
from neo4j import GraphDatabase

logging.basicConfig(level=logging.DEBUG)

class Neo4jDriverDataImporter:
    def __init__(self, uri, auth, csv_file, batch_size):
        self.uri = uri
        self.auth = auth
        self.csv_file = csv_file
        self.batch_size = batch_size
        self.total_rows = 0
        self.total_batches = 0

    def _create_database_node(self, tx, batch):
        query = """
            UNWIND $batch AS row
            MERGE (db:Database {dbid: row.dbid})
                ON CREATE SET db.createdAt = datetime(), db.updatedAt = datetime()
                ON MATCH SET db.updatedAt = datetime()
                SET db.neo4j_version = row.neo4jversion

            MERGE (driver:Driver {
                type: row.driver_type,
                version: row.driver_version,
                name: row.driver
            })
                ON CREATE SET driver.createdAt = datetime(), driver.updatedAt = datetime()
                ON MATCH SET driver.updatedAt = datetime()

            MERGE (db)-[:USES]->(driver)
        """
        tx.run(query, batch=batch)

    def import_data(self):
        with GraphDatabase.driver(self.uri, auth=self.auth) as driver:
            with driver.session(database="neo4j") as session:
                with open(self.csv_file, mode='r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    batch = []

                    for row in reader:
                        self.total_rows += 1
                        match = re.search(r'\d+\.\d+\.\d+', row['driver_version'])
                        row['driver_version'] = match.group(0) if match else row['driver_version']
                        batch.append(row)

                        if len(batch) >= self.batch_size:
                            session.write_transaction(self._create_database_node, batch)
                            logging.info(f"Inserted batch of {len(batch)} records.")
                            self.total_batches += 1
                            batch = []

                    if batch:
                        session.write_transaction(self._create_database_node, batch)
                        logging.info(f"Inserted final batch of {len(batch)} records.")
                        self.total_batches += 1

        logging.info(f"\u2705 Total rows processed: {self.total_rows}")
        logging.info(f"\u2705 Total batches inserted: {self.total_batches}")