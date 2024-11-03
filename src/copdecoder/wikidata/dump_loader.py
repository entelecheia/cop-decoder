import bz2
import gzip
import logging
from typing import Generator, Optional, Dict, Set
import xml.etree.ElementTree as ET
from datetime import datetime
import os
from neo4j import GraphDatabase
from tqdm import tqdm
import requests
import hashlib
import argparse


class WikidataLoader:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        """
        Initialize the Wikidata loader with Neo4j connection details.

        Args:
            neo4j_uri: URI for Neo4j database
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.logger = self._setup_logger()
        self.batch_size = 1000
        self.processed_entities = 0

    def _setup_logger(self) -> logging.Logger:
        """Configure logging for the loader."""
        logger = logging.getLogger("WikidataLoader")
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def _setup_neo4j_constraints(self):
        """Set up necessary Neo4j constraints and indexes."""
        with self.driver.session() as session:
            # Create constraints
            constraints = [
                "CREATE CONSTRAINT wikidata_entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
                "CREATE CONSTRAINT wikidata_property_id IF NOT EXISTS FOR (p:Property) REQUIRE p.id IS UNIQUE",
                "CREATE INDEX wikidata_entity_label IF NOT EXISTS FOR (e:Entity) ON (e.label)",
                "CREATE INDEX wikidata_property_label IF NOT EXISTS FOR (p:Property) ON (p.label)",
            ]

            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    self.logger.warning(f"Constraint creation failed: {str(e)}")

    def download_dump(self, dump_url: str, output_path: str) -> str:
        """
        Download Wikidata dump file if not already present.

        Args:
            dump_url: URL of the Wikidata dump
            output_path: Path to save the downloaded file

        Returns:
            Path to the downloaded file
        """
        if os.path.exists(output_path):
            self.logger.info(f"Dump file already exists at {output_path}")
            return output_path

        self.logger.info(f"Downloading dump from {dump_url}")
        response = requests.get(dump_url, stream=True)
        total_size = int(response.headers.get("content-length", 0))

        with open(output_path, "wb") as f:
            with tqdm(total=total_size, unit="iB", unit_scale=True) as pbar:
                for data in response.iter_content(chunk_size=1024 * 1024):
                    size = f.write(data)
                    pbar.update(size)

        return output_path

    def _parse_dump_file(self, file_path: str) -> Generator[ET.Element, None, None]:
        """
        Parse the Wikidata dump file and yield entity elements.

        Args:
            file_path: Path to the dump file

        Yields:
            XML elements representing Wikidata entities
        """
        # Determine file type and open accordingly
        if file_path.endswith(".bz2"):
            open_func = bz2.open
        elif file_path.endswith(".gz"):
            open_func = gzip.open
        else:
            open_func = open

        context = ET.iterparse(open_func(file_path, "rb"), events=("end",))

        for event, elem in context:
            if elem.tag.endswith("entity"):
                yield elem
                elem.clear()

    def _extract_entity_data(self, entity_elem: ET.Element) -> Dict:
        """
        Extract relevant data from entity XML element.

        Args:
            entity_elem: XML element representing a Wikidata entity

        Returns:
            Dictionary containing extracted entity data
        """
        entity_data = {
            "id": entity_elem.get("id"),
            "type": entity_elem.get("type"),
            "labels": {},
            "descriptions": {},
            "claims": [],
        }

        # Extract labels
        labels_elem = entity_elem.find(".//labels")
        if labels_elem is not None:
            for label in labels_elem.findall(".//label"):
                entity_data["labels"][label.get("language")] = label.get("value")

        # Extract descriptions
        descriptions_elem = entity_elem.find(".//descriptions")
        if descriptions_elem is not None:
            for desc in descriptions_elem.findall(".//description"):
                entity_data["descriptions"][desc.get("language")] = desc.get("value")

        # Extract claims/statements
        claims_elem = entity_elem.find(".//claims")
        if claims_elem is not None:
            for claim in claims_elem.findall(".//claim"):
                property_id = claim.get("property")
                mainsnak = claim.find(".//mainsnak")
                if mainsnak is not None:
                    datavalue = mainsnak.find(".//datavalue")
                    if datavalue is not None:
                        value_type = datavalue.get("type")
                        value = datavalue.find(".//value")
                        if value is not None:
                            entity_data["claims"].append(
                                {
                                    "property": property_id,
                                    "value_type": value_type,
                                    "value": (
                                        value.text if value.text else value.get("id")
                                    ),
                                }
                            )

        return entity_data

    def _create_entity_query(self, entity_data: Dict) -> str:
        """Generate Cypher query for creating/updating an entity."""
        labels = {k: v for k, v in entity_data["labels"].items() if v}
        descriptions = {k: v for k, v in entity_data["descriptions"].items() if v}

        query = """
        MERGE (e:Entity {id: $id})
        SET e.type = $type,
            e.labels = $labels,
            e.descriptions = $descriptions,
            e.lastUpdated = datetime()
        """

        return query

    def _create_relationship_query(self, entity_id: str, claim: Dict) -> str:
        """Generate Cypher query for creating relationships between entities."""
        return """
        MATCH (e1:Entity {id: $entity_id})
        MATCH (e2:Entity {id: $target_id})
        MERGE (e1)-[r:HAS_PROPERTY {property: $property}]->(e2)
        SET r.value_type = $value_type,
            r.lastUpdated = datetime()
        """

    def load_dump(self, dump_path: str):
        """
        Load Wikidata dump into Neo4j.

        Args:
            dump_path: Path to the Wikidata dump file
        """
        self.logger.info("Setting up Neo4j constraints and indexes...")
        self._setup_neo4j_constraints()

        self.logger.info(f"Starting to process dump file: {dump_path}")
        batch = []
        relationships = []

        try:
            for entity_elem in self._parse_dump_file(dump_path):
                entity_data = self._extract_entity_data(entity_elem)
                batch.append(entity_data)

                # Collect relationships
                for claim in entity_data["claims"]:
                    if claim["value_type"] == "wikibase-entityid":
                        relationships.append(
                            {
                                "source_id": entity_data["id"],
                                "property": claim["property"],
                                "target_id": claim["value"],
                                "value_type": claim["value_type"],
                            }
                        )

                if len(batch) >= self.batch_size:
                    self._process_batch(batch, relationships)
                    batch = []
                    relationships = []

            # Process remaining items
            if batch:
                self._process_batch(batch, relationships)

        except Exception as e:
            self.logger.error(f"Error processing dump: {str(e)}")
            raise

        self.logger.info(f"Finished processing {self.processed_entities} entities")

    def _process_batch(self, batch: list, relationships: list):
        """Process a batch of entities and their relationships."""
        with self.driver.session() as session:
            # Create entities
            for entity_data in batch:
                session.run(self._create_entity_query(entity_data), entity_data)

            # Create relationships
            for rel in relationships:
                session.run(self._create_relationship_query(rel["source_id"], rel), rel)

        self.processed_entities += len(batch)
        self.logger.info(f"Processed {self.processed_entities} entities")

    def close(self):
        """Close the Neo4j connection."""
        self.driver.close()


def main():
    parser = argparse.ArgumentParser(description="Load Wikidata dump into Neo4j")
    parser.add_argument("--neo4j-uri", required=True, help="Neo4j database URI")
    parser.add_argument("--neo4j-user", required=True, help="Neo4j username")
    parser.add_argument("--neo4j-password", required=True, help="Neo4j password")
    parser.add_argument("--dump-url", required=True, help="URL of Wikidata dump")
    parser.add_argument(
        "--output-path", required=True, help="Path to save/load dump file"
    )

    args = parser.parse_args()

    loader = WikidataLoader(
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
    )

    try:
        # Download dump if necessary
        dump_path = loader.download_dump(args.dump_url, args.output_path)

        # Load dump into Neo4j
        loader.load_dump(dump_path)
    finally:
        loader.close()


if __name__ == "__main__":
    main()
