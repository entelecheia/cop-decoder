from dataclasses import dataclass
from typing import List, Optional

import requests


@dataclass
class WikidataEntity:
    id: str
    label: str
    description: str
    url: str


class WikidataAPI:
    def __init__(self):
        self.endpoint = "https://www.wikidata.org/w/api.php"

    def search_entity(
        self, query: str, language: str = "en", limit: int = 5
    ) -> List[WikidataEntity]:
        """
        Search for Wikidata entities by name and return their IDs and metadata.

        Args:
            query: Name of entity to search
            language: Language code (default: "en")
            limit: Maximum number of results to return

        Returns:
            List of WikidataEntity objects
        """
        params = {
            "action": "wbsearchentities",
            "format": "json",
            "search": query,
            "language": language,
            "limit": limit,
        }

        try:
            response = requests.get(self.endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            entities = []
            for item in data.get("search", []):
                entity = WikidataEntity(
                    id=item.get("id", ""),
                    label=item.get("label", ""),
                    description=item.get("description", "No description available"),
                    url=item.get("url", ""),
                )
                entities.append(entity)

            return entities

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Wikidata: {e}")
            return []

    def get_best_match(
        self, query: str, language: str = "en"
    ) -> Optional[WikidataEntity]:
        """
        Get the best matching entity for a given query.

        Args:
            query: Name of entity to search
            language: Language code

        Returns:
            WikidataEntity object or None if no match found
        """
        entities = self.search_entity(query, language, limit=1)
        return entities[0] if entities else None


def main():
    wikidata = WikidataAPI()

    # Example queries
    test_queries = ["Albert Einstein", "Google LLC", "Seoul National University"]

    print("\nWikidata Entity Search Results:")
    print("-" * 50)

    for query in test_queries:
        entity = wikidata.get_best_match(query)
        if entity:
            print(f"\nQuery: {query}")
            print(f"Entity ID: {entity.id}")
            print(f"Label: {entity.label}")
            print(f"Description: {entity.description}")
            print(f"URL: {entity.url}")
        else:
            print(f"\nNo results found for: {query}")


if __name__ == "__main__":
    main()
