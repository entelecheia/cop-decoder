import networkx as nx
import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper


class WikidataCentrality:
    def __init__(self):
        self.endpoint = "https://query.wikidata.org/sparql"
        self.sparql = SPARQLWrapper(self.endpoint)
        self.sparql.setReturnFormat(JSON)

    def get_subgraph(self, entity_id: str, max_nodes: int = 5000) -> list:
        """
        Fetch a subgraph around an entity using SPARQL

        Args:
            entity_id: Wikidata ID (e.g., 'Q937' for Albert Einstein)
            max_nodes: Maximum number of nodes to return
        """
        query = f"""
        SELECT DISTINCT ?source ?target
        WHERE {{
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            {{
                SELECT DISTINCT ?source ?target
                WHERE {{
                    VALUES ?start {{ wd:{entity_id} }}
                    ?source ?p ?target .
                    {{
                        # Forward traversal
                        wd:{entity_id} ?p1 ?target .
                        ?target ?p2 ?source .
                    }} UNION {{
                        # Backward traversal
                        ?source ?p1 wd:{entity_id} .
                        ?target ?p2 ?source .
                    }}
                    FILTER(ISIRI(?source) && ISIRI(?target))
                    FILTER(?p != wdt:P31 && ?p != wdt:P279)  # Exclude instance-of and subclass-of
                }}
                LIMIT {max_nodes}
            }}
        }}
        """

        try:
            self.sparql.setQuery(query)
            results = self.sparql.query().convert()

            edges = []
            for result in results["results"]["bindings"]:
                source = result["source"]["value"].split("/")[-1]
                target = result["target"]["value"].split("/")[-1]
                edges.append((source, target))

            return edges

        except Exception as e:
            print(f"Error querying Wikidata: {e}")
            return []

    def calculate_centrality(self, entity_id: str) -> dict:
        """
        Calculate PageRank, Eigenvector, and Degree centrality for an entity

        Args:
            entity_id: Wikidata ID

        Returns:
            Dictionary containing centrality metrics
        """
        # Get the subgraph
        edges = self.get_subgraph(entity_id)

        if not edges:
            return {
                "entity_id": entity_id,
                "pagerank": None,
                "eigenvector": None,
                "degree": None,
                "node_count": 0,
                "edge_count": 0,
            }

        # Create NetworkX graph
        G = nx.DiGraph()
        G.add_edges_from(edges)

        # Calculate centralities
        try:
            pagerank = nx.pagerank(G, alpha=0.85, max_iter=100)
            eigenvector = nx.eigenvector_centrality(G, max_iter=100)
            degree = nx.degree_centrality(G)

            return {
                "entity_id": entity_id,
                "pagerank": pagerank.get(entity_id, None),
                "eigenvector": eigenvector.get(entity_id, None),
                "degree": degree.get(entity_id, None),
                "node_count": G.number_of_nodes(),
                "edge_count": G.number_of_edges(),
            }

        except Exception as e:
            print(f"Error calculating centrality: {e}")
            return {
                "entity_id": entity_id,
                "pagerank": None,
                "eigenvector": None,
                "degree": None,
                "node_count": G.number_of_nodes(),
                "edge_count": G.number_of_edges(),
            }


def main():
    # Initialize the centrality calculator
    wikidata = WikidataCentrality()

    # Example entities to analyze
    entities = [
        "Q937",  # Albert Einstein
        "Q5284",  # Isaac Newton
        "Q7251",  # Marie Curie
    ]

    # Calculate centralities
    results = []
    for entity_id in entities:
        print(f"\nCalculating centrality for {entity_id}...")
        centrality = wikidata.calculate_centrality(entity_id)
        results.append(centrality)

        print(f"PageRank: {centrality['pagerank']:.6f}")
        print(f"Eigenvector Centrality: {centrality['eigenvector']:.6f}")
        print(f"Degree Centrality: {centrality['degree']:.6f}")
        print(f"Nodes in subgraph: {centrality['node_count']}")
        print(f"Edges in subgraph: {centrality['edge_count']}")

    # Create DataFrame with results
    df = pd.DataFrame(results)
    print("\nFinal Results:")
    print(df)


if __name__ == "__main__":
    main()
