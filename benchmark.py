import pandas as pd
from ingestor import Neo4JIngestor, PSQLIngestor
from queries import (
    POSTGRES_SIMPLE,
    POSTGRES_COMPLEX,
    NEO4J_SIMPLE,
    NEO4J_COMPLEX
)

def run_metrics(pg_runner: PSQLIngestor | None = None, neo4j_runner: Neo4JIngestor | None = None):
    if pg_runner is None and neo4j_runner is None:
        raise ValueError("No database is provided")

    results = []

    if pg_runner is not None:
        results += pg_runner.metrics(
            POSTGRES_SIMPLE,  "simple"
        )
        results += pg_runner.metrics(
            POSTGRES_COMPLEX, "complex"
        )

    if neo4j_runner is not None:
        results += neo4j_runner.metrics(
            NEO4J_SIMPLE, "simple"
        )
        results += neo4j_runner.metrics(
            NEO4J_COMPLEX, "complex"
        )

    df = pd.DataFrame(results)
    df["preview"] = df["preview"].astype(str)
    df.to_csv("benchmark_results.csv", index=False)

    return df

