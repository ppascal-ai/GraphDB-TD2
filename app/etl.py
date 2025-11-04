import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from neo4j import GraphDatabase, Driver
from dotenv import load_dotenv
from pathlib import Path


load_dotenv(override=True)


def log(message: str) -> None:
    print(f"[ETL] {message}")


def get_pg_conn():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        cursor_factory=RealDictCursor,
    )
    conn.autocommit = True
    return conn


def get_neo4j_driver() -> Driver:
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    return GraphDatabase.driver(uri, auth=(user, password))


def wait_for_postgres(timeout_seconds: int = 120, backoff_seconds: float = 2.0) -> None:
    start = time.time()
    while True:
        try:
            with get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()
            log("Postgres is available")
            return
        except Exception as e:
            if time.time() - start > timeout_seconds:
                raise RuntimeError(f"Timed out waiting for Postgres: {e}")
            log(f"Waiting for Postgres... ({e})")
            time.sleep(backoff_seconds)


def wait_for_neo4j(timeout_seconds: int = 120, backoff_seconds: float = 2.0) -> Driver:
    start = time.time()
    while True:
        try:
            driver = get_neo4j_driver()
            # Verify connectivity (Neo4j >= 4.x)
            driver.verify_connectivity()
            with driver.session() as session:
                session.run("RETURN 1").consume()
            log("Neo4j is available")
            return driver
        except Exception as e:
            if time.time() - start > timeout_seconds:
                raise RuntimeError(f"Timed out waiting for Neo4j: {e}")
            log(f"Waiting for Neo4j... ({e})")
            time.sleep(backoff_seconds)


def run_cypher(driver: Driver, query: str, params: Optional[Dict[str, Any]] = None) -> None:
    with driver.session() as session:
        session.run(query, params or {}).consume()


def run_cypher_file(driver: Driver, path: str) -> None:
    if not os.path.exists(path):
        log(f"Cypher file not found: {path}")
        return
    content = open(path, "r", encoding="utf-8").read()
    # Naive split on ';' is acceptable for simple DDL
    statements = [s.strip() for s in content.split(";") if s.strip()]
    for stmt in statements:
        run_cypher(driver, stmt)


def chunked(iterable: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for i in range(0, len(iterable)):
        if i % size == 0:
            yield iterable[i : i + size]


def chunk(iterable: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    """Alias to match requested API name; splits sequences into batches of given size."""
    return chunked(iterable, size)


def fetch_all(cur, sql: str) -> List[Dict[str, Any]]:
    cur.execute(sql)
    rows = cur.fetchall()
    return list(rows)


def etl() -> None:
    # 1) Wait for both DBs
    wait_for_postgres()
    driver = wait_for_neo4j()

    try:
        # 2) Apply queries.cypher
        # Prefer local queries.cypher next to this file; fallback to env/default path
        local_queries = Path(__file__).with_name("queries.cypher")
        queries_path = str(local_queries if local_queries.exists() else Path(os.getenv("QUERIES_PATH", "/workspace/app/queries.cypher")))
        log("Applying Neo4j constraints and indexes...")
        run_cypher_file(driver, queries_path)

        # 3) Extract from Postgres
        log("Extracting data from Postgres...")
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                customers = []
                products = []
                categories = []
                orders = []
                order_items = []
                events = []

                def safe_fetch(name: str, sql: str) -> List[Dict[str, Any]]:
                    try:
                        rows = fetch_all(cur, sql)
                        log(f"Fetched {len(rows)} {name}")
                        return rows
                    except Exception as e:
                        log(f"Warning: could not fetch {name}: {e}")
                        return []

                customers = safe_fetch(
                    "customers",
                    "SELECT id, name, join_date FROM customers",
                )
                products = safe_fetch(
                    "products",
                    "SELECT id, name, category_id FROM products",
                )
                categories = safe_fetch(
                    "categories",
                    "SELECT id, name FROM categories",
                )
                orders = safe_fetch(
                    "orders",
                    "SELECT id, customer_id, ts FROM orders",
                )
                order_items = safe_fetch(
                    "order_items",
                    "SELECT order_id, product_id, quantity FROM order_items",
                )
                events = safe_fetch(
                    "events",
                    "SELECT customer_id, product_id, event_type FROM events",
                )

        # 4) Load nodes and relationships (MERGE + batching)
        batch_size = int(os.getenv("BATCH_SIZE", "500"))

        log("Loading Categories...")
        for batch in chunk(categories, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MERGE (c:Category {id: row.id})
                SET c.name = row.name
                """,
                {"rows": batch},
            )

        log("Loading Products...")
        for batch in chunk(products, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MERGE (p:Product {id: row.id})
                SET p.name = row.name,
                    p.category_id = row.category_id
                """,
                {"rows": batch},
            )

        log("Linking Products to Categories...")
        for batch in chunk(products, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MATCH (p:Product {id: row.id})
                MATCH (c:Category {id: row.category_id})
                MERGE (p)-[:IN_CATEGORY]->(c)
                """,
                {"rows": batch},
            )

        log("Loading Customers...")
        for batch in chunk(customers, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MERGE (c:Customer {id: row.id})
                SET c.name = row.name,
                    c.join_date = row.join_date
                """,
                {"rows": batch},
            )

        log("Loading Orders...")
        for batch in chunk(orders, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MERGE (o:Order {id: row.id})
                SET o.ts = row.ts
                WITH o, row
                MATCH (c:Customer {id: row.customer_id})
                MERGE (c)-[:PLACED]->(o)
                """,
                {"rows": batch},
            )

        log("Loading Order-Item relationships...")
        for batch in chunk(order_items, batch_size):
            run_cypher(
                driver,
                """
                UNWIND $rows AS row
                MATCH (o:Order {id: row.order_id})
                MATCH (p:Product {id: row.product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = row.quantity
                """,
                {"rows": batch},
            )

        # Map events to relationship types
        event_type_to_rel = {
            "view": "VIEWED",
            "click": "CLICKED",
            "add_to_cart": "ADDED_TO_CART",
        }

        # Split events by type to keep Cypher simple
        log("Loading Event relationships...")
        by_type: Dict[str, List[Dict[str, Any]]] = {t: [] for t in event_type_to_rel}
        for e in events:
            t = str(e.get("event_type", "")).lower()
            if t in by_type:
                by_type[t].append(e)

        for t, rel in event_type_to_rel.items():
            data = by_type.get(t, [])
            if not data:
                continue
            log(f"Loading {rel} events...")
            for batch in chunk(data, batch_size):
                run_cypher(
                    driver,
                    f"""
                    UNWIND $rows AS row
                    MATCH (c:Customer {{id: row.customer_id}})
                    MATCH (p:Product {{id: row.product_id}})
                    MERGE (c)-[:{rel}]->(p)
                    """,
                    {"rows": batch},
                )

    finally:
        try:
            driver.close()
        except Exception:
            pass

    # 5) Final required log line
    print("ETL done.")


def main() -> None:
    etl()


if __name__ == "__main__":
    main()

