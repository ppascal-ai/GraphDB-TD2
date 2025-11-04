import os
import time
from typing import Dict, List, Optional

from fastapi import FastAPI
from etl import etl as run_etl
from etl import get_neo4j_driver as get_driver

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/etl")  # checks uses GET; switch to @app.post if you prefer POST (then change checks)
def trigger_etl():
    run_etl()           # run synchronously so checks wait for completion
    return {"ok": True}



@app.get("/recs")
def recs(customer_id: Optional[str] = None, product_id: Optional[str] = None) -> Dict[str, List]:
    """Minimal recommendations endpoint using simple Cypher strategies.

    - If product_id is provided: try basket co-occurrence, then same-category.
    - If customer_id is provided: recommend co-occurring products not yet purchased.
    - Returns 200 with empty list if no signal or DB not available.

    TODO: Switch to Personalized PageRank (PPR) when GDS is present:
    - Use gds.graph.project / gds.pageRank to compute personalized scores for a given
      customer or product seed, with damping/teleport parameters, and return top-N.
    """
    started = time.time()
    items: List[Dict] = []

    driver = get_driver()
    if driver is None:
        return {"items": [], "took_ms": int((time.time() - started) * 1000)}
    try:
        driver.verify_connectivity()
    except Exception:
        return {"items": [], "took_ms": int((time.time() - started) * 1000)}


    try:
        with driver.session() as session:
            if product_id is not None:
                # Basket co-occurrence: customers/orders that also contain other products
                rows = session.run(
                    """
                    MATCH (p:Product {id: $pid})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(other:Product)
                    WHERE other.id <> $pid
                    RETURN other.id AS product_id, count(*) AS score
                    ORDER BY score DESC
                    LIMIT 10
                    """,
                    {"pid": product_id},
                ).data()
                if rows:
                    items = [
                        {
                            "product_id": r["product_id"],
                            "score": float(r["score"]),
                            "reason": "co-occurrence",
                        }
                        for r in rows
                    ]
                else:
                    # Same-category fallback
                    rows = session.run(
                        """
                        MATCH (p:Product {id: $pid})-[:IN_CATEGORY]->(c)<-[:IN_CATEGORY]-(other:Product)
                        WHERE other.id <> $pid
                        RETURN other.id AS product_id
                        LIMIT 10
                        """,
                        {"pid": product_id},
                    ).data()
                    items = [
                        {"product_id": r["product_id"], "score": 1.0, "reason": "same-category"}
                        for r in rows
                    ]

            elif customer_id is not None:
                # Recommend products co-occurring with customer's purchased products, excluding already purchased
                rows = session.run(
                    """
                    MATCH (c:Customer {id: $cid})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
                    MATCH (p)<-[:CONTAINS]-(:Order)-[:CONTAINS]->(other:Product)
                    WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(other)
                    RETURN other.id AS product_id, count(*) AS score
                    ORDER BY score DESC
                    LIMIT 10
                    """,
                    {"cid": customer_id},
                ).data()
                if rows:
                    items = [
                        {
                            "product_id": r["product_id"],
                            "score": float(r["score"]),
                            "reason": "co-occurrence",
                        }
                        for r in rows
                    ]
                else:
                    # Same-category fallback from user's purchased categories
                    rows = session.run(
                        """
                        MATCH (c:Customer {id: $cid})-[:PLACED]->(:Order)-[:CONTAINS]->(:Product)-[:IN_CATEGORY]->(cat)
                        MATCH (other:Product)-[:IN_CATEGORY]->(cat)
                        WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(other)
                        RETURN DISTINCT other.id AS product_id
                        LIMIT 10
                        """,
                        {"cid": customer_id},
                    ).data()
                    items = [
                        {"product_id": r["product_id"], "score": 1.0, "reason": "same-category"}
                        for r in rows
                    ]
            # else: neither provided -> return empty list
    except Exception:
        # Graceful degradation for demo purposes
        items = []
    finally:
        try:
            driver.close()
        except Exception:
            pass

    took_ms = int((time.time() - started) * 1000)
    return {"items": items, "took_ms": took_ms}


