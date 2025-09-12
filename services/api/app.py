# ./services/api/app.py
from fastapi import FastAPI, Query
from typing import List
from elasticsearch import Elasticsearch

app = FastAPI(title="IR Game Insights API")

ES_HOST = "http://es:9200"
es = Elasticsearch(ES_HOST)

@app.get("/health")
def health():
    try:
        return {"ok": es.ping(), "es": ES_HOST}
    except Exception as e:
        return {"ok": False, "error": str(e), "es": ES_HOST}

@app.get("/search")
def search_games(q: str = Query(default="بازی")):
    body = {"query": {"multi_match": {"query": q, "fields": ["title^2","description"]}}}
    res = es.search(index="games", body=body)
    hits = [h["_source"] for h in res.get("hits", {}).get("hits", [])]
    return {"count": len(hits), "items": hits}

@app.get("/top-features")
def top_features(genre: str = "hyper-casual"):
    body = {
      "size": 0,
      "query": {"term": {"genre": genre}},
      "aggs": {
        "features": {
          "terms": {"field": "feature_flags", "size": 25},
          "aggs": {
            "avg_rating": {"avg": {"field": "rating"}},
            "p90_installs": {"percentiles": {"field": "installs", "percents":[90]}}
          }
        }
      }
    }
    res = es.search(index="games", body=body)
    buckets = res.get("aggregations", {}).get("features", {}).get("buckets", [])
    return [
        {
            "feature": b["key"],
            "avg_rating": b["avg_rating"]["value"],
            "p90_installs": b["p90_installs"]["values"].get("90.0")
        }
        for b in buckets
    ]
