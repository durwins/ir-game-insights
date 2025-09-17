# services/scraper/scripts/backfill_genre.py
import os, re, sys, math, time
from elasticsearch import Elasticsearch, helpers

ES_URL   = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "games")
BATCH    = int(os.getenv("BATCH", "1000"))

def infer_from_url(u: str):
    if not u: return None
    u = u.lower()
    # myket
    m = re.search(r"myket\.ir/games/([^/?#]+)", u)
    if m:
        slug = m.group(1)
        map_ = {
            "action":"action","adventure":"adventure","casual":"casual","kids":"kids",
            "puzzle":"puzzle","racing":"racing","simulation":"simulation","sports":"sports",
            "strategy":"strategy","word":"word","board":"board"
        }
        return map_.get(slug, slug)
    # bazaar
    m = re.search(r"cafebazaar\.ir/cat/([^/?#]+)", u)
    if m:
        slug = m.group(1)
        map_ = {
            "strategy":"strategy","action":"action","arcade":"arcade","casual":"casual",
            "racing":"racing","simulation":"simulation","word-trivia":"word_trivia",
            "kids-games":"kids","puzzle":"puzzle","sports-game":"sports","board":"board"
        }
        return map_.get(slug, slug)
    return None

def gen_updates(es: Elasticsearch, q):
    # اسناد unknown با source_list_url یا حتی بدونش (fallback: nothing)
    for hit in helpers.scan(
        es,
        index=ES_INDEX,
        query=q,
        size=BATCH,
        _source=["genre","source_list_url","store"],
    ):
        src = hit.get("_source", {})
        gid = hit["_id"]
        if src.get("genre") and src["genre"] != "unknown":
            continue
        g = infer_from_url(src.get("source_list_url",""))
        if g:
            yield {
                "_op_type": "update",
                "_index": ES_INDEX,
                "_id": gid,
                "doc": {"genre": g}
            }

def main():
    es = Elasticsearch(ES_URL)
    q = {
        "query": {
            "bool": {
                "must": [{ "term": {"genre": "unknown"} }],
                "filter": [{ "exists": {"field": "source_list_url"} }]
            }
        }
    }
    cnt = 0
    for ok, info in helpers.streaming_bulk(es, gen_updates(es, q), chunk_size=500, max_retries=3):
        cnt += 1
        if not ok:
            print("ERR:", info, file=sys.stderr)
        if cnt % 1000 == 0:
            print(f"updated {cnt} docs...")
    print(f"done. updated {cnt} docs.")

if __name__ == "__main__":
    main()
