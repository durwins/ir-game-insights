# ./services/scraper/crawler.py
import asyncio, os, re, time, json, datetime as dt
import httpx
from urllib.parse import urlparse
from selectolax.parser import HTMLParser
from elasticsearch import Elasticsearch

ES_URL = os.getenv("ES_HOST", "http://es:9200")
ES_INDEX = os.getenv("ES_INDEX", "games")
URLS_FILE = os.getenv("SCRAPE_URLS_FILE", "/app/urls.txt")
CONCURRENCY = int(os.getenv("CONCURRENCY", "2"))
DELAY_SEC = float(os.getenv("DELAY_SEC", "3.0"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7"
}

es = Elasticsearch(ES_URL)

def now_iso():
    return dt.datetime.utcnow().isoformat()

def read_urls(path: str):
    # BOM-safe read
    if not os.path.exists(path):
        print(f"No URL list at {path}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:  # ← utf-8-sig = removes BOM
        urls = []
        for ln in f:
            s = ln.lstrip("\ufeff").strip()  # ← اضافه، برای اطمینان
            if s and not s.startswith("#"):
                urls.append(s)
        return urls

def domain_to_store(netloc: str):
    if "cafebazaar.ir" in netloc:
        return "bazaar"
    if "myket.ir" in netloc:
        return "myket"
    return "unknown"

def url_to_app_id(url: str):
    # cafebazaar.ir/app/<package>
    # myket.ir/app/<package>
    m = re.search(r"/app/([A-Za-z0-9._-]+)", url)
    return m.group(1) if m else f"manual-{int(time.time())}"

def extract_json_ld(doc: HTMLParser):
    data = {}
    for node in doc.css('script[type="application/ld+json"]'):
        try:
            payload = json.loads(node.text())
        except Exception:
            continue
        # ممکن است آرایهای باشد
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            typ = it.get("@type") or it.get("@context","")
            # SoftwareApplication / MobileApplication / VideoGame
            if isinstance(typ, str) and any(t in typ for t in ["SoftwareApplication", "MobileApplication", "VideoGame"]):
                data["title"] = data.get("title") or it.get("name")
                agg = it.get("aggregateRating") or {}
                data["rating"] = data.get("rating") or (agg.get("ratingValue") if isinstance(agg, dict) else None)
                data["ratings_count"] = data.get("ratings_count") or (agg.get("ratingCount") if isinstance(agg, dict) else None)
                data["description"] = data.get("description") or it.get("description")
                # installs (aggregateRating گاهی count میشود)
    return data

def fallback_meta(doc: HTMLParser):
    title = (doc.css_first("meta[property='og:title']") or doc.css_first("title"))
    desc =  (doc.css_first("meta[name='description']") or doc.css_first("meta[property='og:description']"))
    return {
        "title": (title.attributes.get("content") if title and hasattr(title, "attributes") and title.attributes.get("content") else (title.text() if title else "")).strip(),
        "description": (desc.attributes.get("content") if desc and desc.attributes.get("content") else "").strip()
    }

def extract_fields(html: str):
    doc = HTMLParser(html)
    data = extract_json_ld(doc)
    fb = fallback_meta(doc)
    return {
        "title": (data.get("title") or fb.get("title") or "").strip(),
        "description": (data.get("description") or fb.get("description") or "").strip(),
        "rating": float(data["rating"]) if str(data.get("rating","")).replace(".","",1).isdigit() else None,
        "ratings_count": int(data["ratings_count"]) if str(data.get("ratings_count","")).isdigit() else None
    }

def to_game_doc(url: str, fields: dict):
    parsed = urlparse(url)
    store = domain_to_store(parsed.netloc)
    app_id = url_to_app_id(url)
    return {
        "store": store,
        "app_id": app_id,
        "title": fields.get("title",""),
        "genre": "unknown",
        "rating": fields.get("rating"),
        "ratings_count": fields.get("ratings_count"),
        "installs": None,
        "monetization": "unknown",
        "description": fields.get("description",""),
        "feature_flags": [],
        "released_at": None,
        "updated_at": now_iso()
    }

async def fetch(url: str, client: httpx.AsyncClient):
    r = await client.get(url)
    r.raise_for_status()
    return r.text

async def worker(name, queue: asyncio.Queue):
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        while True:
            url = await queue.get()
            try:
                html = await fetch(url, client)
                fields = extract_fields(html)
                game = to_game_doc(url, fields)
                es.index(index=ES_INDEX, document=game)
                print(f"[{name}] Indexed: {game['store']}/{game['app_id']}  title={game['title'][:40]}")
            except Exception as e:
                print(f"[{name}] ERROR {url}: {e}")
            finally:
                await asyncio.sleep(DELAY_SEC)
                queue.task_done()

async def main():
    urls = read_urls(URLS_FILE)
    if not urls:
        print("No URLs to scrape.")
        return
    queue = asyncio.Queue()
    for u in urls:
        queue.put_nowait(u)
    workers = [asyncio.create_task(worker(f"W{i+1}", queue)) for i in range(CONCURRENCY)]
    await queue.join()
    for w in workers:
        w.cancel()

if __name__ == "__main__":
    asyncio.run(main())
