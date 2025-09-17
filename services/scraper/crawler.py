# ./services/scraper/crawler.py
import asyncio, os, re, time, json, datetime as dt, sys, pathlib
from typing import List, Optional, Dict, Tuple
from urllib.parse import urlparse, urljoin

import httpx
from selectolax.parser import HTMLParser
from elasticsearch import Elasticsearch
from redis.asyncio import Redis

# --- import path for local packages (spiders, adapters)
BASE = pathlib.Path(__file__).parent
sys.path.append(str(BASE))
sys.path.append(str(BASE / "spiders"))
sys.path.append(str(BASE / "adapters"))

# -------------------- ENV --------------------
ES_URL        = os.getenv("ES_HOST", "http://es:9200")
ES_INDEX      = os.getenv("ES_INDEX", "games")

URLS_FILE     = os.getenv("SCRAPE_URLS_FILE", "").strip()
START_URLS    = [u.strip() for u in re.split(r"[;,]", os.getenv("SCRAPE_START_URLS", "")) if u.strip()]

CONCURRENCY   = int(os.getenv("CONCURRENCY", "3"))
DELAY_SEC     = float(os.getenv("DELAY_SEC", "3.0"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "200"))
MAX_APPS      = int(os.getenv("MAX_APPS", "1000"))
SAME_DOMAIN_ONLY   = os.getenv("SAME_DOMAIN_ONLY", "1") == "1"
FOLLOW_LIST_LINKS  = os.getenv("FOLLOW_LIST_LINKS", "1") == "1"  # اگر 0 شود فقط /app/ها را می‌گیرد

REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379/0")
FRONTIER_KEY  = os.getenv("FRONTIER_KEY", "frontier:queue")
SEEN_KEY      = os.getenv("SEEN_KEY", "frontier:seen")
PAGES_COUNT   = os.getenv("PAGES_COUNT", "frontier:pages_count")
APPS_COUNT    = os.getenv("APPS_COUNT", "frontier:apps_count")

# Auto-discover switches
USE_ADAPTERS        = os.getenv("USE_ADAPTERS", "1") == "1"

MYKET_AUTO_DISCOVER = os.getenv("MYKET_AUTO_DISCOVER", "0") == "1"
MYKET_GAMES_ROOT    = os.getenv("MYKET_GAMES_ROOT", "https://myket.ir/games")
MYKET_MAX_LISTS     = int(os.getenv("MYKET_MAX_LISTS", "300"))

BAZAAR_AUTO_DISCOVER = os.getenv("BAZAAR_AUTO_DISCOVER", "0") == "1"
# ریشه‌ای که از آن /cat/* را پیدا می‌کنیم (صفحه‌ی فهرست دسته‌ها)
BAZAAR_ROOT          = os.getenv("BAZAAR_ROOT", "https://cafebazaar.ir/pages/list~app-category~game-categories")
BAZAAR_MAX_LISTS     = int(os.getenv("BAZAAR_MAX_LISTS", "300"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
}

# -------------------- Clients --------------------
es = Elasticsearch(ES_URL)
rds: Redis

# -------------------- Helpers --------------------
APP_PAT = re.compile(r"/app/([A-Za-z0-9._-]+)")

def now_iso() -> str:
    return dt.datetime.utcnow().isoformat()

def domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

def is_app_url(url: str) -> bool:
    return bool(APP_PAT.search(url))

def normalize_url(base: str, href: str) -> str:
    if not href:
        return ""
    return urljoin(base, href)

def parse_html(html: str) -> HTMLParser:
    try:
        return HTMLParser(html)
    except Exception as e:
        # جلوگیری از کرش‌های نادر لکس‌بور/selectolax
        print("[HTML] parse error, length:", len(html), "err:", e)
        return HTMLParser("<html></html>")

def extract_json_ld(doc: HTMLParser) -> Dict:
    data: Dict = {}
    for node in doc.css("script[type='application/ld+json']"):
        try:
            payload = json.loads(node.text())
        except Exception:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            typ = it.get("@type") or it.get("@context", "")
            if isinstance(typ, str) and any(t in typ for t in ["SoftwareApplication", "MobileApplication", "VideoGame"]):
                data["title"] = data.get("title") or it.get("name")
                agg = it.get("aggregateRating") or {}
                if isinstance(agg, dict):
                    data["rating"] = data.get("rating") or agg.get("ratingValue")
                    data["ratings_count"] = data.get("ratings_count") or agg.get("ratingCount")
                data["description"] = data.get("description") or it.get("description")
    return data

def fallback_meta(doc: HTMLParser) -> Dict:
    t = (doc.css_first("meta[property='og:title']") or doc.css_first("title"))
    d = (doc.css_first("meta[name='description']") or doc.css_first("meta[property='og:description']"))
    title = (t.attributes.get("content") if t and hasattr(t, "attributes") and t.attributes.get("content") else (t.text() if t else "")).strip()
    desc  = (d.attributes.get("content") if d and d.attributes.get("content") else "").strip()
    return {"title": title, "description": desc}

def _num(s: Optional[str]) -> Optional[float]:
    if s is None: return None
    t = str(s).strip().replace("٬", "").replace(",", "")
    try: return float(t)
    except Exception: return None

def _int(s: Optional[str]) -> Optional[int]:
    if s is None: return None
    t = re.sub(r"\D", "", str(s))
    try: return int(t) if t else None
    except Exception: return None

def extract_fields_basic(html: str) -> Dict:
    doc = parse_html(html)
    data = extract_json_ld(doc)
    fb = fallback_meta(doc)
    rating = _num(data.get("rating"))
    rc = _int(data.get("ratings_count"))
    return {
        "title": (data.get("title") or fb.get("title") or "").strip(),
        "description": (data.get("description") or fb.get("description") or "").strip(),
        "rating": rating,
        "ratings_count": rc
    }

def try_import_adapters():
    baz = myk = None
    try:
        from adapters import bazaar as bazaar_adapter  # type: ignore
        baz = bazaar_adapter
    except Exception:
        pass
    try:
        from adapters import myket as myket_adapter    # type: ignore
        myk = myket_adapter
    except Exception:
        pass
    return baz, myk

BAZAAR_ADAPTER, MYKET_ADAPTER = try_import_adapters()

def enrich_with_adapter(url: str, html: str, base_fields: Dict) -> Dict:
    if not USE_ADAPTERS:
        return base_fields
    extra: Dict = {}
    try:
        if "cafebazaar.ir" in url and BAZAAR_ADAPTER:
            extra = BAZAAR_ADAPTER.parse(html) or {}
        elif "myket.ir" in url and MYKET_ADAPTER:
            extra = MYKET_ADAPTER.parse(html) or {}
    except Exception as e:
        print("[ADAPTER] error:", e)
        extra = {}
    for k, v in extra.items():
        if v not in (None, "", []):
            base_fields[k] = v
    return base_fields

def to_game_doc(url: str, fields: Dict) -> Dict:
    net = domain(url)
    store = "bazaar" if "cafebazaar.ir" in net else ("myket" if "myket.ir" in net else "unknown")
    m = APP_PAT.search(url)
    app_id = m.group(1) if m else f"manual-{int(time.time())}"
    return {
        "store": store,
        "app_id": app_id,
        "title": fields.get("title", ""),
        "genre": fields.get("genre", "unknown"),
        "rating": fields.get("rating"),
        "ratings_count": fields.get("ratings_count"),
        "installs": fields.get("installs"),
        "monetization": fields.get("monetization", "unknown"),
        "description": fields.get("description", ""),
        "feature_flags": fields.get("feature_flags", []),
        "released_at": fields.get("released_at"),
        "updated_at": fields.get("updated_at") or now_iso(),
        "indexed_at": now_iso(),   # مشخص می‌کند این نوبت کی ایندکس شده
    }

def is_relevant_list_url(url: str) -> bool:
    if "/video/" in url:
        return False
    patterns = [
        # Bazaar lists/categories:
        "cafebazaar.ir/cat/", "cafebazaar.ir/category/game", "list~app-category~game",
        # Myket lists/categories:
        "myket.ir/cat", "myket.ir/list", "myket.ir/games", "myket.ir/apps", "myket.ir/search",
        # homes (برای بذر)
        "cafebazaar.ir/", "myket.ir/"
    ]
    return any(p in url for p in patterns)

def infer_genre_from_url(u: str) -> Optional[str]:
    u = u.lower()
    # Bazaar: /cat/<slug>
    if "cafebazaar.ir/cat/" in u:
        slug = u.split("cafebazaar.ir/cat/")[-1].split("?")[0].split("/")[0]
        mapping = {
            "strategy":"strategy", "action":"action", "arcade":"arcade", "casual":"casual",
            "racing":"racing", "simulation":"simulation", "word-trivia":"word_trivia",
            "kids-games":"kids", "puzzle":"puzzle", "sports-game":"sports"
        }
        return mapping.get(slug, slug or None)

    # Myket: /games/<slug>
    if "myket.ir/games/" in u:
        slug = u.split("myket.ir/games/")[-1].split("?")[0].split("/")[0]
        mapping = {
            "action":"action","adventure":"adventure","casual":"casual","kids":"kids",
            "puzzle":"puzzle","racing":"racing","simulation":"simulation","sports":"sports",
            "strategy":"strategy","word":"word"
        }
        return mapping.get(slug, slug or None)

    return None


def extract_links(base_url: str, html: str) -> Tuple[List[Tuple[str, Optional[str]]], List[str]]:
    doc = parse_html(html)
    app_links: List[Tuple[str, Optional[str]]] = []
    list_links: List[str] = []

    # ژانرِ صفحه‌ی فعلی (اگر صفحه لیست باشد)
    page_genre_hint = infer_genre_from_url(base_url)

    for a in doc.css("a"):
        href = a.attributes.get("href") if a.attributes else None
        if not href:
            continue
        url = normalize_url(base_url, href)
        if not url.startswith("http"):
            continue
        if SAME_DOMAIN_ONLY and domain(url) != domain(base_url):
            continue
        if not (("cafebazaar.ir" in url) or ("myket.ir" in url)):
            continue

        if is_app_url(url):
            app_links.append((url, page_genre_hint))  # <<— همراه hint
        elif is_relevant_list_url(url):
            list_links.append(url)

    # dedup while keeping order
    seen_apps=set(); apps_dedup=[]
    for u,h in app_links:
        if u in seen_apps: 
            continue
        seen_apps.add(u); apps_dedup.append((u,h))

    list_links = list(dict.fromkeys(list_links))
    if not FOLLOW_LIST_LINKS:
        return apps_dedup, []
    return apps_dedup, list_links

async def fetch(url: str, client: httpx.AsyncClient) -> str:
    r = await client.get(url)
    r.raise_for_status()
    return r.text

async def index_app(url: str, html: str, genre_hint: Optional[str] = None, source_list: Optional[str] = None) -> bool:
    fields = extract_fields_basic(html)
    if "خطا" in (fields.get("title", "")):
        print(f"[IDX] WARN skip error page: {url}")
        return False

    fields = enrich_with_adapter(url, html, fields)

    # اگر ژانر تهی/unknown بود، از hint استفاده کن
    g = fields.get("genre")
    if not g or g == "unknown":
        if genre_hint:
            fields["genre"] = genre_hint

    doc = to_game_doc(url, fields)

    # منبع صفحهٔ لیست را ذخیره کن (به درد دیباگ/تحلیل می‌خورد)
    if source_list:
        doc["source_list_url"] = source_list

    try:
        if not es.indices.exists(index=ES_INDEX):
            es.indices.create(index=ES_INDEX)
    except Exception as e:
        print("WARN ensure index:", e)

    doc_id = f"{doc['store']}::{doc['app_id']}"
    es.index(index=ES_INDEX, id=doc_id, document=doc)
    return True


# -------------------- Frontier (Redis) --------------------
async def frontier_init(seed_urls: List[str]):
    q_len = await rds.llen(FRONTIER_KEY)
    if q_len == 0 and seed_urls:
        await rds.rpush(FRONTIER_KEY, *seed_urls)

async def enqueue(url: str, front: bool = False, genre_hint: Optional[str] = None, source_list: Optional[str] = None):
    added = await rds.sadd(SEEN_KEY, url)  # فقط URL را برای dedup چک می‌کنیم
    if added == 1:
        payload = json.dumps({"url": url, "genre_hint": genre_hint, "source_list": source_list or ""})
        if front:
            await rds.lpush(FRONTIER_KEY, payload)
        else:
            await rds.rpush(FRONTIER_KEY, payload)

async def worker(name: str):
    pages_cnt = int((await rds.get(PAGES_COUNT)) or 0)
    apps_cnt  = int((await rds.get(APPS_COUNT)) or 0)

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        while True:
            if MAX_APPS > 0 and apps_cnt >= MAX_APPS:
                break
            if MAX_PAGES > 0 and pages_cnt >= MAX_PAGES:
                break

            raw = await rds.lpop(FRONTIER_KEY)
            if not raw:
                await asyncio.sleep(0.5)
                continue

            # --- این قسمت جدید است: payload صف را parse می‌کنیم (backward-compatible)
            url = raw
            genre_hint = None
            source_list = None
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and "url" in obj:
                    url = obj["url"]
                    genre_hint = obj.get("genre_hint")
                    source_list = obj.get("source_list")
            except Exception:
                # اگر قدیمی بود و فقط URL بود، همین url کافی است
                pass
            # --- پایان بخش جدید

            try:
                html = await fetch(url, client)
            except Exception as e:
                print(f"[{name}] ERROR fetch {url}: {e}")
                await asyncio.sleep(DELAY_SEC)
                continue

            if is_app_url(url):
                ok = await index_app(url, html, genre_hint=genre_hint, source_list=source_list)
                if ok:
                    apps_cnt += 1
                    await rds.set(APPS_COUNT, apps_cnt)
                    print(f"[{name}] Indexed app ({apps_cnt}/{MAX_APPS}): {url}")
            else:
                # توجه: extract_links حالا (app_links, list_links) می‌دهد که
                # app_links = List[Tuple[url, genre_hint_from_this_page]]
                app_links, list_links = extract_links(url, html)

                # اپ‌ها جلو صف + ژانر صفحه فعلی را به‌عنوان hint پاس بده
                # اگر extract_links برای این لینک gh نداد، از خود URL صفحه فعلی حدس بزنیم
                for link, gh in app_links:
                    await enqueue(link, front=True,
                                  genre_hint=(gh or infer_genre_from_url(url)),
                                  source_list=url)

                # لیست‌ها انتهای صف
                for link in list_links:
                    await enqueue(link, front=False)

                pages_cnt += 1
                await rds.set(PAGES_COUNT, pages_cnt)
                print(f"[{name}] Scanned page ({pages_cnt}/{MAX_PAGES}): {url}  +apps:{len(app_links)} +lists:{len(list_links)}")

            await asyncio.sleep(DELAY_SEC)

# -------------------- Bootstrap (auto-discover) --------------------
def discover_myket(games_root: str, limit_lists: int) -> List[str]:
    from spiders.myket_discover import discover_from_games_root
    return discover_from_games_root(games_root, limit_lists)

def discover_bazaar(root: str, limit_lists: int) -> List[str]:
    from spiders.bazaar_discover import discover_from_bazaar_root
    return discover_from_bazaar_root(root, limit_lists)

async def bootstrap_urls() -> List[str]:
    urls: List[str] = []

    # 0) URLها از فایل (اگر داده‌ای)
    if URLS_FILE and os.path.exists(URLS_FILE):
        try:
            with open(URLS_FILE, "r", encoding="utf-8-sig") as f:
                for ln in f:
                    s = ln.lstrip("\ufeff").strip()
                    if s and not s.startswith("#"):
                        urls.append(s)
        except Exception as e:
            print("[BOOT] read file error:", e)

    # 1) از ENV
    if START_URLS:
        urls += START_URLS

    # 2) autodiscover Myket
    if MYKET_AUTO_DISCOVER:
        print(f"[BOOT] Myket auto-discover from {MYKET_GAMES_ROOT} (limit={MYKET_MAX_LISTS})")
        try:
            u = discover_myket(MYKET_GAMES_ROOT, MYKET_MAX_LISTS)
            if not u:
                print("[BOOT] myket discover returned 0; fallback to root")
                u = [MYKET_GAMES_ROOT]
            print(f"[BOOT] discovered {len(u)} myket list pages")
            urls += u
        except Exception as e:
            print("[BOOT] myket discover error:", e)
            urls += [MYKET_GAMES_ROOT]

    # 3) autodiscover Bazaar (/cat/* از صفحه‌ی game-categories)
    if BAZAAR_AUTO_DISCOVER:
        print(f"[BOOT] Bazaar auto-discover from {BAZAAR_ROOT} (limit={BAZAAR_MAX_LISTS})")
        try:
            b = discover_bazaar(BAZAAR_ROOT, BAZAAR_MAX_LISTS)
            if not b:
                print("[BOOT] bazaar discover returned 0; fallback to root")
                b = [BAZAAR_ROOT]
            print(f"[BOOT] discovered {len(b)} bazaar list pages")
            urls += b
        except Exception as e:
            print("[BOOT] bazaar discover error:", e)
            urls += [BAZAAR_ROOT]

    # dedup با حفظ ترتیب
    deduped: List[str] = []
    seen = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped

# -------------------- Main --------------------
async def main():
    global rds
    rds = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        seeds = await bootstrap_urls()
        if not seeds:
            print("No seeds provided (SCRAPE_START_URLS or SCRAPE_URLS_FILE or auto-discover).")
            return

        await frontier_init(seeds)
        for s in seeds:
            await rds.sadd(SEEN_KEY, s)

        tasks = [asyncio.create_task(worker(f"W{i+1}")) for i in range(CONCURRENCY)]
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Done.")
    finally:
        try:
            await rds.aclose()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())
