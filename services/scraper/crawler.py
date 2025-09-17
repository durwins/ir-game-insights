# ./services/scraper/crawler.py
import asyncio, os, re, time, json, datetime as dt, sys, pathlib, hashlib
from typing import List, Optional, Dict, Tuple, Set
from urllib.parse import urlparse, urljoin

import httpx
from selectolax.parser import HTMLParser
from elasticsearch import Elasticsearch, helpers
from redis.asyncio import Redis

# --- import path for local packages (spiders, adapters)
BASE = pathlib.Path(__file__).parent
sys.path.append(str(BASE))
sys.path.append(str(BASE / "spiders"))
sys.path.append(str(BASE / "adapters"))
sys.path.append(str(BASE / "utils"))

# ==================== ENV ====================
ES_URL        = os.getenv("ES_HOST", "http://es:9200").rstrip("/")
ES_INDEX      = os.getenv("ES_INDEX", "games")

# reviews
ES_REVIEWS_INDEX    = os.getenv("ES_REVIEWS_INDEX", "reviews")
ENABLE_REVIEWS      = os.getenv("ENABLE_REVIEWS", "1") == "1"
REVIEWS_PER_APP     = int(os.getenv("REVIEWS_PER_APP", "50"))
ENABLE_AJAX_REVIEWS = os.getenv("ENABLE_AJAX_REVIEWS", "1") == "1"  # ⬅️ جدید

# assets (icons/screenshots)
ES_ASSETS_INDEX  = os.getenv("ES_ASSETS_INDEX", "assets")

URLS_FILE     = os.getenv("SCRAPE_URLS_FILE", "").strip()
START_URLS    = [u.strip() for u in re.split(r"[;,]", os.getenv("SCRAPE_START_URLS", "")) if u.strip()]

CONCURRENCY   = int(os.getenv("CONCURRENCY", "3"))
DELAY_SEC     = float(os.getenv("DELAY_SEC", "0.8"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "200"))
MAX_APPS      = int(os.getenv("MAX_APPS", "1000"))
SAME_DOMAIN_ONLY   = os.getenv("SAME_DOMAIN_ONLY", "1") == "1"
FOLLOW_LIST_LINKS  = os.getenv("FOLLOW_LIST_LINKS", "1") == "1"

REDIS_URL     = os.getenv("REDIS_URL", "redis://redis:6379/0")
FRONTIER_KEY  = os.getenv("FRONTIER_KEY", "frontier:queue")
SEEN_KEY      = os.getenv("SEEN_KEY", "frontier:seen")
PAGES_COUNT   = os.getenv("PAGES_COUNT", "frontier:pages_count")
APPS_COUNT    = os.getenv("APPS_COUNT", "frontier:apps_count")

# Auto-discover
USE_ADAPTERS        = os.getenv("USE_ADAPTERS", "1") == "1"

MYKET_AUTO_DISCOVER = os.getenv("MYKET_AUTO_DISCOVER", "0") == "1"
MYKET_GAMES_ROOT    = os.getenv("MYKET_GAMES_ROOT", "https://myket.ir/games")
MYKET_MAX_LISTS     = int(os.getenv("MYKET_MAX_LISTS", "300"))

BAZAAR_AUTO_DISCOVER = os.getenv("BAZAAR_AUTO_DISCOVER", "0") == "1"
BAZAAR_ROOT          = os.getenv("BAZAAR_ROOT", "https://cafebazaar.ir/pages/list~app-category~game-categories")
BAZAAR_MAX_LISTS     = int(os.getenv("BAZAAR_MAX_LISTS", "300"))

# HTTP/2 toggle (fallback auto)
HTTP2_ENABLED  = os.getenv("HTTP2", "0") == "1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ==================== Clients ====================
es = Elasticsearch(ES_URL, request_timeout=60)
rds: Redis  # set in main()

# ==================== Helpers ====================
APP_PAT = re.compile(r"/app/([A-Za-z0-9._-]+)")

# --- Genre normalization (FA -> slug) ---
GENRE_MAP_FA = {
    "اکشن": "action","ماجراجویی": "adventure","تفننی": "casual","رانندگی": "racing",
    "مسابقه‌ای": "racing","مسابقه اي": "racing","مسابقه ايی": "racing","پازل": "puzzle",
    "معمایی": "puzzle","شبیه‌سازی": "simulation","شبیه سازی": "simulation","ورزشی": "sports",
    "کلمات": "word","کودکانه": "kids","آرکید": "arcade","استراتژی": "strategy",
    "شوتر": "action","تیراندازی": "action","رانندگي": "racing","هيجاني": "action",
}
CANON_GENRES = {"action","adventure","casual","racing","puzzle","simulation","sports","word","kids","arcade","strategy"}

def _norm_genre(txt: Optional[str]) -> Optional[str]:
    if not txt: return None
    t = re.sub(r"\s+", " ", str(txt)).strip().strip("/").strip()
    low = t.lower()
    if low in CANON_GENRES:
        return low
    key = t.replace("‌","")
    return GENRE_MAP_FA.get(t) or GENRE_MAP_FA.get(key) or None

def now_iso() -> str:
    return dt.datetime.utcnow().isoformat(timespec="seconds")

def domain(url: str) -> str:
    try: return urlparse(url).netloc
    except Exception: return ""

def is_app_url(url: str) -> bool:
    return bool(APP_PAT.search(url))

def normalize_url(base: str, href: str) -> str:
    if not href: return ""
    try:
        u = httpx.URL(base).join(href)
        if u.fragment: u = u.copy_with(fragment="")
        path = re.sub(r"/{2,}", "/", u.path)
        u = u.copy_with(path=path)
        return str(u)
    except Exception:
        return urljoin(base, href)

def parse_html(html: str) -> HTMLParser:
    try: return HTMLParser(html)
    except Exception as e:
        print("[HTML] parse error, length:", len(html), "err:", e)
        return HTMLParser("<html></html>")

def _type_hits(x) -> List[str]:
    if isinstance(x, str): return [x]
    if isinstance(x, list): return [str(t) for t in x if isinstance(t, str)]
    return []

def extract_json_ld(doc: HTMLParser) -> Dict:
    data: Dict = {}
    for node in doc.css("script[type='application/ld+json']"):
        raw = (node.text() or "").strip().rstrip(";")
        if not raw: continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            types = _type_hits(it.get("@type")) or _type_hits(it.get("@context"))
            if any(t in ("SoftwareApplication", "MobileApplication", "VideoGame") for t in types):
                data.setdefault("title", it.get("name"))
                agg = it.get("aggregateRating")
                if isinstance(agg, dict):
                    data.setdefault("rating", agg.get("ratingValue"))
                    data.setdefault("ratings_count", agg.get("ratingCount"))
                data.setdefault("description", it.get("description"))
                data.setdefault("updated_at", it.get("dateModified") or it.get("dateUpdated"))
                data.setdefault("released_at", it.get("datePublished"))
                for key in ("author", "publisher", "creator"):
                    val = it.get(key)
                    if isinstance(val, dict) and val.get("name"):
                        data.setdefault("developer", val.get("name")); break
                    if isinstance(val, str) and val.strip():
                        data.setdefault("developer", val.strip()); break
                for k in ("applicationCategory", "genre", "category"):
                    if not data.get("genre"):
                        gval = it.get(k)
                        if isinstance(gval, str) and gval.strip() == "GameApplication":
                            continue
                        data["genre"] = gval
    return data

def fallback_meta(doc: HTMLParser) -> Dict:
    t = doc.css_first("meta[property='og:title']") or doc.css_first("title")
    d = doc.css_first("meta[name='description']") or doc.css_first("meta[property='og:description']")
    title = (t.attributes.get("content") if t and t.attributes and t.attributes.get("content") else (t.text() if t else "")).strip()
    desc  = (d.attributes.get("content") if d and d.attributes and d.attributes.get("content") else "").strip()
    return {"title": title, "description": desc}

def _num(s: Optional[str]) -> Optional[float]:
    if s is None: return None
    t = str(s).strip().replace("٬", "").replace(",", "")
    try: return float(re.sub(r"[^\d.]", "", t))
    except Exception: return None

def _int(s: Optional[str]) -> Optional[int]:
    if s is None: return None
    t = re.sub(r"[^\d]", "", str(s))
    if not t: return None
    try: return int(t)
    except Exception: return None

def extract_fields_basic(html: str) -> Dict:
    doc = parse_html(html)
    data = extract_json_ld(doc)
    fb = fallback_meta(doc)
    rating = _num(data.get("rating"))
    rc = _int(data.get("ratings_count"))
    out = {
        "title": (data.get("title") or fb.get("title") or "").strip(),
        "description": (data.get("description") or fb.get("description") or "").strip(),
        "rating": rating,
        "ratings_count": rc,
        "updated_at": data.get("updated_at"),
        "released_at": data.get("released_at"),
        "developer": data.get("developer"),
        "genre": data.get("genre") or "unknown",
    }
    return out

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

def _call_adapter(mod, name: str, *args, **kwargs):
    if not mod or not hasattr(mod, name): return None
    fn = getattr(mod, name)
    try:
        # فرض: parserها sync هستند. اگر async بود، صرف‌نظر می‌کنیم (برای fetch_reviews مسیر async جدا داریم).
        if asyncio.iscoroutinefunction(fn):
            return None
        return fn(*args, **kwargs)
    except Exception as e:
        print(f"[ADAPTER] {name} error:", e)
        return None

def enrich_with_adapter(url: str, html: str, base_fields: Dict) -> Dict:
    if not USE_ADAPTERS:
        return base_fields
    extra: Dict = {}
    if "cafebazaar.ir" in url and BAZAAR_ADAPTER:
        extra = _call_adapter(BAZAAR_ADAPTER, "parse", url, html) or _call_adapter(BAZAAR_ADAPTER, "parse_bazaar", url, html) or {}
    elif "myket.ir" in url and MYKET_ADAPTER:
        extra = _call_adapter(MYKET_ADAPTER, "parse", url, html) or _call_adapter(MYKET_ADAPTER, "parse_myket", url, html) or {}
    for k, v in (extra or {}).items():
        if v not in (None, "", [], {}):
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
        "developer": fields.get("developer"),
        "monetization": fields.get("monetization", "unknown"),
        "description": fields.get("description", ""),
        "feature_flags": fields.get("feature_flags", []),
        "released_at": fields.get("released_at"),
        "updated_at": fields.get("updated_at") or now_iso(),
        "indexed_at": now_iso(),
        "source_url": url,
    }

LIST_HINTS = (
    "cafebazaar.ir/cat/","cafebazaar.ir/category/game","list~app-category~game","cafebazaar.ir/collection/",
    "myket.ir/cat","myket.ir/list","myket.ir/games","myket.ir/apps","myket.ir/search",
)

def is_relevant_list_url(url: str) -> bool:
    if "/video/" in url: return False
    return any(p in url for p in LIST_HINTS)

def infer_genre_from_url(u: str) -> Optional[str]:
    u = u.lower()
    if "cafebazaar.ir/cat/" in u:
        slug = u.split("cafebazaar.ir/cat/")[-1].split("?")[0].split("/")[0]
        mapping = {
            "strategy":"strategy","action":"action","arcade":"arcade","casual":"casual",
            "racing":"racing","simulation":"simulation","word-trivia":"word_trivia",
            "kids-games":"kids","puzzle":"puzzle","sports-game":"sports"
        }
        return mapping.get(slug, slug or None)
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
    page_genre_hint = infer_genre_from_url(base_url)

    for a in doc.css("a[href]"):
        href = a.attributes.get("href")
        if not href: continue
        url = normalize_url(base_url, href)
        if not url.startswith("http"): continue
        if SAME_DOMAIN_ONLY and domain(url) != domain(base_url): continue
        if not (("cafebazaar.ir" in url) or ("myket.ir" in url)): continue
        if is_app_url(url):
            app_links.append((url, page_genre_hint))
        elif is_relevant_list_url(url):
            list_links.append(url)

    if not app_links and not list_links:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
            url = normalize_url(base_url, m.group(1))
            if not url.startswith("http"): continue
            if SAME_DOMAIN_ONLY and domain(url) != domain(base_url): continue
            if not (("cafebazaar.ir" in url) or ("myket.ir" in url)): continue
            if is_app_url(url):
                app_links.append((url, page_genre_hint))
            elif is_relevant_list_url(url):
                list_links.append(url)

    seen_apps: Set[str] = set()
    apps_dedup: List[Tuple[str, Optional[str]]] = []
    for u, h in app_links:
        if u in seen_apps: continue
        seen_apps.add(u); apps_dedup.append((u, h))
    list_links = list(dict.fromkeys(list_links))

    return (apps_dedup, list_links if FOLLOW_LIST_LINKS else [])

# ==================== Reviews (HTML + optional AJAX via adapters) ====================
def _safe_txt(n):
    try: return (n.text() or "").strip()
    except Exception: return ""

def _attr(n, key):
    try: return (n.attributes.get(key) or "").strip()
    except Exception: return ""

def _make_review_id(store: str, app_id: str, r: Dict) -> str:
    base = f"{store}::{app_id}::{r.get('author') or ''}::{r.get('created_at') or ''}::{r.get('title') or ''}::{r.get('body') or ''}"
    return f"{store}::{app_id}::{hashlib.sha1(base.encode('utf-8')).hexdigest()[:20]}"

def parse_reviews_myket(app_url: str, html: str, limit: int) -> List[Dict]:
    out: List[Dict] = []
    doc = parse_html(html)
    for rv in doc.css('[itemprop="review"], .review-card, .user-comment, .comment'):
        author = None
        n = rv.css_first('[itemprop="author"] [itemprop="name"], .author, .username, .user')
        if n: author = _safe_txt(n)

        rating = None
        n = rv.css_first('[itemprop="ratingValue"], .rating, .user-rate')
        if n:
            try: rating = float(re.sub(r"[^\d.]", "", _safe_txt(n).replace("٬","").replace(",","")))
            except Exception: rating = None

        created_at = None
        n = rv.css_first('time[datetime], [itemprop="datePublished"], .date')
        if n: created_at = _attr(n, "datetime") or _safe_txt(n)

        title = None
        n = rv.css_first('.title, .comment-title')
        if n: title = _safe_txt(n)

        body = None
        n = rv.css_first('[itemprop="reviewBody"], .comment, .text, .content, .body')
        if n: body = _safe_txt(n)

        if not (author or body): continue
        out.append({"author":author,"rating":rating,"title":title,"body":body,"created_at":created_at})
        if len(out) >= limit: break
    return out

def parse_reviews_bazaar(app_url: str, html: str, limit: int) -> List[Dict]:
    out: List[Dict] = []
    doc = parse_html(html)
    for rv in doc.css('[itemprop="review"], .Comment, .CommentItem, .review'):
        author = None
        n = rv.css_first('[itemprop="author"] [itemprop="name"], .Comment__author, .username, .user')
        if n: author = _safe_txt(n)

        rating = None
        n = rv.css_first('[itemprop="ratingValue"], .Comment__rating, .rating')
        if n:
            try: rating = float(re.sub(r"[^\d.]", "", _safe_txt(n).replace("٬","").replace(",","")))
            except Exception: rating = None

        created_at = None
        n = rv.css_first('time[datetime], [itemprop="datePublished"], .date')
        if n: created_at = _attr(n, "datetime") or _safe_txt(n)

        title = None
        n = rv.css_first('.title, .Comment__title')
        if n: title = _safe_txt(n)

        body = None
        n = rv.css_first('[itemprop="reviewBody"], .Comment__text, .text, .content')
        if n: body = _safe_txt(n)

        if not (author or body): continue
        out.append({"author":author,"rating":rating,"title":title,"body":body,"created_at":created_at})
        if len(out) >= limit: break
    return out

def extract_reviews_for_page(url: str, html: str, limit: int) -> List[Dict]:
    if "myket.ir" in url:  return parse_reviews_myket(url, html, limit)
    if "cafebazaar.ir" in url:  return parse_reviews_bazaar(url, html, limit)
    return []

async def fetch_reviews_via_adapter(url: str, app_id: str, client: httpx.AsyncClient, limit: int) -> List[Dict]:
    mod = MYKET_ADAPTER if "myket.ir" in url else (BAZAAR_ADAPTER if "cafebazaar.ir" in url else None)
    if not (USE_ADAPTERS and mod and hasattr(mod, "fetch_reviews_ajax")) or limit <= 0:
        return []
    try:
        fn = getattr(mod, "fetch_reviews_ajax")
        if asyncio.iscoroutinefunction(fn):
            return await fn(url, app_id, client, limit) or []
        return fn(url, app_id, client, limit) or []
    except Exception as e:
        print("[ADAPTER] fetch_reviews_ajax error:", e)
        return []

def _dedup_reviews(arr: List[Dict]) -> List[Dict]:
    seen, out = set(), []
    for r in arr:
        key = f"{(r.get('author') or '').strip()}|{(r.get('created_at') or '').strip()}|{(r.get('title') or '').strip()}|{(r.get('body') or '').strip()}"
        h = hashlib.sha1(key.encode('utf-8')).hexdigest()[:20]
        if h in seen: 
            continue
        seen.add(h)
        out.append(r)
    return out

async def extract_reviews_extended(url: str, app_id: str, html: str, limit: int, client: httpx.AsyncClient) -> List[Dict]:
    # 1) HTML
    reviews = extract_reviews_for_page(url, html, limit)
    reviews = _dedup_reviews(reviews)
    if len(reviews) >= limit or not ENABLE_AJAX_REVIEWS:
        return reviews[:limit]

    # 2) AJAX via adapter
    try:
        extra = await fetch_reviews_via_adapter(url, app_id, client, limit - len(reviews))
        if extra:
            reviews = _dedup_reviews(reviews + extra)
    except Exception as e:
        print(f"[IDX] WARN ajax reviews for {url}: {e}")

    return reviews[:limit]

def bulk_index_reviews(app_url: str, app_title: str, app_id: str, store: str, reviews: List[Dict]) -> int:
    if not reviews: return 0
    ts = now_iso()
    actions = []
    for r in reviews:
        doc = {
            "store": store, "app_id": app_id, "app_title": app_title,
            "author": r.get("author"), "rating": r.get("rating"),
            "title": r.get("title"), "body": r.get("body"),
            "created_at": r.get("created_at"), "indexed_at": ts,
            "source_url": app_url,
        }
        rid = _make_review_id(store, app_id, r)
        actions.append({
            "_op_type": "update", "_index": ES_REVIEWS_INDEX, "_id": rid,
            "doc": doc, "doc_as_upsert": True,
        })
    ok, _ = helpers.bulk(es, actions, raise_on_error=False, request_timeout=60)
    return ok or 0

# ==================== Assets (icons & screenshots) ====================
def extract_image_urls(base_url: str, html: str) -> Dict[str, List[str]]:
    doc = parse_html(html)
    out = {"icon": [], "screenshots": []}

    og = doc.css_first('meta[property="og:image"]')
    if og and og.attributes.get("content"):
        out["icon"].append(normalize_url(base_url, og.attributes["content"]))

    sels = [
        '.screenshot img', '.screenshots img', '.gallery img', '.Gallery img',
        'img[data-src]', 'img[src]'
    ]
    for sel in sels:
        for n in doc.css(sel):
            src = (n.attributes.get("src") or n.attributes.get("data-src") or "").strip()
            if not src: continue
            u = normalize_url(base_url, src)
            if "/video/" in u: continue
            out["screenshots"].append(u)

    out["icon"] = list(dict.fromkeys(out["icon"]))
    out["screenshots"] = [u for i,u in enumerate(out["screenshots"]) if u not in out["screenshots"][:i]]
    return out

def _asset_id(store: str, app_id: str, typ: str, url: str) -> str:
    return f"{store}::{app_id}::{typ}::{hashlib.sha1(url.encode('utf-8')).hexdigest()[:16]}"

def bulk_index_assets(app_url: str, app_title: str, app_id: str, store: str, assets: Dict[str, List[str]]) -> int:
    ts = now_iso()
    actions = []
    for typ, urls in assets.items():
        for u in urls:
            doc = {
                "store": store, "app_id": app_id, "app_title": app_title,
                "type": "icon" if typ == "icon" else "screenshot",
                "url": u, "indexed_at": ts, "source_url": app_url,
            }
            actions.append({
                "_op_type": "update", "_index": ES_ASSETS_INDEX,
                "_id": _asset_id(store, app_id, doc["type"], u),
                "doc": doc, "doc_as_upsert": True,
            })
    if not actions: return 0
    ok, _ = helpers.bulk(es, actions, raise_on_error=False, request_timeout=60)
    return ok or 0

# ==================== Breadcrumb → Genre ====================
def genre_from_breadcrumbs_myket(html: str) -> Optional[str]:
    doc = parse_html(html)
    for node in doc.css("script[type='application/ld+json']"):
        raw = (node.text() or "").strip().rstrip(";")
        if not raw: continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict) and obj.get("@type") == "BreadcrumbList":
            items = obj.get("itemListElement") or []
            name = None
            for it in items:
                if isinstance(it, dict) and str(it.get("position")) == "2":
                    name = it.get("name") or (isinstance(it.get("item"), dict) and it["item"].get("name"))
                    break
            if not name and len(items) >= 2 and isinstance(items[1], dict):
                it = items[1]
                name = it.get("name") or (isinstance(it.get("item"), dict) and it["item"].get("name"))
            g = _norm_genre(name)
            if g: return g
    return None

def genre_from_breadcrumbs_bazaar(html: str) -> Optional[str]:
    doc = parse_html(html)
    ol = doc.css_first("ol.Breadcrumb__list")
    if not ol: return None
    lis = [li.text(strip=True) for li in ol.css("li")]
    if len(lis) >= 2:
        return _norm_genre(lis[1])
    return None

# ==================== Network ====================
async def fetch(url: str, client: httpx.AsyncClient, retries: int = 3) -> str:
    backoff = 1.0
    for i in range(retries + 1):
        try:
            r = await client.get(url)
            if r.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError("busy", request=r.request, response=r)
            r.raise_for_status()
            return r.text
        except Exception:
            if i >= retries: raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 8.0)

# ==================== Indexing ====================
def _store_from_url(u: str) -> str:
    n = domain(u)
    return ("bazaar" if "cafebazaar.ir" in n else ("myket" if "myket.ir" in n else "unknown"))

def _app_id_from_url(u: str) -> Optional[str]:
    m = APP_PAT.search(u)
    return m.group(1) if m else None

def _doc_id(u: str) -> str:
    return f"{_store_from_url(u)}::{_app_id_from_url(u) or 'unknown'}"

async def index_app(url: str, html: str, client: httpx.AsyncClient,  # ⬅️ client اضافه شد
                    genre_hint: Optional[str] = None, source_list: Optional[str] = None) -> bool:
    fields = extract_fields_basic(html)
    if "خطا" in (fields.get("title") or ""):
        print(f"[IDX] WARN skip error page: {url}")
        return False

    fields = enrich_with_adapter(url, html, fields)

    # genre enrichment/fallback
    if (not fields.get("genre")) or (fields.get("genre") in {"unknown", "GameApplication"}):
        g = None
        if "myket.ir" in url: g = genre_from_breadcrumbs_myket(html)
        elif "cafebazaar.ir" in url: g = genre_from_breadcrumbs_bazaar(html)
        if not g and genre_hint: g = _norm_genre(genre_hint)
        if not g: g = _norm_genre(infer_genre_from_url(url))
        if g: fields["genre"] = g

    doc = to_game_doc(url, fields)
    if source_list: doc["source_list_url"] = source_list

    # Upsert game
    body = {"doc": doc, "doc_as_upsert": True}
    try:
        es.update(index=ES_INDEX, id=_doc_id(url), body=body)
    except Exception:
        try:
            es.index(index=ES_INDEX, id=_doc_id(url), document=doc)
        except Exception as e2:
            print("[ES] index error:", e2)
            return False

    # reviews (HTML + optional AJAX via adapter) – استفاده از همان client
    if ENABLE_REVIEWS:
        try:
            app_id = _app_id_from_url(url) or doc["app_id"]
            store  = _store_from_url(url)
            reviews_html = extract_reviews_for_page(url, html, REVIEWS_PER_APP)
            extra_cnt = 0
            if len(reviews_html) < REVIEWS_PER_APP and ENABLE_AJAX_REVIEWS:
                reviews_all = await extract_reviews_extended(url, app_id, html, REVIEWS_PER_APP, client)
                extra_cnt = max(0, len(reviews_all) - len(reviews_html))
            else:
                reviews_all = reviews_html

            n_ok = bulk_index_reviews(url, doc["title"], app_id, store, reviews_all)
            if n_ok:
                print(f"[IDX] Reviews indexed: {n_ok} for {url} (ajax:{extra_cnt})")
        except Exception as e:
            print(f"[IDX] WARN reviews for {url}: {e}")

    # assets (icons & screenshots)
    try:
        imgs = extract_image_urls(url, html)
        n_assets = bulk_index_assets(url, doc["title"], doc["app_id"], doc["store"], imgs)
        if n_assets:
            print(f"[IDX] Assets indexed: {n_assets} for {url} (icon:{len(imgs.get('icon',[]))} shots:{len(imgs.get('screenshots',[]))})")
    except Exception as e:
        print(f"[IDX] WARN assets for {url}: {e}")

    return True

# ==================== Frontier (Redis) ====================
async def ensure_indices_once():
    try:
        if not es.indices.exists(index=ES_INDEX):
            es.indices.create(index=ES_INDEX)
        if ENABLE_REVIEWS and not es.indices.exists(index=ES_REVIEWS_INDEX):
            es.indices.create(index=ES_REVIEWS_INDEX)
        if ES_ASSETS_INDEX and not es.indices.exists(index=ES_ASSETS_INDEX):
            try: es.indices.create(index=ES_ASSETS_INDEX)
            except Exception: pass
    except Exception as e:
        print("[ES] ensure index warn:", e)

async def frontier_init(seed_urls: List[str]):
    await ensure_indices_once()
    q_len = await rds.llen(FRONTIER_KEY)
    if q_len == 0 and seed_urls:
        payloads = [json.dumps({"url": u, "genre_hint": infer_genre_from_url(u), "source_list": ""}) for u in seed_urls]
        if payloads: await rds.rpush(FRONTIER_KEY, *payloads)

async def enqueue(url: str, front: bool = False, genre_hint: Optional[str] = None, source_list: Optional[str] = None):
    added = await rds.sadd(SEEN_KEY, url)
    if added == 1:
        payload = json.dumps({"url": url, "genre_hint": genre_hint, "source_list": source_list or ""})
        if front: await rds.lpush(FRONTIER_KEY, payload)
        else:     await rds.rpush(FRONTIER_KEY, payload)

async def worker(name: str):
    pages_cnt = int((await rds.get(PAGES_COUNT)) or 0)
    apps_cnt  = int((await rds.get(APPS_COUNT)) or 0)

    try:
        client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=30, http2=HTTP2_ENABLED)
    except Exception:
        client = httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=30, http2=False)

    async with client:
        while True:
            if MAX_APPS > 0 and apps_cnt >= MAX_APPS:  break
            if MAX_PAGES > 0 and pages_cnt >= MAX_PAGES:  break

            raw = await rds.lpop(FRONTIER_KEY)
            if not raw:
                await asyncio.sleep(0.4); continue

            url = raw; genre_hint = None; source_list = None
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and "url" in obj:
                    url = obj["url"]; genre_hint = obj.get("genre_hint"); source_list = obj.get("source_list")
            except Exception:
                pass

            try:
                html = await fetch(url, client)
            except Exception as e:
                print(f"[{name}] ERROR fetch {url}: {e}")
                await asyncio.sleep(DELAY_SEC)
                continue

            if is_app_url(url):
                ok = await index_app(url, html, client, genre_hint=genre_hint, source_list=source_list)  # ⬅️ client
                if ok:
                    apps_cnt += 1
                    await rds.set(APPS_COUNT, apps_cnt)
                    print(f"[{name}] Indexed app ({apps_cnt}/{MAX_APPS}): {url}")
            else:
                app_links, list_links = extract_links(url, html)
                for link, gh in app_links:
                    await enqueue(link, front=True, genre_hint=(gh or infer_genre_from_url(url)), source_list=url)
                for link in list_links:
                    await enqueue(link, front=False)
                pages_cnt += 1
                await rds.set(PAGES_COUNT, pages_cnt)
                print(f"[{name}] Scanned page ({pages_cnt}/{MAX_PAGES}): {url}  +apps:{len(app_links)} +lists:{len(list_links)}")

            await asyncio.sleep(DELAY_SEC)

# ==================== Bootstrap (auto-discover) ====================
def discover_myket(games_root: str, limit_lists: int) -> List[str]:
    from spiders.myket_discover import discover_from_games_root
    return discover_from_games_root(games_root, limit_lists)

def discover_bazaar(root: str, limit_lists: int) -> List[str]:
    from spiders.bazaar_discover import discover_from_bazaar_root
    return discover_from_bazaar_root(root, limit_lists)

async def bootstrap_urls() -> List[str]:
    urls: List[str] = []

    if URLS_FILE and os.path.exists(URLS_FILE):
        try:
            with open(URLS_FILE, "r", encoding="utf-8-sig") as f:
                for ln in f:
                    s = ln.lstrip("\ufeff").strip()
                    if s and not s.startswith("#"):
                        urls.append(s)
        except Exception as e:
            print("[BOOT] read file error:", e)

    if START_URLS:
        urls += START_URLS

    if MYKET_AUTO_DISCOVER:
        print(f"[BOOT] Myket auto-discover from {MYKET_GAMES_ROOT} (limit={MYKET_MAX_LISTS})")
        try:
            u = discover_myket(MYKET_GAMES_ROOT, MYKET_MAX_LISTS)
            if not u: u = [MYKET_GAMES_ROOT]
            print(f"[BOOT] discovered {len(u)} myket list pages")
            urls += u
        except Exception as e:
            print("[BOOT] myket discover error:", e); urls += [MYKET_GAMES_ROOT]

    if BAZAAR_AUTO_DISCOVER:
        print(f"[BOOT] Bazaar auto-discover from {BAZAAR_ROOT} (limit={BAZAAR_MAX_LISTS})")
        try:
            b = discover_bazaar(BAZAAR_ROOT, BAZAAR_MAX_LISTS)
            if not b: b = [BAZAAR_ROOT]
            print(f"[BOOT] discovered {len(b)} bazaar list pages")
            urls += b
        except Exception as e:
            print("[BOOT] bazaar discover error:", e); urls += [BAZAAR_ROOT]

    deduped: List[str] = []
    seen = set()
    for u in urls:
        if u in seen: continue
        seen.add(u); deduped.append(u)
    return deduped

# ==================== Main ====================
async def main():
    global rds
    rds = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        seeds = await bootstrap_urls()
        if not seeds:
            print("No seeds provided (SCRAPE_START_URLS or SCRAPE_URLS_FILE or auto-discover).")
            return

        await frontier_init(seeds)
        if seeds:
            await rds.sadd(SEEN_KEY, *seeds)

        tasks = [asyncio.create_task(worker(f"W{i+1}")) for i in range(CONCURRENCY)]
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Done.")
    finally:
        try: await rds.aclose()
        except Exception: pass

if __name__ == "__main__":
    asyncio.run(main())
