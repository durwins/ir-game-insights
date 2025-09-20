# services/scraper/miner.py
import os, re, json, math, datetime as dt
from typing import Dict, List, Any, Iterable, Optional, Tuple
from elasticsearch import Elasticsearch, helpers

ES_URL        = os.getenv("ES_HOST", "http://es:9200").rstrip("/")
ES_INDEX      = os.getenv("ES_INDEX", "games")
ASSETS_INDEX  = os.getenv("ES_ASSETS_INDEX", "assets")
BATCH_SIZE    = int(os.getenv("MINER_BATCH", "500"))
MAX_DOCS      = int(os.getenv("MINER_MAX_DOCS", "0"))  # 0 = no limit
STORE_FILTER  = os.getenv("MINER_STORE", "").strip()   # e.g. "myket" | "bazaar" | ""

# از options برای حذف DeprecationWarning
es = Elasticsearch(ES_URL).options(request_timeout=60)

# ---------- Keyword dictionaries ----------
DEFAULT_DICT = {
    "features": {
        "offline":  ["آفلاین","بدون اینترنت","offline","no internet","single player","singleplayer"],
        "online":   ["آنلاین","مولتی","چند نفره","multiplayer","pvp","co-op","clan","guild","clans","leaderboard","لیگ","رنک"],
        "pvp":      ["pvp","player vs player","بازیکن مقابل بازیکن","نبرد آنلاین"],
        "pve":      ["pve","campaign","مرحله‌ای","داستانی","stage","level","levels","ماموریت"],
        "simple_controls": ["کنترل ساده","یک دستی","one hand","tap","tapping","tapper","swipe","اسکِل"],
        "progression": ["level","levels","مرحله","چالش روزانه","daily challenge","quest","ماموریت","پروgression","season","فصل","event","رویداد"],
        "rewarded_ads": ["تماشای ویدئو","دیدن ویدیو","rewarded ad","reward video","ویدئوی جایزه"],
        "interstitial_ads": ["تبلیغ تمام صفحه","interstitial","میان‌برنامه"],
        "iap":      ["خرید داخل برنامه","in-app purchase","iap","gem","coin","shop","store","season pass","battle pass","بتل پس"],
        "cosmetics": ["skin","اسکین","ظاهر","کاستوم","لباس","avatar","emote"],
        "gacha":    ["gacha","چِست","loot box","صندوق","کپسول"],
        "social":   ["دوستان","friend","invite","دعوت","share","اشتراک گذاری","chat","چت"],
        "localization_ir": ["ایرانی","ایران","تهران","فارسی","پرچم ایران","نوروز","چهارشنبه سوری","محرم","فوتبال ایران"],
        "sports":   ["football","soccer","basketball","volleyball","ورزشی","فوتبال","بسکتبال","والیبال"],
        "racing":   ["racing","رانندگی","مسابقه","drift","race","car","ماشین"],
        "puzzle":   ["puzzle","پازل","معمایی","match-3","match3","word","کلمات","کلمه"],
        "hyper_casual": ["runner","endless","tap to play","آسان برای یادگیری","سریع","مرحله‌های کوتاه","یک‌دستی"],
    },
    "marketing_terms": [
        "بهترین","رایگان","free","خفن","هیجان‌انگیز","epic","legendary","no.1","top","برترین","جدید","آپدیت بزرگ","رویداد ویژه"
    ],
    "topics": [
        "زامبی","zombie","استراتژی","strategy","tower defense","td","idle","آیدل",
        "shooting","شوتر","farm","کشاورزی","بقا","survival","sandbox","سندباکس"
    ]
}

def load_dict() -> Dict[str, Any]:
    path = os.path.join(os.path.dirname(__file__), "feature_dict.yml")
    if os.path.exists(path):
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                # merge کمینه با DEFAULT_DICT
                out = DEFAULT_DICT.copy()
                out.update(data)
                return out
        except Exception:
            pass
    return DEFAULT_DICT

DICT = load_dict()

# --------- helpers
def _norm_txt(s: Optional[str]) -> str:
    if not s: return ""
    return s.replace("‌"," ").replace("\u200c"," ").lower()

def find_any(text: str, words: Iterable[str]) -> bool:
    for w in words:
        if w and w.lower() in text:
            return True
    return False

def collect_flags(title: str, desc: str) -> List[str]:
    txt = _norm_txt(title) + " \n " + _norm_txt(desc)
    flags = []
    for key, words in DICT.get("features", {}).items():
        if find_any(txt, words):
            flags.append(key)
    return list(dict.fromkeys(flags))

def collect_terms(title: str, desc: str, key: str) -> List[str]:
    txt = _norm_txt(title) + " \n " + _norm_txt(desc)
    hits = [w for w in DICT.get(key, []) if w and w.lower() in txt]
    return list(dict.fromkeys(hits))

def success_score(doc: Dict[str, Any]) -> float:
    r   = float(doc.get("rating") or 0.0)
    rc  = float(doc.get("ratings_count") or 0.0)
    inst= float(doc.get("installs") or 0.0)
    base = r * math.log1p(rc)
    if inst > 0:
        base *= (1.0 + min(math.log10(inst + 1.0) / 6.0, 0.5))
    return round(base, 4)

# ---------- batch assets aggregation (سریع و مقیاس‌پذیر) ----------
def build_assets_counts_map() -> Dict[Tuple[str, str], Dict[str, int]]:
    """
    خروجی: {(store, app_id): {"icons": x, "shots": y}}
    """
    result: Dict[Tuple[str, str], Dict[str, int]] = {}
    after_key = None

    base_query: Dict[str, Any] = {"match_all": {}}
    if STORE_FILTER:
        base_query = {"term": {"store": STORE_FILTER}}

    while True:
        body = {
            "size": 0,
            "query": base_query,
            "aggs": {
                "by_app": {
                    "composite": {
                        "size": 2000,
                        "sources": [
                            {"store":  {"terms": {"field": "store"}}},
                            {"app_id": {"terms": {"field": "app_id"}}}
                        ],
                        **({"after": after_key} if after_key else {})
                    },
                    "aggs": {
                        "icons": {"filter": {"term": {"type": "icon"}}},
                        "shots": {"filter": {"term": {"type": "screenshot"}}}
                    }
                }
            }
        }
        resp = es.search(index=ASSETS_INDEX, body=body)
        buckets = resp.get("aggregations", {}).get("by_app", {}).get("buckets", [])
        for b in buckets:
            store = b["key"]["store"]
            app   = b["key"]["app_id"]
            icons = int(b["icons"]["doc_count"])
            shots = int(b["shots"]["doc_count"])
            result[(store, app)] = {"icons": icons, "shots": shots}
        after_key = resp.get("aggregations", {}).get("by_app", {}).get("after_key")
        if not after_key:
            break
    return result

# ---------- scan games ----------
def scan_games() -> Iterable[Dict[str, Any]]:
    q = {"term": {"store": STORE_FILTER}} if STORE_FILTER else {"match_all": {}}
    for hit in helpers.scan(
        es, index=ES_INDEX, query={"query": q, "_source": True}, size=1000, preserve_order=False
    ):
        yield hit

def build_updates(docs: Iterable[Dict[str, Any]],
                  assets_map: Dict[Tuple[str, str], Dict[str, int]]) -> Iterable[Dict[str, Any]]:
    n = 0
    for h in docs:
        src = h.get("_source", {})
        app_id = src.get("app_id"); store = src.get("store")
        if not app_id or not store:
            continue

        flags  = collect_flags(src.get("title",""), src.get("description",""))
        terms  = collect_terms(src.get("title",""), src.get("description",""), "marketing_terms")
        topics = collect_terms(src.get("title",""), src.get("description",""), "topics")
        counts = assets_map.get((store, app_id), {"icons": 0, "shots": 0})
        sscore = success_score(src)

        # feature_flags موجود را با پرچم‌های جدید merge کن
        current_flags = src.get("feature_flags") or []
        merged_flags = sorted(list({*current_flags, *flags}))

        doc = {
            "feature_flags": merged_flags,
            "desc_marketing_terms": terms,
            "desc_topics": topics,
            "assets_icon_count": counts["icons"],
            "assets_screenshot_count": counts["shots"],
            # برای سازگاری با ویژوال‌های قدیمی:
            "screenshot_count": counts["shots"],
            "success_score": sscore,
            "features_indexed_at": dt.datetime.utcnow().isoformat(timespec="seconds"),
        }

        yield {
            "_op_type": "update",
            "_index": ES_INDEX,
            "_id": h["_id"],
            "doc": doc,
            "doc_as_upsert": True,
        }

        n += 1
        if MAX_DOCS and n >= MAX_DOCS:
            break

def main():
    assets_map = build_assets_counts_map()
    updates = build_updates(scan_games(), assets_map)
    ok, fail = helpers.bulk(
        es, updates, raise_on_error=False, request_timeout=120, chunk_size=BATCH_SIZE
    )
    print(f"[MINER] bulk ok={ok}, fail={len(fail) if isinstance(fail, list) else 0}")

if __name__ == "__main__":
    import math  # بعد از import بالایی استفاده شد
    main()
