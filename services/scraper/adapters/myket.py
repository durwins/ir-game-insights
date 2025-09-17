import re, json, html
from typing import Optional, Dict, Any, List
from html import unescape

JSONLD_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.I|re.S)
META_RE   = re.compile(r'<meta\s+[^>]*(?:name|property)=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\']', re.I)
IMG_RE    = re.compile(r'<img[^>]+(?:src|data-src)=["\']([^"\']+)["\'][^>]*>', re.I)
VIDEO_RE  = re.compile(r'<(?:video|source)[^>]+src=["\']([^"\']+)["\']', re.I)

def _jsonlds(text: str) -> List[dict]:
    out = []
    for m in JSONLD_RE.finditer(text):
        raw = unescape(m.group(1)).strip().rstrip(';')
        try:
            obj = json.loads(raw)
            out.extend(obj if isinstance(obj, list) else [obj])
        except:
            continue
    return out

def _first_app_ld(blocks: List[dict]) -> Optional[dict]:
    for it in blocks:
        tt = it.get("@type")
        types = [tt] if isinstance(tt, str) else (tt or [])
        if any(t in ("SoftwareApplication","MobileApplication","VideoGame") for t in types):
            return it
    return None

def _meta(text: str) -> Dict[str,str]:
    out = {}
    for k,v in META_RE.findall(text):
        out[k.lower()] = v.strip()
    return out

def _to_int_bytes(v) -> Optional[int]:
    if v is None: return None
    if isinstance(v, int): return v
    s = str(v).strip().replace('\u200c',' ').lower()
    m = re.match(r'([\d\.,]+)\s*([kmgt]?b)', s)
    if not m:
        num = re.sub(r'[^\d]', '', s)
        return int(num) if num else None
    num = float(m.group(1).replace(',',''))
    unit = m.group(2)
    mul = 1
    if unit == 'kb': mul = 1024
    elif unit == 'mb': mul = 1024**2
    elif unit == 'gb': mul = 1024**3
    elif unit == 'tb': mul = 1024**4
    return int(num * mul)

def _collect_screens(html_text: str) -> List[str]:
    out: List[str] = []
    for m in IMG_RE.finditer(html_text):
        u = m.group(1)
        if any(k in u for k in ("screenshot","shot","image.myket","/images/","/storage/","/screens/","/gallery/")):
            out.append(u)
    # JSON-LD
    for ld in _jsonlds(html_text):
        sc = ld.get("screenshot")
        if isinstance(sc, list):
            out.extend([s for s in sc if isinstance(s,str)])
        elif isinstance(sc, str):
            out.append(sc)
    # de-dup
    seen=set(); ret=[]
    for u in out:
        if u in seen: continue
        seen.add(u); ret.append(u)
        if len(ret) >= 20: break
    return ret

def parse_myket(page_url: str, html_text: str) -> Dict[str, Any]:
    # package از URL یا توی HTML
    m = re.search(r"/app/([A-Za-z0-9._-]+)", page_url) or re.search(r"package(Name)?=([A-Za-z0-9._-]+)", html_text)
    package = (m.group(1) if (m and m.lastindex == 1) else (m.group(2) if m else None)) or ""

    ld   = _first_app_ld(_jsonlds(html_text)) or {}
    meta = _meta(html_text)

    title   = (ld.get("name") or meta.get("og:title") or "").strip()
    desc    = (ld.get("description") or meta.get("og:description") or "").strip()
    genre   = ld.get("applicationCategory") or ld.get("genre") or ""
    if isinstance(genre, str) and genre.strip() == "GameApplication":
        genre = ""  # بگذار crawler ژانر را حدس بزند
    os_     = ld.get("operatingSystem") or "ANDROID"
    ver     = (ld.get("softwareVersion") or "").strip()
    fsize   = _to_int_bytes(ld.get("FileSize") or ld.get("fileSize"))
    img     = ld.get("image") or meta.get("og:image")
    url     = ld.get("url") or page_url
    dl      = ld.get("downloadUrl")
    install = ld.get("installUrl")
    pub     = ld.get("datePublished")
    mod     = ld.get("dateModified")

    agg     = ld.get("aggregateRating") or {}
    rating  = agg.get("ratingValue")
    rcount  = agg.get("ratingCount")

    # Screenshots / Videos
    screenshots = _collect_screens(html_text)
    videos = []
    for m in VIDEO_RE.finditer(html_text):
        videos.append(m.group(1))
    videos = list(dict.fromkeys(videos))[:5]

    # installs از meta اگر باشد
    installs = None
    for key in ('myket:installs','app:installs','installs'):
        if meta.get(key):
            txt = meta[key].replace('٬','').replace(',','')
            mult = 1
            if 'میلیون' in txt or 'm' in txt.lower(): mult = 1_000_000
            elif 'هزار' in txt or 'k' in txt.lower(): mult = 1_000
            try:
                num = float(re.sub(r'[^\d.]','', txt))
                installs = int(num * mult)
                break
            except:
                pass

    offers = ld.get("offers") or {}
    price  = offers.get("price")
    curr   = offers.get("priceCurrency")

    return {
        "store": "myket",
        "package": package,
        "title": title,
        "description": desc,
        "genre": genre or None,
        "operating_system": os_,
        "version": ver,
        "file_size_bytes": fsize,
        "image": img,
        "url": url,
        "download_url": dl,
        "install_url": install,
        "released_at": pub,
        "updated_at": mod,
        "rating_value": rating,
        "rating_count": rcount,
        "installs": installs,
        "price": price,
        "currency": curr,
        "screenshots": screenshots,
        "videos": videos,
        "raw": {"jsonld": ld}
    }

async def fetch_reviews_ajax(url: str, app_id: str, client, limit: int) -> List[Dict[str, Any]]:
    """
    تلاش برای استخراج نظرات بیشتر از stateهای درون صفحه (بدون وابستگی به API بیرونی).
    """
    try:
        r = await client.get(url)
        r.raise_for_status()
        text = r.text
    except Exception:
        return []

    out: List[Dict[str, Any]] = []

    # 1) JSON-LD review
    for block in _jsonlds(text):
        rev = block.get("review")
        revs = rev if isinstance(rev, list) else ([rev] if isinstance(rev, dict) else [])
        for rv in revs:
            if not isinstance(rv, dict): continue
            author = (rv.get("author") or {}).get("name") if isinstance(rv.get("author"), dict) else rv.get("author")
            rating = None
            rat = rv.get("reviewRating") or {}
            if isinstance(rat, dict):
                rating = rat.get("ratingValue")
            body   = rv.get("reviewBody") or rv.get("description")
            created= rv.get("datePublished") or rv.get("dateCreated")
            if body or author:
                out.append({"author":author, "rating":rating, "title":None, "body":body, "created_at":created})
            if len(out) >= limit: return out

    # 2) اسکن اسکریپت‌ها برای آبجکت‌های دارای reviews/comments
    for m in re.finditer(r'<script[^>]*>(.*?)</script>', text, re.I|re.S):
        blob = m.group(1)
        if not (("review" in blob) or ("comment" in blob)):
            continue
        for jm in re.finditer(r'(\{[^{}]{0,200}"(reviews|comments)"\s*:\s*\[[\s\S]{0,5000}?\][\s\S]{0,200}\})', blob):
            raw = jm.group(1).strip().rstrip(';')
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            arr = obj.get("reviews") or obj.get("comments") or []
            if isinstance(arr, list):
                for rv in arr:
                    if not isinstance(rv, dict): continue
                    author = (rv.get("authorName") or rv.get("author") or rv.get("user") or "")
                    rating = rv.get("rating") or (rv.get("rate") if isinstance(rv.get("rate"), (int,float)) else None)
                    body   = rv.get("text") or rv.get("body") or rv.get("content") or rv.get("comment")
                    created= rv.get("createdAt") or rv.get("date") or rv.get("time")
                    if body or author:
                        out.append({"author":author, "rating":rating, "title":rv.get("title"), "body":body, "created_at":created})
                    if len(out) >= limit: return out

    return out[:limit]

# سازگاری با crawler._call_adapter
def parse(url: str, html_text: str) -> Dict[str, Any]:
    return parse_myket(url, html_text)
