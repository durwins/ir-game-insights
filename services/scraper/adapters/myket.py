# ./services/scraper/adapters/myket.py
import re
from typing import Optional, Dict

META_TAG_RE = re.compile(
    r'<meta\s+[^>]*(?:name|property)=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\'][^>]*>',
    re.IGNORECASE
)
TIME_TAG_RE = re.compile(r'<time[^>]*datetime=["\']([^"\']+)["\']', re.IGNORECASE)
TEXT_TAG_RE = re.compile(r'<([a-z0-9]+)[^>]*>(.*?)</\1>', re.IGNORECASE | re.DOTALL)

def _meta(html: str) -> Dict[str, str]:
    out = {}
    for k, v in META_TAG_RE.findall(html):
        out[k.lower()] = v.strip()
    return out

def _first_group(pattern: re.Pattern, html: str) -> Optional[str]:
    m = pattern.search(html)
    return m.group(1).strip() if m else None

def _to_float(s: Optional[str]) -> Optional[float]:
    if not s: return None
    t = s.replace('٬','').replace(',','.').strip()
    try: return float(t)
    except: return None

def _to_int(s: Optional[str]) -> Optional[int]:
    if not s: return None
    import re as _re
    t = _re.sub(r'\D','', s)
    return int(t) if t else None

def _parse_installs(s: Optional[str]) -> Optional[int]:
    if not s: return None
    t = s.lower().replace('٬','').replace(',','').strip()
    mult = 1
    if 'm' in t or 'میلیون' in t: mult = 1_000_000
    elif 'k' in t or 'هزار' in t: mult = 1_000
    nums = ''.join(ch for ch in t if ch.isdigit() or ch=='.')
    try: return int(float(nums)*mult) if nums else None
    except: return None

def parse(html: str) -> dict:
    meta = _meta(html)

    title = meta.get('og:title') or meta.get('title') or ''
    desc  = meta.get('description') or meta.get('og:description') or ''

    # rating / count (schema.org or hints)
    rating = _to_float(meta.get('ratingvalue') or meta.get('myket:rating') or '')
    ratings_count = _to_int(meta.get('ratingcount') or meta.get('myket:ratingcount') or '')

    # installs: گاهی داخل متن صفحه/متاهاست؛ با چند الگو امتحان کن
    installs = None
    for key in ('myket:installs','app:installs','installs'):
        installs = installs or _parse_installs(meta.get(key))
    if installs is None:
        m = re.search(r'(?:نصب|install)[^0-9]{0,10}([0-9٬,\.kKmM]+)', html, re.IGNORECASE)
        installs = _parse_installs(m.group(1)) if m else None

    # genre: معمولا از breadcrumbs یا لینک‌های دسته
    genre = None
    gm = re.search(r'href="https://myket\.ir/games/([a-z0-9\-_/]+)"', html, re.IGNORECASE)
    if gm:
        genre = gm.group(1).split('/')[0]
    genre = (genre or 'unknown').strip()

    # updated_at: time[datetime] اگر باشد
    updated_at = _first_group(TIME_TAG_RE, html)

    # feature flags ساده
    flags = []
    dl = desc.lower()
    if ('آفلاین' in desc) or ('offline' in dl): flags.append('offline')
    if ('جدول برتر' in desc) or ('leaderboard' in dl): flags.append('leaderboard')

    # monetization: تخمینی
    monet = 'unknown'
    price = meta.get('price') or ''
    if price:
        monet = 'free' if price == '0' else 'paid'

    return {
        "title": title,
        "description": desc,
        "rating": rating,
        "ratings_count": ratings_count,
        "installs": installs,
        "genre": genre,
        "updated_at": updated_at,
        "monetization": monet,
        "feature_flags": flags,
    }
