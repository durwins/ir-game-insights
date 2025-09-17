import re
from typing import List, Tuple, Set
import httpx
from selectolax.parser import HTMLParser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://cafebazaar.ir/",
}

# صفحه‌ی جزئیات اپ
APP_PAT = re.compile(r"/app/[A-Za-z0-9._-]+(?:\?.*)?$")

# صفحات فهرست/دسته/کلکسیون (در بازار معمول است)
LIST_HINTS = (
    "/cat/",            # دسته‌ها
    "/category/game",   # صفحات کلی بازی
    "/collection/",     # کالکشن‌های تماتیک
    "/list/",           # برخی لیست‌ها
)

def _is_bazaar(url: str) -> bool:
    return url.startswith("https://cafebazaar.ir/")

def _is_list_url(url: str, base_was_list: bool) -> bool:
    if any(h in url for h in LIST_HINTS):
        return True
    # اگر صفحه فعلی خودش لیست باشد، page= هم لیست محسوب می‌شود
    if base_was_list and "?page=" in url:
        return True
    return False

def _normalize(base: str, href: str) -> str:
    """Join + حذف fragment و نرمال‌سازی ساده‌ی کوئری‌های تکراری."""
    try:
        u = httpx.URL(base).join(href)
        # حذف fragment
        if u.fragment:
            u = u.copy_with(fragment="")
        # تبدیل // به / در مسیر (اگر وجود داشت)
        path = re.sub(r"/{2,}", "/", u.path)
        u = u.copy_with(path=path)
        return str(u)
    except Exception:
        return ""

def _uniq(seq: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _links_from(html: str, base: str) -> Tuple[List[str], List[str]]:
    """
    خروجی:
      apps  - لینک‌های صفحه جزئیات اپ‌ها (/app/…)
      lists - لینک‌های لیست/دسته/کلکسیون برای اسکن بیشتر
    """
    doc = HTMLParser(html)
    apps, lists = [], []

    base_is_list = any(h in base for h in LIST_HINTS)

    for a in doc.css("a[href]"):
        href = a.attributes.get("href")
        if not href:
            continue
        url = _normalize(base, href)
        if not url or not _is_bazaar(url):
            continue
        if "/video/" in url:
            continue

        if APP_PAT.search(url):
            apps.append(url)
        elif _is_list_url(url, base_is_list):
            lists.append(url)

    # Fallback regex (در صورت خراب بودن DOM)
    if not apps and not lists:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
            url = _normalize(base, m.group(1))
            if not url or not _is_bazaar(url):
                continue
            if APP_PAT.search(url):
                apps.append(url)
            elif _is_list_url(url, base_is_list):
                lists.append(url)

    return _uniq(apps), _uniq(lists)

def _get(client: httpx.Client, url: str, timeout: float, retries: int = 2) -> httpx.Response:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            r = client.get(url, headers=HEADERS, follow_redirects=True, timeout=timeout)
            # ری‌تری روی کدهای موقتی
            if r.status_code in (429, 500, 502, 503, 504):
                raise httpx.HTTPStatusError("server busy", request=r.request, response=r)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
    raise last_exc or RuntimeError("request failed")

def discover_from_bazaar_root(
    root: str,
    max_lists: int = 300,
    max_pages_per_cat: int = 50,
    per_request_timeout: float = 20.0,
) -> List[str]:
    """
    از یک صفحه ریشه (مثل صفحه لیست دسته‌ها) شروع می‌کند و تمام URLهای لیست/دسته را جمع می‌کند.
    - فقط URLهای «لیست/دسته» برگردانده می‌شود (نه اپ‌ها).
    - برای هر /cat/* تا max_pages_per_cat صفحه page=N پیشنهاد می‌شود، اما با سقف max_lists کنترل می‌شود.
    """
    if not _is_bazaar(root):
        raise ValueError("Root must be an https://cafebazaar.ir/ URL")

    seeds: List[str] = []
    seen: Set[str] = set()
    q: List[str] = [root]

    with httpx.Client(http2=True) as client:
        while q and len(seeds) < max_lists:
            url = q.pop(0)
            if url in seen:
                continue
            seen.add(url)

            try:
                r = _get(client, url, timeout=per_request_timeout)
            except Exception as e:
                print(f"[BAZAAR] fetch error {url}: {e}")
                continue

            # خود صفحه‌ی فعلی را به لیستِ لیست‌ها اضافه کن
            seeds.append(str(r.url))
            if len(seeds) >= max_lists:
                break

            apps, lists = _links_from(r.text, str(r.url))

            # برای هر /cat/*، صفحه‌بندی را (عاقلانه) اضافه کن
            for u in lists:
                if "/cat/" in u:
                    base_no_q = u.split("?", 1)[0]
                    # اگر همین u قبلاً page داشت، دوباره page نساز
                    has_page = "page=" in u
                    if not has_page:
                        for p in range(2, max_pages_per_cat + 1):
                            paged = f"{base_no_q}?page={p}"
                            if (paged not in seen) and (paged not in q) and (len(seeds) + len(q) < max_lists * 2):
                                q.append(paged)

            # بقیه لیست‌ها را در صف بگذار
            for u in lists:
                if (u not in seen) and (u not in q) and (len(seeds) + len(q) < max_lists * 2):
                    q.append(u)

    # یکتا و محدود
    return _uniq(seeds)[:max_lists]
