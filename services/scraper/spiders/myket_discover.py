# ./services/scraper/spiders/myket_discover.py
import re
from typing import List, Tuple, Set
import httpx
from selectolax.parser import HTMLParser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://myket.ir/",
}

APP_PAT = re.compile(r"/app/[A-Za-z0-9._-]+(?:\?.*)?$")
LIST_HINTS = (
    "/games/",      # صفحات اصلی بازی‌ها و زیرمسیرهایش
    "/list/",       # لیست‌های موضوعی
    "/collection/", # اگر وجود داشته باشد
    "/search",      # نتایج جستجو
)

def _uniq(seq: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _normalize(base: str, href: str) -> str:
    try:
        u = httpx.URL(base).join(href)
        if u.fragment:
            u = u.copy_with(fragment="")
        # path را تمیز کن
        path = re.sub(r"/{2,}", "/", u.path)
        u = u.copy_with(path=path)
        return str(u)
    except Exception:
        return ""

def _is_list_url(url: str, base_was_list: bool) -> bool:
    if any(h in url for h in LIST_HINTS):
        return True
    # اگر صفحهٔ پایه خودش لیست بود، page= را هم لیست حساب کن
    if base_was_list and "?page=" in url:
        return True
    return False

def _links_from(html: str, base: str) -> Tuple[List[str], List[str]]:
    """
    برمی‌گرداند:
      app_links  - لینک‌های جزئیات اپ‌ها (/app/…)
      list_links - لینک‌های لیست/دسته برای اسکن بیشتر
    """
    doc = HTMLParser(html)
    app_links, list_links = [], []
    base_is_list = any(h in base for h in LIST_HINTS)

    for a in doc.css("a[href]"):
        href = a.attributes.get("href")
        if not href:
            continue
        url = _normalize(base, href)
        if not url or not url.startswith("https://myket.ir/"):
            continue
        if "/video/" in url:
            continue

        if APP_PAT.search(url):
            app_links.append(url)
        elif _is_list_url(url, base_is_list):
            list_links.append(url)

    # fallback regex اگر DOM مشکل داشت
    if not app_links and not list_links:
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, flags=re.I):
            url = _normalize(base, m.group(1))
            if not url or not url.startswith("https://myket.ir/"):
                continue
            if "/video/" in url:
                continue
            if APP_PAT.search(url):
                app_links.append(url)
            elif _is_list_url(url, base_is_list):
                list_links.append(url)

    return _uniq(app_links), _uniq(list_links)

def discover_from_games_root(games_root: str = "https://myket.ir/games", max_lists: int = 200) -> List[str]:
    """
    از صفحهٔ ریشهٔ بازی‌ها شروع می‌کند و فقط URLهای «لیست/دسته» را برمی‌گرداند
    (نه لینک اپ‌ها). خود هر صفحهٔ بازدیدشده هم به عنوان seed نگه داشته می‌شود.
    """
    seeds: List[str] = []
    seen: Set[str] = set()
    q: List[str] = [games_root]

    # HTTP/2 کمک می‌کند، ولی اجباری نیست
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30, http2=True) as client:
        while q and len(seeds) < max_lists:
            url = q.pop(0)
            if url in seen:
                continue
            seen.add(url)

            try:
                r = client.get(url)
                r.raise_for_status()
            except Exception as e:
                print(f"[MYKET] fetch error {url}: {e}")
                continue

            # خود صفحه را نگه داریم
            seeds.append(str(r.url))
            if len(seeds) >= max_lists:
                break

            _, list_links = _links_from(r.text, str(r.url))

            # صف کردن لینک‌های لیست (با سقف)
            for u in list_links:
                if u not in seen and u not in q and (len(seeds) + len(q) < max_lists * 2):
                    q.append(u)

    return seeds or [games_root]
