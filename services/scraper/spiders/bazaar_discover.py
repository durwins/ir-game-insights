import re
from typing import List, Tuple
import httpx
from selectolax.parser import HTMLParser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://cafebazaar.ir/",
}
APP_PAT = re.compile(r"/app/([A-Za-z0-9._-]+)")

def _links_from(html: str, base: str) -> Tuple[List[str], List[str]]:
    doc = HTMLParser(html)
    apps, lists = [], []
    for a in doc.css("a"):
        if not a.attributes: continue
        href = a.attributes.get("href")
        if not href: continue
        try:
            url = str(httpx.URL(base).join(href))
        except Exception:
            continue
        if not url.startswith("https://cafebazaar.ir/"): continue
        if "/video/" in url: continue

        if APP_PAT.search(url):
            apps.append(url)
        elif ("/cat/" in url) or ("/category/game" in url):
            lists.append(url)
        elif "?page=" in url and ("/cat/" in base or "/category/game" in base):
            lists.append(url)

    def uniq(xs):
        seen=set(); out=[]
        for x in xs:
            if x in seen: continue
            seen.add(x); out.append(x)
        return out

    return uniq(apps), uniq(lists)

def discover_from_bazaar_root(root: str, max_lists: int = 300) -> List[str]:
    """
    از صفحه‌ی فهرست دسته‌ها شروع می‌کنیم (root) و همه‌ی /cat/* + صفحه‌های page=N را تولید می‌کنیم.
    خروجی: URLهای «لیست/دسته» (نه اپ‌ها) برای اسکن توسط worker.
    """
    seeds: List[str] = []
    seen = set()
    q: List[str] = [root]

    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        while q and len(seeds) < max_lists:
            url = q.pop(0)
            if url in seen: continue
            seen.add(url)

            try:
                r = client.get(url); r.raise_for_status()
            except Exception as e:
                print(f"[BAZAAR] fetch error {url}: {e}")
                continue

            # خود همین صفحه هم برای اسکن بعدی نگه می‌داریم
            seeds.append(str(r.url))

            # لینک‌های داخل صفحه
            apps, lists = _links_from(r.text, str(r.url))

            # از هر /cat/* به صورت پیش‌فرض page=2..50 رو هم اضافه کن (احتمال صفحه‌بندی)
            for u in lists:
                if "/cat/" in u:
                    base_no_q = u.split("?")[0]
                    for p in range(2, 51):
                        paged = f"{base_no_q}?page={p}"
                        if paged not in seen and paged not in q and len(seeds) + len(q) < max_lists*2:
                            q.append(paged)

            # باقی لیست‌ها
            for u in lists:
                if u not in seen and u not in q and len(seeds) + len(q) < max_lists*2:
                    q.append(u)

    return list(dict.fromkeys(seeds))[:max_lists]
