# ./services/scraper/spiders/myket_discover.py
import re
from typing import List, Tuple
import httpx
from selectolax.parser import HTMLParser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://myket.ir/",
}

APP_PAT = re.compile(r"/app/([A-Za-z0-9._-]+)")

def _links_from(html: str, base: str) -> Tuple[List[str], List[str]]:
    doc = HTMLParser(html)
    app_links, list_links = [], []
    for a in doc.css("a"):
        href = a.attributes.get("href") if a.attributes else None
        if not href:
            continue
        try:
            url = str(httpx.URL(base).join(href))
        except Exception:
            continue
        if not url.startswith("https://myket.ir/"):
            continue
        if APP_PAT.search(url):
            app_links.append(url)
        elif ("/games/" in url or "/list/" in url) and ("/video/" not in url):
            list_links.append(url)
    # de-dup keep order
    def uniq(xs): 
        s=set(); out=[]
        for x in xs:
            if x in s: continue
            s.add(x); out.append(x)
        return out
    return uniq(app_links), uniq(list_links)

def discover_from_games_root(games_root: str = "https://myket.ir/games", max_lists: int = 200) -> List[str]:
    seeds: List[str] = []
    seen = set()
    q: List[str] = [games_root]
    with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30) as client:
        while q and len(seeds) < max_lists:
            url = q.pop(0)
            if url in seen:
                continue
            seen.add(url)
            try:
                r = client.get(url)
                r.raise_for_status()
            except Exception as e:
                print(f"[DISCOVER] fetch error {url}: {e}")
                continue
            _, list_links = _links_from(r.text, str(r.url))
            seeds.append(url)  # خود همین صفحه را هم seed نگه می‌داریم
            for u in list_links:
                if u not in seen and u not in q and len(seeds) + len(q) < max_lists * 2:
                    q.append(u)
    # اگر به هر دلیل چیزی پیدا نشد، حداقل خود root را برگردان
    return seeds or [games_root]
