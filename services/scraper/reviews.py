# services/scraper/reviews.py
import re, html, json
from typing import List, Dict, Optional, Tuple

def _clean(t: Optional[str]) -> Optional[str]:
    if not t: return None
    return html.unescape(t).strip()

def parse_reviews_myket(page_url: str, html_text: str, limit: int = 50) -> List[Dict]:
    """
    TODO: بعد از دریافت دو نمونه HTML «صفحه ریویو» از مایکت، selectorها را دقیق می‌کنیم.
    فعلاً سعی می‌کنیم چند ریویو اولیه را از خود صفحه اپ (اگه SSR باشد) بخوانیم.
    """
    reviews: List[Dict] = []

    # نمونه اسکلت؛ اگر بلوک ریویوی مشخصی در HTML بود:
    # for m in re.finditer(r'<div class="ReviewItem">(.+?)</div>\s*</div>', html_text, re.S):
    #     block = m.group(1)
    #     txt = _clean(re.search(r'<p[^>]*>(.*?)</p>', block, re.S).group(1)) if re.search(...) else None
    #     rating = ...
    #     author = ...
    #     created = ...
    #     if txt:
    #         reviews.append({
    #             "store": "myket",
    #             "rating": rating,
    #             "text": txt,
    #             "author": author,
    #             "created_at": created
    #         })
    #     if len(reviews) >= limit: break

    return reviews

def parse_reviews_bazaar(page_url: str, html_text: str, limit: int = 50) -> List[Dict]:
    """
    TODO: پس از نمونه HTML ریویو «بازار»، selectorها را دقیق می‌کنیم.
    """
    reviews: List[Dict] = []
    return reviews

def parse_reviews(page_url: str, html_text: str, limit: int = 50) -> List[Dict]:
    if "myket.ir" in page_url:
        return parse_reviews_myket(page_url, html_text, limit=limit)
    if "cafebazaar.ir" in page_url:
        return parse_reviews_bazaar(page_url, html_text, limit=limit)
    return []
