# ./services/scraper/details.py
import re, hashlib
from typing import Dict, Any
from adapters.myket import parse_myket
from adapters.bazaar import parse_bazaar

def detect_store(page_url: str, html_text: str) -> str:
    u = (page_url or "").lower()
    h = (html_text or "").lower()
    if "cafebazaar" in u or "bazaar://" in h or "bazaar.ir" in u:
        return "bazaar"
    if "myket" in u or "myket.ir" in u:
        return "myket"
    return "myket"

def parse_any(page_url: str, html_text: str) -> Dict[str, Any]:
    store = detect_store(page_url, html_text)
    data = parse_bazaar(page_url, html_text) if store == "bazaar" else parse_myket(page_url, html_text)
    package = data.get("package") or ""
    data["_id"] = f"{store}:{package}" if package else hashlib.sha1((page_url or html_text[:512]).encode("utf-8","ignore")).hexdigest()
    data["url"] = data.get("url") or page_url
    return data
