"""
brazil_news_fetcher.py

Fetches RSS headlines from Brazilian news sources, translates them to English,
categorizes them, and writes output to docs/brazil_news.json.

Categories: Diplomacy, Military, Energy, Economy, Local Events
Max 20 stories per category, no story older than 7 days.
Replaces oldest entries when new stories are found.
No API keys required — uses deep-translator (Google Translate backend, free tier).
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

COUNTRY = "brazil"
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / f"{COUNTRY}_news.json"

MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7

CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# ---------------------------------------------------------------------------
# RSS Sources
# Notes:
#   - oglobo.globo.com: O Globo's standalone RSS has been discontinued;
#     replaced with R7 Noticias (https://noticias.r7.com/feed.xml), a major
#     Brazilian broadcaster with an active feed.
#   - noticias.uol.com.br: No clean public RSS endpoint; replaced with the
#     main UOL RSS aggregator (https://rss.uol.com.br/) which surfaces UOL
#     Noticias content.
# ---------------------------------------------------------------------------

RSS_SOURCES = [
    {
        "name": "Folha de S.Paulo",
        "urls": [
            "https://feeds.folha.uol.com.br/emcimadahora/rss091.xml",
            "https://www1.folha.uol.com.br/feed/",
        ],
    },
    {
        "name": "R7 Noticias",           # replaces O Globo (RSS discontinued)
        "urls": [
            "https://noticias.r7.com/feed.xml",
        ],
    },
    {
        "name": "Agencia Brasil (EBC)",
        "urls": [
            "https://agenciabrasil.ebc.com.br/rss/feed.xml",
        ],
    },
    {
        "name": "G1 Globo",
        "urls": [
            "https://g1.globo.com/rss/g1/brasil/index.xml",
            "https://g1.globo.com/rss/g1/economia/index.xml",
            "https://g1.globo.com/rss/g1/politica/index.xml",
        ],
    },
    {
        "name": "UOL",                   # replaces noticias.uol.com.br direct (no clean RSS)
        "urls": [
            "https://rss.uol.com.br/",
        ],
    },
    {
        "name": "Correio Braziliense",
        "urls": [
            "https://www.correiobraziliense.com.br/feed",
        ],
    },
    {
        "name": "Estadao",
        "urls": [
            "https://www.estadao.com.br/arc/outboundfeeds/rss/",
            "https://www.estadao.com.br/arc/outboundfeeds/rss/?from=0&outputType=json",
        ],
    },
    {
        "name": "The Brazilian Report",
        "urls": [
            "https://brazilian.report/feed/",
        ],
    },
    {
        "name": "The Rio Times Online",
        "urls": [
            "https://riotimesonline.com/feed/",
        ],
    },
]

# ---------------------------------------------------------------------------
# Category keyword rules (applied to translated title + description)
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacy", "diplomatic", "foreign minister", "ambassador", "embassy",
        "treaty", "bilateral", "multilateral", "un ", "united nations", "g20",
        "brics", "summit", "lula", "itamaraty", "foreign policy", "trade deal",
        "sanctions", "relations with", "nato", "mercosul", "mercosur",
        "international", "consulate", "visa", "president visit", "state visit",
        "foreign affairs",
    ],
    "Military": [
        "military", "armed forces", "army", "navy", "air force", "defense",
        "soldier", "troops", "weapons", "war", "conflict", "security forces",
        "police operation", "drug trafficking", "organized crime", "gang",
        "operation", "commandos", "marines", "generals", "pentagon", "war games",
        "exercise", "maneuver", "bomb", "missile", "border security",
    ],
    "Energy": [
        "energy", "oil", "gas", "petrobras", "petrol", "fuel", "refinery",
        "renewable", "solar", "wind power", "hydroelectric", "nuclear",
        "electricity", "power plant", "biofuel", "ethanol", "pre-sal",
        "pre-salt", "offshore", "eletrobras", "aneel", "pipeline",
        "energy transition", "carbon", "emissions", "climate",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "interest rate", "selic",
        "central bank", "finance", "budget", "fiscal", "tax", "revenue",
        "trade", "exports", "imports", "investment", "market", "stock",
        "bovespa", "b3", "real ", "currency", "jobs", "unemployment",
        "industry", "agriculture", "agribusiness", "soybean", "beef",
        "recession", "growth", "minister of finance", "bndes", "spending",
    ],
    "Local Events": [
        "state", "city", "municipal", "mayor", "governor", "local",
        "sao paulo", "rio de janeiro", "brasilia", "belo horizonte",
        "salvador", "fortaleza", "manaus", "recife", "porto alegre",
        "curitiba", "flood", "landslide", "drought", "fire", "protest",
        "strike", "election", "education", "health", "hospital", "culture",
        "festival", "carnival", "community", "neighborhood", "infrastructure",
    ],
}


def classify(title: str, description: str) -> str:
    """Return the best matching category or 'Local Events' as fallback."""
    text = (title + " " + (description or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                scores[cat] += 1
    best = max(scores, key=scores.get)
    # Only assign if at least one keyword matched; otherwise fall back
    if scores[best] == 0:
        return "Local Events"
    return best


# ---------------------------------------------------------------------------
# Translation helper
# ---------------------------------------------------------------------------

_translator = GoogleTranslator(source="auto", target="en")


def safe_translate(text: str) -> str:
    """Translate text to English; return original on failure."""
    if not text or not text.strip():
        return text
    # Skip translation if text looks already English (rough heuristic)
    latin_common = re.compile(r"\b(the|and|is|in|of|to|a|for|on|that|with)\b", re.I)
    if len(latin_common.findall(text)) >= 3:
        return text
    try:
        result = _translator.translate(text[:4900])  # API char limit
        return result if result else text
    except Exception as exc:
        log.warning("Translation failed for '%s…': %s", text[:60], exc)
        return text


# ---------------------------------------------------------------------------
# Feed fetching
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; StratagemdriveBrazilNewsBot/1.0; "
        "+https://stratagemdrive.github.io)"
    )
}


def fetch_feed(url: str) -> list[dict]:
    """Fetch and parse a single RSS/Atom feed URL; return list of raw entries."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        log.info("  Fetched %d entries from %s", len(feed.entries), url)
        return feed.entries
    except Exception as exc:
        log.warning("  Could not fetch %s: %s", url, exc)
        return []


def parse_published(entry) -> datetime | None:
    """Extract a timezone-aware published datetime from a feed entry."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    # Try string fields
    for attr in ("published", "updated"):
        s = getattr(entry, attr, None)
        if s:
            try:
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(s)
            except Exception:
                pass
    return None


def entry_to_story(entry, source_name: str) -> dict | None:
    """Convert a feed entry to a story dict; return None if unusable."""
    title_raw = getattr(entry, "title", "") or ""
    desc_raw = getattr(entry, "summary", "") or ""
    # Strip HTML tags from description
    desc_clean = re.sub(r"<[^>]+>", " ", desc_raw).strip()

    url = getattr(entry, "link", "") or ""

    published_dt = parse_published(entry)
    if not published_dt:
        published_dt = datetime.now(timezone.utc)

    # Enforce freshness
    age = datetime.now(timezone.utc) - published_dt
    if age > timedelta(days=MAX_AGE_DAYS):
        return None

    # Translate
    title_en = safe_translate(title_raw)
    desc_en = safe_translate(desc_clean[:300]) if desc_clean else ""

    category = classify(title_en, desc_en)

    return {
        "title": title_en.strip(),
        "source": source_name,
        "url": url.strip(),
        "published_date": published_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "category": category,
    }


# ---------------------------------------------------------------------------
# JSON store management
# ---------------------------------------------------------------------------

def load_existing() -> dict:
    """Load existing JSON or return empty structure."""
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "stories" in data:
                return data
        except Exception as exc:
            log.warning("Could not load existing JSON: %s", exc)
    return {"stories": [], "last_updated": ""}


def save_output(data: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Saved %d stories to %s", len(data["stories"]), OUTPUT_FILE)


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def merge_stories(existing_stories: list[dict], new_stories: list[dict]) -> list[dict]:
    """
    Merge new stories into existing ones per category:
    - Drop stories older than MAX_AGE_DAYS
    - Deduplicate by URL
    - Keep up to MAX_PER_CATEGORY per category
    - When over limit, replace oldest entries first
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MAX_AGE_DAYS)

    # Index existing by URL
    by_url: dict[str, dict] = {}
    for s in existing_stories:
        url = s.get("url", "")
        if url:
            by_url[url] = s

    # Add new stories (overwrite same URL)
    for s in new_stories:
        url = s.get("url", "")
        if url:
            by_url[url] = s

    # Filter out stale stories
    fresh = []
    for s in by_url.values():
        try:
            pub = datetime.strptime(s["published_date"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            if pub >= cutoff:
                fresh.append(s)
        except Exception:
            pass  # Malformed date — discard

    # Group by category
    by_cat: dict[str, list[dict]] = {cat: [] for cat in CATEGORIES}
    for s in fresh:
        cat = s.get("category", "Local Events")
        if cat not in by_cat:
            cat = "Local Events"
        by_cat[cat].append(s)

    # Sort each category newest-first, keep up to MAX_PER_CATEGORY
    result = []
    for cat in CATEGORIES:
        entries = sorted(
            by_cat[cat],
            key=lambda x: x.get("published_date", ""),
            reverse=True,
        )
        result.extend(entries[:MAX_PER_CATEGORY])

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=== Brazil News Fetcher starting ===")
    existing_data = load_existing()
    existing_stories = existing_data.get("stories", [])

    all_new: list[dict] = []

    for source in RSS_SOURCES:
        source_name = source["name"]
        log.info("Processing source: %s", source_name)
        for url in source["urls"]:
            entries = fetch_feed(url)
            for entry in entries:
                story = entry_to_story(entry, source_name)
                if story:
                    all_new.append(story)
            # Be polite to servers
            time.sleep(1)

    log.info("Collected %d candidate new stories", len(all_new))

    merged = merge_stories(existing_stories, all_new)

    # Summary
    for cat in CATEGORIES:
        count = sum(1 for s in merged if s.get("category") == cat)
        log.info("  %-15s: %d stories", cat, count)

    output = {
        "country": COUNTRY,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories": merged,
    }

    save_output(output)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
