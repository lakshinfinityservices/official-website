#!/usr/bin/env python3
"""
Laksh Tech Hub - news fetcher.

Collects science & technology stories from public RSS/Atom feeds, sorts every
story into one of the 12 Tech Hub categories, merges with the stories already
stored in articles.json, and DROPS anything older than DAYS_TO_KEEP (15) days.

Run from anywhere:  python scripts/fetch_news.py
Output:             <repo root>/articles.json
"""

import html
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET


# --------------------------------------------------
# SETTINGS
# --------------------------------------------------

# articles.json lives in the repository root (next to index.html),
# whatever folder the script is started from.
ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = ROOT_DIR / "articles.json"

DAYS_TO_KEEP = 15            # anything older than this is removed automatically
MAX_PER_CATEGORY = 30        # keeps every Tech Hub card full but the file small
MAX_ENTRIES_PER_FEED = 40
REQUEST_TIMEOUT = 25
MAX_FEED_BYTES = 6 * 1024 * 1024
WORKERS = 8
USER_AGENT = "LakshTechHub/2.0 (+https://github.com/lakshinfinityservices/official-website)"

CATEGORIES = [
    "Innovation",
    "Alert",
    "News",
    "Threats",
    "Notifications",
    "Research & Discovery",
    "Emerging Technology",
    "Trends & Forecasts",
    "Industry & Business",
    "Policy & Regulation",
    "Updates & Patches",
    "Opportunities",
]


def gnews(query):
    """Google News RSS search limited to the retention window."""
    q = query.replace(" ", "+")
    return (
        f"https://news.google.com/rss/search?q={q}+when:{DAYS_TO_KEEP}d"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    )


# "hint" = the category a feed most likely belongs to. It is only used as a
# tie-breaker when the story's own words do not point to a clear category.
RSS_FEEDS = [
    # ---- General science & technology news -------------------------------
    {"url": "https://www.sciencedaily.com/rss/top/science.xml", "source": "ScienceDaily", "hint": "Research & Discovery"},
    {"url": "https://phys.org/rss-feed/science-news/", "source": "Phys.org", "hint": "Research & Discovery"},
    {"url": "https://phys.org/rss-feed/technology-news/", "source": "Phys.org Technology", "hint": "Innovation"},
    {"url": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "source": "BBC Science", "hint": "News"},
    {"url": "https://feeds.bbci.co.uk/news/technology/rss.xml", "source": "BBC Technology", "hint": "News"},
    {"url": "https://www.technologyreview.com/feed/", "source": "MIT Technology Review", "hint": "Emerging Technology"},
    {"url": "https://www.theverge.com/rss/index.xml", "source": "The Verge", "hint": "News"},
    {"url": "https://feeds.arstechnica.com/arstechnica/index", "source": "Ars Technica", "hint": "News"},
    {"url": "https://www.wired.com/feed/rss", "source": "WIRED", "hint": "News"},
    {"url": "https://www.newscientist.com/feed/home/", "source": "New Scientist", "hint": "Research & Discovery"},
    {"url": "https://www.engadget.com/rss.xml", "source": "Engadget", "hint": "News"},
    {"url": "https://www.theregister.com/headlines.atom", "source": "The Register", "hint": "News"},
    {"url": "https://www.zdnet.com/news/rss.xml", "source": "ZDNet", "hint": "News"},
    {"url": "https://www.indiatoday.in/rss/1206550", "source": "India Today Technology", "hint": "News"},
    {"url": "https://www.thehindu.com/sci-tech/technology/feeder/default.rss", "source": "The Hindu Tech", "hint": "News"},

    # ---- Research & Discovery --------------------------------------------
    {"url": "https://www.nature.com/nature.rss", "source": "Nature", "hint": "Research & Discovery"},
    {"url": "https://www.science.org/rss/news_current.xml", "source": "Science", "hint": "Research & Discovery"},
    {"url": "https://www.eurekalert.org/rss.xml", "source": "EurekAlert", "hint": "Research & Discovery"},
    {"url": "https://api.quantamagazine.org/feed/", "source": "Quanta Magazine", "hint": "Research & Discovery"},
    {"url": "https://www.nasa.gov/feed/", "source": "NASA", "hint": "Research & Discovery"},
    {"url": "https://rss.arxiv.org/rss/cs.AI", "source": "arXiv AI", "hint": "Research & Discovery"},

    # ---- Emerging Technology ---------------------------------------------
    {"url": "https://spectrum.ieee.org/feeds/feed.rss", "source": "IEEE Spectrum", "hint": "Emerging Technology"},
    {"url": "https://spacenews.com/feed/", "source": "SpaceNews", "hint": "Emerging Technology"},
    {"url": "https://www.space.com/feeds/all", "source": "Space.com", "hint": "Emerging Technology"},
    {"url": "https://thequantuminsider.com/feed/", "source": "The Quantum Insider", "hint": "Emerging Technology"},
    {"url": "https://www.therobotreport.com/feed/", "source": "The Robot Report", "hint": "Emerging Technology"},
    {"url": "https://venturebeat.com/category/ai/feed/", "source": "VentureBeat AI", "hint": "Emerging Technology"},

    # ---- Innovation ------------------------------------------------------
    {"url": "https://newatlas.com/index.rss", "source": "New Atlas", "hint": "Innovation"},
    {"url": "https://interestingengineering.com/feed", "source": "Interesting Engineering", "hint": "Innovation"},
    {"url": "https://hackaday.com/blog/feed/", "source": "Hackaday", "hint": "Innovation"},

    # ---- Threats / Alert / Notifications ---------------------------------
    {"url": "https://www.cisa.gov/cybersecurity-advisories/all.xml", "source": "CISA", "hint": "Notifications"},
    {"url": "https://www.bleepingcomputer.com/feed/", "source": "BleepingComputer", "hint": "Threats"},
    {"url": "https://feeds.feedburner.com/TheHackersNews", "source": "The Hacker News", "hint": "Threats"},
    {"url": "https://krebsonsecurity.com/feed/", "source": "Krebs on Security", "hint": "Threats"},
    {"url": "https://feeds.feedburner.com/securityweek", "source": "SecurityWeek", "hint": "Threats"},
    {"url": "https://www.darkreading.com/rss.xml", "source": "Dark Reading", "hint": "Threats"},
    {"url": gnews("critical vulnerability actively exploited zero-day"), "source": "Google News", "hint": "Alert", "gnews": True},
    {"url": gnews("urgent security warning technology outage recall"), "source": "Google News", "hint": "Alert", "gnews": True},
    {"url": gnews("CERT-In advisory OR CISA advisory OR official advisory technology"), "source": "Google News", "hint": "Notifications", "gnews": True},

    # ---- Updates & Patches -----------------------------------------------
    {"url": "https://chromereleases.googleblog.com/feeds/posts/default", "source": "Chrome Releases", "hint": "Updates & Patches"},
    {"url": "https://msrc.microsoft.com/blog/feed/", "source": "Microsoft Security Response Center", "hint": "Updates & Patches"},
    {"url": "https://ubuntu.com/security/notices/rss.xml", "source": "Ubuntu Security Notices", "hint": "Updates & Patches"},
    {"url": gnews("firmware update OR security patch OR BIOS update laptop"), "source": "Google News", "hint": "Updates & Patches", "gnews": True},
    {"url": gnews("end of life OR end of support software hardware"), "source": "Google News", "hint": "Updates & Patches", "gnews": True},

    # ---- Industry & Business / Trends ------------------------------------
    {"url": "https://techcrunch.com/feed/", "source": "TechCrunch", "hint": "Industry & Business"},
    {"url": "https://news.crunchbase.com/feed/", "source": "Crunchbase News", "hint": "Industry & Business"},
    {"url": "https://siliconangle.com/feed/", "source": "SiliconANGLE", "hint": "Industry & Business"},
    {"url": "https://inc42.com/feed/", "source": "Inc42", "hint": "Industry & Business"},
    {"url": "https://economictimes.indiatimes.com/tech/rssfeeds/13357270.cms", "source": "Economic Times Tech", "hint": "Industry & Business"},
    {"url": gnews("technology market forecast OR outlook OR adoption trend"), "source": "Google News", "hint": "Trends & Forecasts", "gnews": True},
    {"url": gnews("tech investment trends analysts predict"), "source": "Google News", "hint": "Trends & Forecasts", "gnews": True},

    # ---- Policy & Regulation ---------------------------------------------
    {"url": "https://www.eff.org/rss/updates.xml", "source": "EFF", "hint": "Policy & Regulation"},
    {"url": "https://www.techpolicy.press/rss/feed.xml", "source": "Tech Policy Press", "hint": "Policy & Regulation"},
    {"url": "https://www.medianama.com/feed/", "source": "MediaNama", "hint": "Policy & Regulation"},
    {"url": gnews("technology regulation OR privacy law OR AI Act OR data protection"), "source": "Google News", "hint": "Policy & Regulation", "gnews": True},

    # ---- Opportunities ---------------------------------------------------
    {"url": "https://www.nsf.gov/rss/rss_www_funding_pgm_annc_inf.xml", "source": "NSF Funding", "hint": "Opportunities"},
    {"url": gnews("research grant OR fellowship OR hackathon OR technology challenge applications open"), "source": "Google News", "hint": "Opportunities", "gnews": True},
    {"url": gnews("technology certification OR skilling programme OR innovation challenge India"), "source": "Google News", "hint": "Opportunities", "gnews": True},
]


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# --------------------------------------------------
# HTML TEXT AND IMAGE EXTRACTION
# --------------------------------------------------

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        return " ".join(self.parts)


class ImageExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.images = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "img":
            attributes = dict(attrs)
            image = (
                attributes.get("src")
                or attributes.get("data-src")
                or attributes.get("data-original")
            )
            if image and not image.startswith("data:"):
                self.images.append(image)


def clean_text(value):
    if not value:
        return ""
    extractor = TextExtractor()
    try:
        extractor.feed(str(value))
        result = extractor.get_text()
    except Exception:
        result = str(value)
    result = html.unescape(result)
    return re.sub(r"\s+", " ", result).strip()


def extract_image_from_html(value, base_url):
    if not value:
        return ""
    extractor = ImageExtractor()
    try:
        extractor.feed(str(value))
        if extractor.images:
            return urljoin(base_url, extractor.images[0])
    except Exception:
        pass
    return ""


# --------------------------------------------------
# XML HELPERS
# --------------------------------------------------

def local_name(tag):
    if not isinstance(tag, str):
        return ""
    return tag.split("}")[-1].lower()


def get_child_text(element, names):
    names = [name.lower() for name in names]
    for child in list(element):
        if local_name(child.tag) in names:
            value = "".join(child.itertext()).strip()
            if value:
                return value
    return ""


def get_link(entry, feed_url):
    # RSS: <link>https://...</link>
    link = get_child_text(entry, ["link"])
    if link:
        return urljoin(feed_url, link.strip())

    # Atom: <link rel="alternate" href="..."/>  (prefer alternate)
    fallback = ""
    for child in list(entry):
        if local_name(child.tag) != "link":
            continue
        href = child.attrib.get("href", "")
        if not href:
            continue
        if child.attrib.get("rel", "alternate") == "alternate":
            return urljoin(feed_url, href)
        fallback = fallback or href
    if fallback:
        return urljoin(feed_url, fallback)

    # Last resort: a <guid> that is a URL
    guid = get_child_text(entry, ["guid", "id"])
    if guid.startswith("http"):
        return guid
    return ""


IMAGE_RE = re.compile(r"\.(jpg|jpeg|png|webp|gif|avif)(\?|$)", re.I)


def get_image(element, feed_url, description):
    for child in element.iter():
        name = local_name(child.tag)

        if name in ("thumbnail", "content", "enclosure"):
            url = child.attrib.get("url", "")
            if not url:
                continue
            kind = child.attrib.get("type", "")
            medium = child.attrib.get("medium", "")
            if (
                name == "thumbnail"
                or kind.startswith("image/")
                or medium == "image"
                or IMAGE_RE.search(url)
            ):
                return urljoin(feed_url, url)

    return extract_image_from_html(description, feed_url)


# --------------------------------------------------
# DATE HANDLING
# --------------------------------------------------

def parse_date(value):
    """Return an aware UTC datetime, or None."""
    if not value:
        return None
    value = value.strip()

    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.astimezone(timezone.utc)
    except Exception:
        return None


def parse_iso(value):
    return parse_date(value) if value else None


# --------------------------------------------------
# CATEGORY CLASSIFICATION
# --------------------------------------------------
# Every keyword is matched on word boundaries. A hit in the headline is worth
# 3 points, a hit in the summary 1 point, and the feed's own "hint" category
# gets 2 points. The highest score wins; under 3 points falls back to the
# feed hint, then to "News".

CATEGORY_KEYWORDS = {
    "Alert": [
        "zero-day", "zero day", "actively exploited", "known exploited",
        "critical vulnerability", "critical flaw", "critical security",
        "emergency patch", "emergency update", "urgent", "security alert",
        "recall", "recalled", "outage", "goes down", "service disruption",
        "safety warning", "warns users", "warns of", "red alert",
        "fire hazard", "overheating battery", "do not use",
    ],
    "Threats": [
        "ransomware", "malware", "phishing", "cyberattack", "cyber attack",
        "cyber-attack", "data breach", "breached", "hackers", "hacker",
        "hacked", "espionage", "spyware", "botnet", "ddos", "infostealer",
        "trojan", "backdoor", "supply chain attack", "supply-chain attack",
        "supply-chain", "fraud", "scam", "scammers", "state-sponsored",
        "threat actor", "apt", "vulnerability", "vulnerabilities", "exploit",
        "cyber threat", "stolen data", "leaked",
    ],
    "Notifications": [
        "advisory", "advisories", "bulletin", "circular", "public notice",
        "notice", "notification", "deadline", "mandatory", "official statement",
        "guidelines issued", "guidelines", "notified", "announces policy",
        "important notice", "compliance deadline", "last date",
    ],
    "Research & Discovery": [
        "researchers", "scientists", "study", "studies", "discovery",
        "discovered", "discover", "journal", "peer-reviewed", "paper",
        "clinical trial", "experiment", "laboratory", "lab study", "findings",
        "fossil", "species", "genome", "telescope", "astronomers", "physicists",
        "biologists", "arxiv", "nature", "published in",
    ],
    "Emerging Technology": [
        "quantum", "robot", "robots", "robotics", "humanoid",
        "artificial intelligence", "ai", "machine learning", "llm",
        "large language model", "generative ai", "agentic", "chatbot",
        "autonomous", "self-driving", "driverless", "drone", "drones",
        "satellite", "rocket", "spacecraft", "lunar", "moon mission", "mars",
        "space station", "graphene", "metamaterial", "advanced materials",
        "neuromorphic", "fusion", "semiconductor", "photonic", "6g",
        "augmented reality", "virtual reality", "brain-computer",
    ],
    "Trends & Forecasts": [
        "forecast", "forecasts", "outlook", "trend", "trends", "predicts",
        "prediction", "predictions", "projected", "projection", "expected to",
        "market size", "adoption", "survey finds", "survey", "by 2030",
        "by 2028", "by 2027", "analysts", "gartner", "idc", "future of",
        "will reach", "growth rate", "demand for",
    ],
    "Industry & Business": [
        "acquires", "acquisition", "acquired", "merger", "merge", "raises",
        "raised", "funding", "series a", "series b", "series c", "seed round",
        "earnings", "revenue", "quarterly", "profit", "partnership", "partners with",
        "appoints", "appointed", "steps down", "ceo", "cfo", "ipo", "valuation",
        "layoffs", "joint venture", "stake", "shares", "investors", "startup",
    ],
    "Policy & Regulation": [
        "regulation", "regulations", "regulator", "regulators", "regulatory",
        "legislation", "bill", "lawmakers", "law", "executive order",
        "privacy", "gdpr", "dpdp", "antitrust", "compliance", "standards",
        "ban", "bans", "banned", "fined", "fine", "supreme court", "court",
        "export controls", "senate", "parliament", "ai act", "government",
        "ministry", "policy", "right to repair",
    ],
    "Updates & Patches": [
        "patch", "patches", "patched", "security update", "update",
        "updates", "firmware", "release notes", "rolls out", "rollout",
        "end of life", "end-of-life", "end of support", "end-of-support",
        "bios", "driver", "stable channel", "new version", "version",
        "windows 11", "windows 10", "android 1", "ios 2", "ubuntu", "kernel",
        "hotfix", "bug fix", "upgrade", "cve-",
    ],
    "Opportunities": [
        "grant", "grants", "fellowship", "fellowships", "scholarship",
        "call for proposals", "call for papers", "applications open",
        "apply now", "applications invited", "hackathon", "challenge",
        "certification", "internship", "funding opportunity", "accelerator",
        "bootcamp", "upskilling", "skilling", "competition", "programme",
        "program", "course", "workshop", "registration open",
    ],
    "Innovation": [
        "invention", "invents", "invented", "patent", "patents", "prototype",
        "breakthrough", "new material", "novel", "unveils", "world's first",
        "first-ever", "engineers develop", "engineers create", "innovative",
        "innovation", "new device", "new technology", "developed a new",
        "creates a new", "cheaper way", "new method",
    ],
}

# Tie-breaker order when two categories score the same
PRIORITY = [
    "Alert", "Threats", "Updates & Patches", "Notifications",
    "Policy & Regulation", "Opportunities", "Industry & Business",
    "Research & Discovery", "Emerging Technology", "Innovation",
    "Trends & Forecasts",
]

KEYWORD_PATTERNS = {
    category: [
        re.compile(r"(?<![a-z0-9])" + re.escape(word) + r"(?![a-z0-9])")
        for word in words
    ]
    for category, words in CATEGORY_KEYWORDS.items()
}


def classify_category(title, description, hint=""):
    title_l = (title or "").lower()
    desc_l = (description or "").lower()

    scores = {}
    for category, patterns in KEYWORD_PATTERNS.items():
        score = 0
        for pattern in patterns:
            if pattern.search(title_l):
                score += 3
            if pattern.search(desc_l):
                score += 1
        if category == hint:
            score += 2
        scores[category] = score

    best_score = max(scores.values())
    if best_score < 3:
        return hint if hint in CATEGORIES else "News"

    for category in PRIORITY:
        if scores[category] == best_score:
            return category
    return hint if hint in CATEGORIES else "News"


# --------------------------------------------------
# FETCH RSS FEEDS
# --------------------------------------------------

CONTROL_CHARS = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def parse_xml(content):
    try:
        return ET.fromstring(content)
    except ET.ParseError:
        return ET.fromstring(CONTROL_CHARS.sub(b"", content))


def fetch_feed(feed):
    feed_url = feed["url"]
    source_name = feed["source"]
    hint = feed.get("hint", "")
    is_gnews = feed.get("gnews", False)
    now = datetime.now(timezone.utc)

    request = Request(
        feed_url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        },
    )

    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            content = response.read(MAX_FEED_BYTES)
        root = parse_xml(content)
    except Exception as error:
        logging.warning("Could not read %s: %s", source_name, error)
        return []

    entries = [
        element for element in root.iter()
        if local_name(element.tag) in ("item", "entry")
    ][:MAX_ENTRIES_PER_FEED]

    articles = []

    for entry in entries:
        title = clean_text(get_child_text(entry, ["title"]))
        if not title:
            continue

        link = get_link(entry, feed_url)
        if not link:
            continue

        source = source_name
        description_raw = get_child_text(
            entry, ["description", "summary", "encoded", "content"]
        )
        description = clean_text(description_raw)

        if is_gnews:
            # Google News titles end with " - Publisher"; the real publisher
            # is also in <source>. The description is only a link list.
            publisher = clean_text(get_child_text(entry, ["source"]))
            if publisher:
                source = publisher
                suffix = " - " + publisher
                if title.endswith(suffix):
                    title = title[: -len(suffix)].strip()
            description = ""

        published = parse_date(
            get_child_text(entry, ["pubdate", "published", "updated", "date"])
        )

        if len(description) > 350:
            description = description[:347].rsplit(" ", 1)[0] + "..."

        articles.append({
            "title": title,
            "description": description,
            "url": link,
            "source": source,
            "publishedAt": published.isoformat() if published else "",
            "category": classify_category(title, description, hint),
            "image": "" if is_gnews else get_image(entry, feed_url, description_raw),
        })

    logging.info("Found %s articles from %s", len(articles), source_name)
    return articles


# --------------------------------------------------
# EXISTING DATA, CLEAN-UP, DUPLICATES
# --------------------------------------------------

def load_existing():
    """Stories already saved in articles.json (so a failed feed never empties a card)."""
    try:
        data = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(data, dict) and isinstance(data.get("articles"), list):
        items = data["articles"]
    elif isinstance(data, list):
        items = data
    else:
        return []  # e.g. the single sample object

    return [a for a in items if isinstance(a, dict) and a.get("title") and a.get("url")]


def url_key(url):
    """Normalise a URL so the same story from two feeds is not shown twice."""
    try:
        parts = urlsplit(url.strip())
    except Exception:
        return url.strip().lower()
    host = parts.netloc.lower()
    # Google News links need their query string to stay unique
    query = parts.query if "news.google.com" in host else ""
    return urlunsplit((parts.scheme.lower(), host, parts.path.rstrip("/"), query, "")).lower()


def title_key(title):
    return re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()


def prepare_articles(fresh, existing):
    """Merge, drop everything older than DAYS_TO_KEEP, de-duplicate, balance."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DAYS_TO_KEEP)

    # first-seen date of stories we already stored
    first_seen = {}
    for article in existing:
        when = parse_iso(article.get("publishedAt"))
        if when:
            first_seen[url_key(article["url"])] = when

    merged = []
    for article in fresh:
        when = parse_iso(article.get("publishedAt"))
        if not when:
            # feed gave no date: keep the date we first saw it, else today
            when = first_seen.get(url_key(article["url"]), now)
        article["publishedAt"] = min(when, now).isoformat()  # no future dates
        merged.append(article)
    merged.extend(existing)  # existing stories fill in anything not re-fetched

    seen_urls, seen_titles = set(), set()
    kept = []
    removed_old = 0

    # newest first, so when duplicates exist the freshest copy wins
    def sort_date(item):
        return parse_iso(item.get("publishedAt")) or datetime.min.replace(tzinfo=timezone.utc)

    merged.sort(key=sort_date, reverse=True)

    for article in merged:
        title = (article.get("title") or "").strip()
        url = (article.get("url") or "").strip()
        if not title or not url.startswith(("http://", "https://")):
            continue

        when = parse_iso(article.get("publishedAt"))
        if not when or when < cutoff:
            removed_old += 1
            continue  # older than 15 days -> removed automatically

        uk, tk = url_key(url), title_key(title)
        if uk in seen_urls or tk in seen_titles:
            continue
        seen_urls.add(uk)
        seen_titles.add(tk)

        if article.get("category") not in CATEGORIES:
            article["category"] = classify_category(
                title, article.get("description", ""), ""
            )

        kept.append({
            "title": title,
            "description": article.get("description", ""),
            "url": url,
            "source": article.get("source", ""),
            "publishedAt": when.isoformat(),
            "category": article["category"],
            "image": article.get("image", ""),
        })

    # keep the newest MAX_PER_CATEGORY stories in each category
    counts = {}
    balanced = []
    for article in kept:  # already newest first
        n = counts.get(article["category"], 0)
        if n < MAX_PER_CATEGORY:
            counts[article["category"]] = n + 1
            balanced.append(article)

    logging.info("Removed %s stories older than %s days", removed_old, DAYS_TO_KEEP)
    return balanced


# --------------------------------------------------
# SAVE ARTICLES.JSON
# --------------------------------------------------

def save_articles(articles):
    counts = {c: 0 for c in CATEGORIES}
    for article in articles:
        counts[article["category"]] = counts.get(article["category"], 0) + 1

    data = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "daysKept": DAYS_TO_KEEP,
        "articleCount": len(articles),
        "categoryCounts": counts,
        "articles": articles,
    }

    OUTPUT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    logging.info("Saved %s articles to %s", len(articles), OUTPUT_FILE)
    for category in CATEGORIES:
        logging.info("  %-22s %s", category, counts[category])


# --------------------------------------------------
# MAIN PROGRAM
# --------------------------------------------------

def main():
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(fetch_feed, RSS_FEEDS))

    fresh = [article for batch in results for article in batch]
    failed = sum(1 for batch in results if not batch)
    logging.info("Fetched %s stories from %s feeds (%s empty/failed)",
                 len(fresh), len(RSS_FEEDS), failed)

    existing = load_existing()

    # If the network is down, keep the old file instead of wiping it.
    if not fresh and not existing:
        logging.error("No stories fetched and nothing stored. Leaving file untouched.")
        return 1

    final_articles = prepare_articles(fresh, existing)
    save_articles(final_articles)
    logging.info("News update completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
