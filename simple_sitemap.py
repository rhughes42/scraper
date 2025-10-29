"""
Simple sitemap crawler mode.

Traverses a website within the same domain and gathers lightweight metadata
for each reachable HTML page. Intended for small to medium sites where a full
site crawl is acceptable without specialised crawling infrastructure.
"""

from __future__ import annotations

from collections import Counter, deque
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag, urlunparse

import re

import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.exceptions import RequestException

ALLOWED_SCHEMES = ("http", "https")
DEFAULT_TIMEOUT = 15
MAX_KEYWORDS = 15
MIN_KEYWORD_LENGTH = 4


class SitemapGenerationError(Exception):
    """Raised when sitemap generation cannot be completed."""


def generate_sitemap(start_url: str, max_pages: Optional[int] = None) -> Dict:
    """
    Crawl the target website starting from start_url and build a sitemap.

    Args:
        start_url: Starting URL for the crawl.
        max_pages: Optional safety limit on pages to visit.

    Returns:
        Dictionary describing the sitemap and page metadata.
    """
    normalized_start = _prepare_start_url(start_url)
    parsed_start = urlparse(normalized_start)
    domain = parsed_start.netloc

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "PandektesSitemapBot/1.0 (+https://pandektes.example)",
            "Accept": "text/html,application/xhtml+xml",
        }
    )

    visited: Set[str] = set()
    queue: deque[str] = deque([normalized_start])
    pages: List[Dict] = []

    while queue:
        current_url = queue.popleft()
        if current_url in visited:
            continue
        if max_pages is not None and len(visited) >= max_pages:
            break

        visited.add(current_url)

        page_data, outgoing_links = _process_page(session, current_url, domain)
        if page_data is None:
            continue

        pages.append(page_data)

        for link in outgoing_links:
            if link not in visited and link not in queue:
                queue.append(link)

    return {
        "start_url": normalized_start,
        "page_count": len(pages),
        "pages": pages,
    }


def _prepare_start_url(url: str) -> str:
    candidate = url.strip()
    if not candidate:
        raise SitemapGenerationError("Start URL must not be empty.")

    parsed = urlparse(candidate)
    if not parsed.scheme:
        candidate = "https://" + candidate.lstrip("/")
        parsed = urlparse(candidate)

    if not parsed.netloc:
        raise SitemapGenerationError(f"Invalid URL: {url}")

    return _normalize_url(candidate)


def _normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    normalized = parsed._replace(scheme=scheme, path=path)
    return urlunparse(normalized)


def _process_page(
    session: requests.Session, url: str, domain: str
) -> Tuple[Optional[Dict], Set[str]]:
    try:
        response = session.get(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
    except RequestException:
        return None, set()

    if not _is_html_response(response):
        return None, set()

    soup = BeautifulSoup(response.text, "html.parser")
    text_content = soup.get_text(" ", strip=True)

    headings = _extract_headings(soup)
    keywords = _extract_keywords(soup, text_content)
    images = _extract_images(soup, url)
    outgoing_links = _extract_links(soup, url, domain)

    page_data = {
        "url": url,
        "title": soup.title.get_text(strip=True) if soup.title else None,
        "length": _estimate_length(text_content),
        "section_headings": headings,
        "keywords": keywords,
        "images": images,
        "links": sorted(outgoing_links),
    }

    return page_data, outgoing_links


def _is_html_response(response: Response) -> bool:
    content_type = response.headers.get("Content-Type", "")
    return "text/html" in content_type or "application/xhtml+xml" in content_type


def _estimate_length(text_content: str) -> int:
    words = re.findall(r"\w+", text_content)
    return len(words)


def _extract_headings(soup: BeautifulSoup) -> List[str]:
    headings: List[str] = []
    for heading in soup.find_all(re.compile(r"^h[1-6]$", re.IGNORECASE)):
        text = heading.get_text(" ", strip=True)
        if text:
            headings.append(text)
    return headings


def _extract_keywords(soup: BeautifulSoup, text_content: str) -> List[str]:
    meta = soup.find("meta", attrs={"name": re.compile(r"keywords", re.IGNORECASE)})
    if meta and meta.get("content"):
        keywords = [word.strip() for word in meta["content"].split(",")]
        return [kw for kw in keywords if kw]

    words = re.findall(r"[a-zA-Z]{%d,}" % MIN_KEYWORD_LENGTH, text_content.lower())
    if not words:
        return []
    counter = Counter(words)
    return [word for word, _ in counter.most_common(MAX_KEYWORDS)]


def _extract_images(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Optional[str]]]:
    images: List[Dict[str, Optional[str]]] = []
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src or src.startswith("data:"):
            continue
        absolute_src = urljoin(base_url, src)
        images.append({"src": absolute_src, "alt": img.get("alt") or None})
    return images


def _extract_links(soup: BeautifulSoup, base_url: str, domain: str) -> Set[str]:
    links: Set[str] = set()
    for anchor in soup.find_all("a", href=True):
        candidate = anchor["href"].strip()
        normalized = _normalize_candidate_link(candidate, base_url, domain)
        if normalized:
            links.add(normalized)
    return links


def _normalize_candidate_link(href: str, base_url: str, domain: str) -> Optional[str]:
    if not href or href.startswith("#"):
        return None
    if any(href.lower().startswith(prefix) for prefix in ("mailto:", "tel:", "javascript:", "data:")):
        return None

    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)

    if parsed.scheme not in ALLOWED_SCHEMES or not parsed.netloc:
        return None
    if parsed.netloc != domain:
        return None

    return _normalize_url(absolute)
