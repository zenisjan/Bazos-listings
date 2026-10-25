"""
Bazos.cz Scraper - Comprehensive scraping tool for bazos.cz classified ads.

This Actor scrapes listings from bazos.cz across multiple categories and extracts
detailed information from individual listing pages with full pagination support.
Now includes PostgreSQL database integration.
"""

from __future__ import annotations

import os
import asyncio
import copy
import math
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta, timezone

from apify import Actor
from bs4 import BeautifulSoup
from httpx import AsyncClient, HTTPStatusError, TransportError

# Import our database manager - handle different import paths
try:
    from .database import db_manager
except ImportError:
    try:
        from database import db_manager
    except ImportError:
        # Add current directory to path if needed
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        from database import db_manager

# Category mapping for Bazos.cz subdomains
CATEGORY_DOMAINS = {
    "auto": "auto.bazos.cz",
    "deti": "deti.bazos.cz", 
    "dum": "dum.bazos.cz",
    "elektro": "elektro.bazos.cz",
    "foto": "foto.bazos.cz",
    "hudba": "hudba.bazos.cz",
    "knihy": "knihy.bazos.cz",
    "mobil": "mobil.bazos.cz",
    "motorky": "motorky.bazos.cz",
    "nabytek": "nabytek.bazos.cz",
    "obleceni": "obleceni.bazos.cz", 
    "pc": "pc.bazos.cz",
    "prace": "prace.bazos.cz",
    "reality": "reality.bazos.cz",
    "sluzby": "sluzby.bazos.cz",
    "sport": "sport.bazos.cz",
    "stroje": "stroje.bazos.cz",
    "vstupenky": "vstupenky.bazos.cz",
    "zvirata": "zvirata.bazos.cz",
    "ostatni": "ostatni.bazos.cz"
}


# Safety margin (seconds) subtracted from the Apify hard timeout so the run can
# stop scraping, flush its data, and mark the actor_run "completed" before the
# platform kills it (which would otherwise leave the run stuck as "running" and
# break sold-detection).
_DEADLINE_MARGIN_SECS = 120


def _run_deadline() -> Optional[datetime]:
    """Return the UTC time to stop scraping by, derived from the Apify timeout.

    Apify exposes the run's hard-timeout instant as an ISO 8601 env var. We stop
    a couple of minutes early so there is time to save and close out cleanly.
    Returns None (no limit) for local runs where the var is absent.
    """
    iso = os.environ.get("APIFY_TIMEOUT_AT") or os.environ.get("ACTOR_TIMEOUT_AT")
    if not iso:
        return None
    try:
        deadline = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    return deadline - timedelta(seconds=_DEADLINE_MARGIN_SECS)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# The Postgres `price` column is INTEGER; a bigger (joke) price can't be stored
# and would fail the whole category's insert, so such values become None.
_PRICE_MAX = 2_147_483_647

# Detail pages are fetched DETAIL_CONCURRENCY at a time, their requests starting at
# least DETAIL_MIN_INTERVAL_SECS apart (<= 4 requests/s). One by one, with a 0.5 s
# pause each, an uncapped search (byt: ~16,000 listings) would outlast the run.
DETAIL_CONCURRENCY = 4
DETAIL_MIN_INTERVAL_SECS = 0.25

# More failed detail pages than this share AND count of a category's fetches means
# a block or an outage, not the odd listing deleted since paging (HTTP 410): the run
# is then incomplete, so its card-only rows (no coordinates) don't replace good data.
DETAIL_FAILURE_SHARE_LIMIT = 0.02
DETAIL_FAILURE_COUNT_LIMIT = 20

# bazos serves at most this many results for one search URL: the page at offset
# 20,000 is a 404 that re-serves the last cards ("Zobrazeno 19981-20000 ... z N").
SITE_RESULT_LIMIT = 20_000
# A search reporting more results than this is split into smaller searches (see
# BazosScraper._scrape_search); the margin absorbs listings added while paging.
SPLIT_THRESHOLD = 19_500
# Most searches (page-1 fetches) one category may spend on splitting.
MAX_SPLIT_SEARCHES = 200
# How far the subcategories' result counts may fall short of the whole search's
# (listings removed between those page-1 fetches) before the split counts as
# leaving listings out.
_SPLIT_COVERAGE_SLACK = 5

# Results header of a list page, e.g. "Zobrazeno 21-40 inzerátů z 15 829".
_RESULTS_RE = re.compile(r'Zobrazeno\s+(\d+)\s*-\s*\d+\s+\w+\s+z\s+(\d[\d\s]*)')


def _slice_label(category: str, search_query: Optional[str]) -> str:
    """Short name of what a run scrapes (e.g. "reality/byt"), for status reasons."""
    return f"{category}/{search_query}" if search_query else category


def _price_range_text(lo: int, hi: Optional[int]) -> str:
    return f"{lo}+ Kč" if hi is None else f"{lo}-{hi} Kč"


def _price_split_point(lo: int, hi: Optional[int]) -> Optional[int]:
    """Price to cut the band [lo, hi] at: [lo, cut] + [cut + 1, hi] (None: can't).

    The geometric middle, as prices span orders of magnitude (rents in thousands,
    sales in millions); an open band is cut below _PRICE_MAX.
    """
    top = _PRICE_MAX if hi is None else hi
    if top <= lo:
        return None
    return min(max(lo, math.isqrt(lo * top)), top - 1)


def _multiline_text(element) -> str:
    """An element's text as a browser shows it: each <br> breaks the line, while
    whitespace in the HTML source (like the newline after every <br/>) is a space."""
    element = copy.copy(element)  # leave the parsed page untouched
    for br in element.find_all('br'):
        br.replace_with('\x00')
    lines = [
        re.sub(r'[ \t\r\n\f]+', ' ', part).strip(' ')
        for part in element.get_text().split('\x00')
    ]
    return re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip('\n')


def _short_error(e: BaseException) -> str:
    """One-line, bounded description of an error, for the run's status message."""
    text = " ".join(str(e).split())
    return f"{type(e).__name__}: {text}"[:200] if text else type(e).__name__


# Bounded exponential backoff for transient fetch failures (timeouts, resets,
# 429/5xx). A single flaky response used to abort an entire category scrape.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


async def _request_with_retry(client, method, url, attempts=3, give_up=None, **kwargs):
    """Issue a request, retrying transient failures with exponential backoff.

    Returns the final response without raising for status (callers keep their
    own raise_for_status()); re-raises the last transport error if the
    connection itself keeps failing. No retry once `give_up()` is true (the
    run's time budget is spent: the margin is for saving, not for retries).
    """
    for attempt in range(1, attempts + 1):
        try:
            response = await client.request(method, url, **kwargs)
        except TransportError as e:
            if attempt == attempts or (give_up is not None and give_up()):
                raise
            reason = repr(e)
        else:
            if (
                response.status_code not in _RETRYABLE_STATUS
                or attempt == attempts
                or (give_up is not None and give_up())
            ):
                return response
            reason = f"HTTP {response.status_code}"
        delay = 2.0 * (2 ** (attempt - 1))
        Actor.log.warning(
            f"Transient fetch failure ({reason}); retry {attempt}/{attempts - 1} "
            f"in {delay:.0f}s: {url}"
        )
        await asyncio.sleep(delay)


class _FailRunOnCrash:
    """Marks the actor_runs row 'failed' if the run dies with an unhandled error.

    Historically a terminal status was written only on the happy path, so a crash
    (or an Apify hard-timeout kill) left the row 'running' forever and the web app
    could not tell a dead run from a live one — sold-detection then mistreated the
    run's listings. Used as `async with Actor, _FailRunOnCrash():` so it exits
    before Actor; the exception still propagates and fails the platform run too.
    """

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            try:
                Actor.log.error(f"Run failed with unhandled error: {exc!r}")
                db_manager.update_actor_run_status("failed", 0)
                db_manager.close_pool()
            except Exception as finalize_error:
                Actor.log.error(f"Could not mark run failed: {finalize_error}")
        return False


@dataclass
class _Search:
    """One category's search as configured, and the listings collected for it."""
    category: str
    base_url: str
    label: str
    query: Optional[str]
    location: Optional[str]
    price_min: Optional[int]
    price_max: Optional[int]
    max_listings: int
    listings: List[Dict[str, Any]] = field(default_factory=list)

    def capped(self) -> bool:
        return self.max_listings > 0 and len(self.listings) >= self.max_listings


@dataclass
class _Paged:
    """How paging one search URL went."""
    total: Optional[int] = None  # result count its first page reported
    # (response, soup) of page 1 when it stopped there to have the search split
    first_page: Optional[tuple] = None
    at_site_limit: bool = False  # stopped where bazos stops serving results


class BazosScraper:
    """Main scraper class for Bazos.cz listings with enhanced pagination."""

    def __init__(self, client: AsyncClient):
        self.client = client
        self.scraped_listings = set()
        # UTC instant to stop scraping by (set from the Apify timeout in main()).
        self.deadline: Optional[datetime] = None
        # Why this run may have missed listings; any entry fails the run (main()).
        self.incomplete_reasons: list[str] = []
        # Detail pages that failed (those listings are still saved from their card).
        self.detail_failures = 0
        self._last_detail_error = ""
        # Event-loop time the next detail request may start (see DETAIL_*).
        self._next_detail_start = 0.0
        # Per category: list pages scraped, and searches left for splitting.
        self._pages_scraped = 0
        self._split_searches_left = MAX_SPLIT_SEARCHES

    def _past_deadline(self) -> bool:
        return self.deadline is not None and _now_utc() >= self.deadline

    async def scrape_category_listings(
        self,
        category: str,
        max_listings: int = 0,
        search_query: Optional[str] = None,
        location: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Scrape a category's search results, every page of them (0 = no cap)."""

        domain = CATEGORY_DOMAINS.get(category, "www.bazos.cz")
        search = _Search(
            category=category,
            base_url=f"https://{domain}",
            label=_slice_label(category, search_query),
            query=search_query,
            location=location,
            price_min=price_min or None,
            price_max=price_max or None,
            max_listings=max_listings,
        )
        self._pages_scraped = 0
        self._split_searches_left = MAX_SPLIT_SEARCHES

        Actor.log.info(f"Starting to scrape category: {category}")
        await self._scrape_search(search, '/', search.price_min, search.price_max)
        Actor.log.info(
            f"Scraped {len(search.listings)} listings from category {category} "
            f"across {self._pages_scraped} pages"
        )
        return search.listings

    async def _scrape_search(
        self, search: _Search, path: str, lo: Optional[int], hi: Optional[int]
    ) -> None:
        """Scrape one search: a subcategory `path` and price band [lo, hi] of `search`.

        bazos serves only the first SITE_RESULT_LIMIT results of a search, so one
        that reports more (on its page 1) is split instead of paged: into its
        subcategories ("/prodam/byt/", ...), which partition it including the
        listings without a price ("Dohodou", "V textu") that no price filter or
        price sort ever returns; or, if it is price-filtered anyway, into two price
        bands. One that can't be split is scraped as far as bazos serves it and the
        run is recorded as incomplete.
        """
        name = self._search_name(search, path, lo, hi)
        self._split_searches_left -= 1
        may_split = self._split_searches_left > 0 and (
            search.max_listings == 0
            or search.max_listings - len(search.listings) > SPLIT_THRESHOLD
        )
        paged = await self._scrape_pages(
            search, path, lo, hi, name,
            split_above=SPLIT_THRESHOLD if may_split else None,
        )
        if paged.first_page is None:
            if paged.at_site_limit:
                self.incomplete_reasons.append(
                    f"{name}: more than {SITE_RESULT_LIMIT} results; bazos serves "
                    f"no more (stopped at offset {SITE_RESULT_LIMIT})"
                )
            return

        subpaths = self._subcategory_paths(paged.first_page[1]) if path == '/' else []
        if subpaths:
            await self._split_by_subcategory(search, subpaths, lo, hi, name, paged.total)
            return
        if lo is not None or hi is not None:
            cut = _price_split_point(lo or 1, hi)
            if cut is not None:
                await self._split_by_price(search, path, lo or 1, cut, hi, name)
                return
        self.incomplete_reasons.append(
            f"{name}: {paged.total} results and no way to split them; bazos serves "
            f"only the newest {SITE_RESULT_LIMIT} of a search"
        )
        await self._scrape_pages(search, path, lo, hi, name, first_page=paged.first_page)

    async def _split_by_subcategory(
        self,
        search: _Search,
        subpaths: List[str],
        lo: Optional[int],
        hi: Optional[int],
        name: str,
        total: Optional[int],
    ) -> None:
        """Scrape a too-big search subcategory by subcategory.

        First reads every subcategory's result count, so a partition that misses
        listings (they add up to less than the whole) is caught.
        """
        Actor.log.info(
            f"{name}: {total} results, more than bazos serves for one search; "
            f"splitting it into {len(subpaths)} subcategories"
        )
        totals: Dict[str, Optional[int]] = {}
        for sub in subpaths:
            if self._past_deadline():
                self.incomplete_reasons.append(
                    f"{name}: run time budget reached while splitting it by subcategory"
                )
                return
            await asyncio.sleep(1)
            self._split_searches_left -= 1
            totals[sub] = await self._probe_total(search, sub, lo, hi)
        covered = sum(t for t in totals.values() if t)
        Actor.log.info(f"{name}: subcategories hold {covered} of {total} results")
        if total is not None and covered + _SPLIT_COVERAGE_SLACK < total:
            self.incomplete_reasons.append(
                f"{name}: its subcategories hold only {covered} of its {total} results"
            )
        todo = [sub for sub, t in totals.items() if t != 0]  # None: count unknown
        for i, sub in enumerate(todo):
            if search.capped():
                break
            if self._past_deadline():
                self.incomplete_reasons.append(
                    f"{name}: run time budget reached; {len(todo) - i} of "
                    f"{len(todo)} subcategories not scraped"
                )
                break
            await asyncio.sleep(1)
            await self._scrape_search(search, sub, lo, hi)

    async def _split_by_price(
        self, search: _Search, path: str, lo: int, cut: int, hi: Optional[int], name: str
    ) -> None:
        """Scrape a too-big price-filtered search as the bands [lo, cut] + [cut+1, hi].

        (bazos's price bounds are inclusive. Listings without a price match no
        price filter, so this split is only used where a price filter is set.)
        """
        Actor.log.info(
            f"{name}: more results than bazos serves for one search; "
            f"splitting it at {cut} Kč"
        )
        for band_lo, band_hi in ((lo, cut), (cut + 1, hi)):
            if search.capped():
                break
            if self._past_deadline():
                self.incomplete_reasons.append(
                    f"{name}: run time budget reached; price band "
                    f"{_price_range_text(band_lo, band_hi)} not scraped"
                )
                break
            await asyncio.sleep(1)
            await self._scrape_search(search, path, band_lo, band_hi)

    async def _scrape_pages(
        self,
        search: _Search,
        path: str,
        lo: Optional[int],
        hi: Optional[int],
        name: str,
        split_above: Optional[int] = None,
        first_page: Optional[tuple] = None,
    ) -> _Paged:
        """Page one search URL, newest first, adding unseen listings to the search.

        Stops after page 1 (keeping it in the result) if that reports more than
        `split_above` results, and before the page where bazos stops serving
        results; any early stop for another reason is an incomplete reason.
        """
        result = _Paged()
        page_offset = 0
        page_number = 1

        while True:
            if self._past_deadline():
                Actor.log.warning(
                    f"Run time budget reached while paging {name}; "
                    f"stopping at {len(search.listings)} listings"
                )
                self.incomplete_reasons.append(
                    f"{name}: run time budget reached while paging "
                    f"(stopped at {len(search.listings)} listings)"
                )
                break

            url = self._build_search_url(
                search.base_url, page_offset, search.query, search.location, lo, hi,
                path=path,
            )

            try:
                if first_page is not None:
                    response, soup = first_page
                    first_page = None
                else:
                    Actor.log.info(f"Scraping page {page_number} (offset {page_offset}): {url}")
                    response = await _request_with_retry(
                        self.client, "GET", url, follow_redirects=True, give_up=self._past_deadline
                    )
                    soup = BeautifulSoup(response.content, 'lxml')

                # Checked before raise_for_status(): bazos serves a page past the
                # last result as a 404 that still renders the list header.
                if self._is_end_of_results(soup):
                    Actor.log.info(f"No more listings found for {name} on page {page_number}")
                    break

                response.raise_for_status()

                if page_number == 1:
                    result.total = self._reported_total(soup)
                    if split_above is not None and (result.total or 0) > split_above:
                        result.first_page = (response, soup)
                        break

                page_listings = self._extract_listings_from_page(soup, search.category, search.base_url)

                if not page_listings and not soup.find('div', class_='inzeraty inzeratyflex'):
                    # Not bazos's end-of-results page, so this is a block/consent
                    # page or a layout change, not the end of the list.
                    Actor.log.error(f"No parsable listings on {url}")
                    self.incomplete_reasons.append(
                        f"{name}: page {page_number} had no parsable listings"
                    )
                    break
                # (Cards that were all collected already — the list shifted while
                # paging — just yield nothing new; carry on to the next page.)

                search.listings.extend(page_listings)
                self._pages_scraped += 1

                Actor.log.info(f"Extracted {len(page_listings)} listings from page {page_number} (total so far: {len(search.listings)})")

                if search.capped():
                    del search.listings[search.max_listings:]
                    Actor.log.info(f"Reached maximum listings limit ({search.max_listings})")
                    break

                has_next, next_offset = self._check_next_page(soup, page_offset)
                if not has_next:
                    Actor.log.info(f"No next page found after page {page_number}")
                    break
                if next_offset >= SITE_RESULT_LIMIT:
                    result.at_site_limit = True
                    break

                page_offset = next_offset
                page_number += 1

                # Rate limiting between pages
                await asyncio.sleep(1)

            except Exception as e:
                self._record_page_failure(name, page_number, url, e)
                break

        return result

    async def _probe_total(
        self, search: _Search, path: str, lo: Optional[int], hi: Optional[int]
    ) -> Optional[int]:
        """Result count of a search's page 1 (0: no results; None: page failed)."""
        name = self._search_name(search, path, lo, hi)
        url = self._build_search_url(
            search.base_url, 0, search.query, search.location, lo, hi, path=path
        )
        Actor.log.info(f"Reading the result count of {url}")
        try:
            response = await _request_with_retry(
                self.client, "GET", url, follow_redirects=True, give_up=self._past_deadline
            )
            soup = BeautifulSoup(response.content, 'lxml')
            if self._is_end_of_results(soup):
                return 0
            response.raise_for_status()
        except Exception as e:
            self._record_page_failure(name, 1, url, e)
            return None
        return self._reported_total(soup)

    def _record_page_failure(self, name: str, page_number: int, url: str, e: Exception) -> None:
        if isinstance(e, HTTPStatusError):
            Actor.log.error(f"HTTP error while scraping {url}: {e}")
            self.incomplete_reasons.append(
                f"{name}: page {page_number} returned HTTP {e.response.status_code}"
            )
        else:
            Actor.log.error(f"Error while scraping {url}: {e}")
            self.incomplete_reasons.append(
                f"{name}: page {page_number} failed ({_short_error(e)})"
            )

    @staticmethod
    def _search_name(search: _Search, path: str, lo: Optional[int], hi: Optional[int]) -> str:
        """`search`'s label, plus the subcategory / price band of a split part of it."""
        parts = []
        if path != '/':
            parts.append(path.strip('/'))
        if (lo, hi) != (search.price_min, search.price_max):
            parts.append(_price_range_text(lo or 1, hi))
        return f"{search.label} [{', '.join(parts)}]" if parts else search.label

    def _build_search_url(
        self,
        base_url: str,
        page_offset: int,
        search_query: Optional[str] = None,
        location: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None,
        path: str = '/',
    ) -> str:
        """Build search URL with filters and pagination (`path`: a subcategory)."""

        if page_offset == 0:
            url = f"{base_url}{path}"
        else:
            url = f"{base_url}{path}{page_offset}/"

        params = []

        if search_query:
            params.append(f"hledat={search_query}")
        if location:
            params.append(f"hlokalita={location}")
        if price_min:
            params.append(f"cenaod={price_min}")
        if price_max:
            params.append(f"cenado={price_max}")

        if params:
            url += "?" + "&".join(params)

        return url
    
    def _extract_listings_from_page(self, soup: BeautifulSoup, category: str, base_url: str) -> List[Dict[str, Any]]:
        """Extract listing data from a page."""
        
        listings = []
        listing_containers = soup.find_all('div', class_='inzeraty inzeratyflex')
        
        for container in listing_containers:
            try:
                listing = self._extract_listing_data(container, category, base_url)
                if listing and listing['id'] not in self.scraped_listings:
                    listings.append(listing)
                    self.scraped_listings.add(listing['id'])
            except Exception as e:
                Actor.log.warning(f"Error extracting listing: {e}")
                continue
                
        return listings
    
    def _extract_listing_data(self, container: BeautifulSoup, category: str, base_url: str) -> Optional[Dict[str, Any]]:
        """Extract data from a single listing container."""
        
        # Title and URL
        title_link = container.find('h2', class_='nadpis')
        if not title_link:
            return None
            
        title_a = title_link.find('a')
        if not title_a:
            return None
            
        title = title_a.get_text(strip=True)
        relative_url = title_a.get('href')
        url = urljoin(base_url, relative_url)
        
        # Extract listing ID from URL
        listing_id = self._extract_listing_id(url)
        
        # Image
        image_container = container.find('div', class_='inzeratynadpis')
        image_url = None
        if image_container:
            img = image_container.find('img', class_='obrazek')
            if img:
                image_url = img.get('src')
                if image_url and not image_url.startswith('http'):
                    image_url = urljoin(base_url, image_url)
        
        # Description
        description_div = container.find('div', class_='popis')
        description = description_div.get_text(strip=True) if description_div else ""
        
        # Price
        price_div = container.find('div', class_='inzeratycena')
        price_text = price_div.get_text(strip=True) if price_div else ""
        price = self._extract_price(price_text)
        
        # Location ("Brno<br>625 00" -> "Brno 625 00")
        location_div = container.find('div', class_='inzeratylok')
        location = location_div.get_text(' ', strip=True) if location_div else ""
        
        # Views
        views_div = container.find('div', class_='inzeratyview')
        views_text = views_div.get_text(strip=True) if views_div else ""
        views = self._extract_views(views_text)
        
        # Date/TOP status
        date_span = container.find('span', class_='velikost10')
        date_info = date_span.get_text(strip=True) if date_span else ""
        is_top = 'TOP' in date_info
        date = self._extract_date(date_info)
        
        return {
            'id': listing_id,
            'title': title,
            'url': url,
            'category': category,
            'price': price,
            'price_text': price_text,
            'description': description,
            'location': location,
            'views': views,
            'date': date,
            'is_top': is_top,
            'image_url': image_url,
            'scraped_at': datetime.now().isoformat()
        }
    
    async def scrape_details(self, label: str, listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Merge detail-page data into a category's listings, keeping their order.

        Up to DETAIL_CONCURRENCY detail pages are in flight at once. Once the run's
        time budget is spent the remaining listings keep their card data, so we
        still save what we have, and the run is recorded as incomplete.
        """
        semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)
        failures_before = self.detail_failures
        done = 0
        skipped = 0

        async def enrich(listing: Dict[str, Any]) -> Dict[str, Any]:
            nonlocal done, skipped
            async with semaphore:
                # Checked on both sides of the pacing wait: no request starts past
                # the deadline, and the listings it cuts off wait for nothing.
                if not self._past_deadline():
                    await self._pace_detail_request()
                if self._past_deadline():
                    skipped += 1
                    return listing
                detailed = await self.scrape_detailed_data(listing)
            done += 1
            if done % 100 == 0:
                Actor.log.info(f"Scraped detailed data for {done}/{len(listings)} listings")
            return detailed

        detailed_listings = list(await asyncio.gather(*(enrich(item) for item in listings)))

        if skipped:
            Actor.log.warning(
                f"Run time budget reached mid-detail; keeping "
                f"{len(listings) - skipped} enriched + {skipped} card-only"
            )
            self.incomplete_reasons.append(
                f"{label}: run time budget reached; {skipped} listings saved "
                f"without detail data"
            )
        failed = self.detail_failures - failures_before
        fetched = len(listings) - skipped
        if failed > DETAIL_FAILURE_COUNT_LIMIT and failed > DETAIL_FAILURE_SHARE_LIMIT * fetched:
            self.incomplete_reasons.append(
                f"{label}: {failed} of {fetched} detail pages failed (last: "
                f"{self._last_detail_error}); saved without detail data"
            )
        return detailed_listings

    async def _pace_detail_request(self) -> None:
        """Wait for this detail request's start slot, DETAIL_MIN_INTERVAL_SECS apart."""
        loop = asyncio.get_running_loop()
        start = max(loop.time(), self._next_detail_start)
        self._next_detail_start = start + DETAIL_MIN_INTERVAL_SECS
        await asyncio.sleep(start - loop.time())

    async def scrape_detailed_data(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch one listing's detail page (retrying transient failures) and merge it.

        If it still fails (e.g. HTTP 410: deleted since it was paged) the listing
        keeps its card data and the failure is counted.
        """
        try:
            response = await _request_with_retry(
                self.client, "GET", listing['url'], follow_redirects=True,
                give_up=self._past_deadline,
            )
            response.raise_for_status()
            details = self._extract_detailed_data(BeautifulSoup(response.content, 'lxml'))
            if not details:
                # A block or error page served as 200, not a listing.
                raise ValueError("no listing data on the detail page")
        except Exception as e:
            self.detail_failures += 1
            self._last_detail_error = (
                f"HTTP {e.response.status_code}" if isinstance(e, HTTPStatusError)
                else _short_error(e)
            )
            Actor.log.warning(f"Error scraping detailed data for {listing['url']}: {e}")
            return listing

        Actor.log.debug(f"Scraped detailed data for listing {listing['id']}")
        return {**listing, **details}

    def _extract_detailed_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed information from listing page."""

        details = {}

        # Full description, line breaks kept ("…řádek.<br/>\nDalší…")
        desc_div = soup.find('div', class_='popisdetail')
        if desc_div:
            details['full_description'] = _multiline_text(desc_div)
        
        # Contact information
        contact_table = soup.find('table', width="100%")
        if contact_table:
            # Extract contact name. The label cell is "Jméno:<br/><br/>" (so
            # td.string is None); match its text instead, skipping the outer cell
            # that wraps the nested contact table (its text starts with it too).
            name_cell = contact_table.find(
                lambda tag: tag.name == 'td'
                and tag.find('td') is None
                and tag.get_text(strip=True).startswith('Jméno')
            )
            if name_cell and name_cell.find_next_sibling('td'):
                contact_name = name_cell.find_next_sibling('td').get_text(strip=True)
                details['contact_name'] = contact_name
            
            # Extract phone (if revealed)
            phone_cell = contact_table.find('td', string=re.compile(r'Telefon:'))
            if phone_cell and phone_cell.find_next_sibling('td'):
                phone_text = phone_cell.find_next_sibling('td').get_text(strip=True)
                if not 'zobraz číslo' in phone_text:
                    details['phone'] = phone_text
            
            # Extract exact location coordinates if available
            location_link = contact_table.find('a', href=re.compile(r'google\.com/maps'))
            if location_link:
                href = location_link.get('href')
                coords = self._extract_coordinates(href)
                if coords:
                    details['coordinates'] = coords
        
        # All images
        carousel = soup.find('div', class_='carousel')
        if carousel:
            images = []
            for img in carousel.find_all('img', class_='carousel-cell-image'):
                img_url = img.get('data-flickity-lazyload') or img.get('src')
                if img_url:
                    images.append(img_url)
            details['images'] = images
        
        # Similar/related listings. A card's first link wraps just its image; the
        # title link is the one in .nadpis.
        similar_section = soup.find('div', class_='podobne')
        if similar_section:
            similar_listings = []
            for listing_div in similar_section.find_all('div', class_='inzeraty inzeratyflex'):
                title_box = listing_div.find(class_='nadpis')
                similar_title_link = (
                    title_box.find('a', href=True) if title_box else None
                ) or listing_div.find('a', href=True)
                if similar_title_link:
                    title = similar_title_link.get_text(strip=True)
                    if not title:
                        img = listing_div.find('img', alt=True)
                        title = img['alt'].strip() if img else ''
                    similar_listings.append({
                        'title': title,
                        'url': similar_title_link.get('href')
                    })
            details['similar_listings'] = similar_listings
        
        return details
    
    def _extract_listing_id(self, url: str) -> str:
        """Extract listing ID from URL."""
        match = re.search(r'/inzerat/(\d+)/', url)
        return match.group(1) if match else ""
    
    def _extract_price(self, price_text: str) -> Optional[int]:
        """Extract numeric price from price text."""
        if not price_text:
            return None
        
        # Remove common Czech price indicators and extract numbers
        price_match = re.search(r'([\d\s]+)', price_text.replace(' ', ''))
        if price_match:
            try:
                price = int(price_match.group(1).replace(' ', ''))
                return price if price <= _PRICE_MAX else None
            except ValueError:
                pass
        return None
    
    def _extract_views(self, views_text: str) -> int:
        """Extract view count from views text."""
        match = re.search(r'(\d+)', views_text)
        return int(match.group(1)) if match else 0
    
    def _extract_date(self, date_info: str) -> str:
        """Extract date from date info string."""
        # Look for date pattern like [15.9. 2025]
        date_match = re.search(r'\[([^\]]+)\]', date_info)
        return date_match.group(1) if date_match else ""
    
    def _extract_coordinates(self, maps_url: str) -> Optional[Dict[str, float]]:
        """Extract coordinates from Google Maps URL."""
        coord_match = re.search(r'place/([0-9.-]+),([0-9.-]+)', maps_url)
        if coord_match:
            return {
                'latitude': float(coord_match.group(1)),
                'longitude': float(coord_match.group(2))
            }
        return None
    
    def _is_end_of_results(self, soup: BeautifulSoup) -> bool:
        """True if this is bazos's own "nothing (more) to show" list page.

        An empty search says "Žádný inzerát nenalezen"; a page past the last
        result (served as 404) says e.g. "Zobrazeno 821-806 inzerátů z 806".
        Any other page without listings (block page, layout change) is not.
        """
        if soup.find('div', class_='inzeraty inzeratyflex'):
            return False
        header = soup.find('div', class_='listainzerat')
        if not header:
            return False
        text = header.get_text(' ', strip=True)
        if 'Žádný inzerát nenalezen' in text:
            return True
        match = _RESULTS_RE.search(text)
        return bool(match) and int(match.group(1)) > int(re.sub(r'\D', '', match.group(2)))

    def _reported_total(self, soup: BeautifulSoup) -> Optional[int]:
        """Result count from a list page's header ("Zobrazeno 1-20 inzerátů z 15 829")."""
        header = soup.find('div', class_='listainzerat')
        match = _RESULTS_RE.search(header.get_text(' ', strip=True)) if header else None
        return int(re.sub(r'\D', '', match.group(2))) if match else None

    def _subcategory_paths(self, soup: BeautifulSoup) -> List[str]:
        """The category's subcategories ("/prodam/byt/", ...) from a list page's menu.

        Same-site links only: a menu entry that points at another bazos section
        (e.g. "Ubytování" -> sluzby.bazos.cz) is a different category.
        """
        paths: List[str] = []
        for link in soup.select('div.menuleft div.barvaleva a[href]'):
            href = link['href']
            if re.fullmatch(r'(?:/[\w-]+)+/', href) and href not in paths:
                paths.append(href)
        return paths

    def _check_next_page(self, soup: BeautifulSoup, current_offset: int) -> tuple[bool, int]:
        """Enhanced method to check for next page and return next offset."""
        
        # Method 1: Check pagination section
        pagination = soup.find('div', class_='strankovani')
        if pagination:
            # Look for "Další" (Next) link
            next_link = pagination.find('a', string=re.compile(r'Další|Next'))
            if next_link:
                href = next_link.get('href', '')
                # Extract offset from URL like "/20/" or "/40/"
                offset_match = re.search(r'/(\d+)/', href)
                if offset_match:
                    next_offset = int(offset_match.group(1))
                    return True, next_offset
            
            # Look for numbered page links
            page_links = pagination.find_all('a')
            for link in page_links:
                href = link.get('href', '')
                offset_match = re.search(r'/(\d+)/', href)
                if offset_match:
                    offset = int(offset_match.group(1))
                    if offset > current_offset:
                        return True, offset
        
        # Method 2: Check if we have listings on current page
        # If we have 20 listings (full page), there might be more
        listing_containers = soup.find_all('div', class_='inzeraty inzeratyflex')
        if len(listing_containers) >= 20:  # Full page typically has 20 listings
            next_offset = current_offset + 20
            return True, next_offset
        
        # Method 3: the "Zobrazeno 1-20 inzerátů z 15 829" header (the total has
        # thousands separators)
        total_listings = self._reported_total(soup)
        if total_listings is not None and current_offset + 20 < total_listings:
            return True, current_offset + 20

        return False, current_offset


async def main() -> None:
    """Main entry point for the Bazos.cz scraper with enhanced pagination and database integration."""
    
    async with Actor, _FailRunOnCrash():
        # Get input configuration
        actor_input = await Actor.get_input() or {}
        
        # Extract configuration with proper defaults
        # Handle categories - use provided categories or default to ['auto']
        categories = actor_input.get('categories')
        if not categories or len(categories) == 0:
            categories = ['auto']
        
        max_listings = actor_input.get('maxListings', 100)
        include_detailed_data = actor_input.get('includeDetailedData', True)
        search_query = actor_input.get('searchQuery')
        location = actor_input.get('location')
        price_min = actor_input.get('priceMin')
        price_max = actor_input.get('priceMax')
        
        # Get actor run information
        actor_run_id = os.environ.get('APIFY_ACTOR_RUN_ID') or os.environ.get('ACTOR_RUN_ID')
        if not actor_run_id:
            actor_run_id = f"local-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        actor_run_start = datetime.now()
        
        Actor.log.info(f"Starting Bazos.cz scraper with categories: {categories}")
        Actor.log.info(f"Max listings per category: {max_listings} (0 = unlimited)")
        Actor.log.info(f"Include detailed data: {include_detailed_data}")
        Actor.log.info(f"Actor Run ID: {actor_run_id}")
        if search_query:
            Actor.log.info(f"Search query: {search_query}")
        if location:
            Actor.log.info(f"Location filter: {location}")
        if price_min or price_max:
            Actor.log.info(f"Price range: {price_min or 0} - {price_max or 'unlimited'} CZK")
        
        # Initialize database connection
        try:
            # Get scraper name from environment or use default
            scraper_name = os.environ.get('SCRAPER_NAME', 'bazos_scraper')
            db_manager.scraper_name = scraper_name
            
            db_manager.initialize_pool()
            db_manager.set_actor_run_info(actor_run_id, actor_run_start)
            
            # Create actor run record in database
            db_manager.create_actor_run(
                categories=categories,
                max_listings=max_listings,
                search_query=search_query,
                location=location,
                price_min=price_min,
                price_max=price_max
            )
            
            Actor.log.info("Database connection established and actor run created")
            db_manager_available = True
            
        except Exception as e:
            Actor.log.error(f"Failed to initialize database: {e}")
            Actor.log.warning("Continuing without database integration")
            db_manager_available = False
        
        # Create HTTP client with proper headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'cs,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        async with AsyncClient(headers=headers, timeout=30.0) as client:
            scraper = BazosScraper(client)
            scraper.deadline = _run_deadline()
            if scraper.deadline:
                Actor.log.info(f"Run time budget until (UTC): {scraper.deadline.isoformat()}")

            all_listings = []
            total_pages_scraped = 0
            saved_count = 0  # rows actually inserted into the database
            # Shared with the scraper, which adds its paging failures here.
            incomplete_reasons = scraper.incomplete_reasons
            
            # Scrape each category
            for category_index, category in enumerate(categories):
                if scraper._past_deadline():
                    Actor.log.warning("Run time budget reached; stopping before remaining categories")
                    incomplete_reasons.append(
                        f"run time budget reached; skipped categories: "
                        f"{', '.join(categories[category_index:])}"
                    )
                    break
                if category not in CATEGORY_DOMAINS:
                    Actor.log.warning(f"Unknown category: {category}, skipping")
                    continue
                label = _slice_label(category, search_query)
                
                try:
                    Actor.log.info(f"=== Starting category: {category} ===")
                    
                    # Refresh database connection pool every few categories for long runs
                    if db_manager_available and category_index > 0 and category_index % 3 == 0:
                        try:
                            Actor.log.info("Refreshing database connection pool for long-running operation")
                            db_manager.refresh_pool()
                        except Exception as e:
                            Actor.log.warning(f"Failed to refresh connection pool: {e}")
                    
                    category_listings = await scraper.scrape_category_listings(
                        category=category,
                        max_listings=max_listings,
                        search_query=search_query,
                        location=location,
                        price_min=price_min,
                        price_max=price_max
                    )
                    
                    # Scrape detailed data if requested (coordinates come only
                    # from the detail page)
                    if include_detailed_data and category_listings:
                        Actor.log.info(f"Scraping detailed data for {len(category_listings)} listings from {category}")
                        category_listings = await scraper.scrape_details(label, category_listings)
                    
                    all_listings.extend(category_listings)
                    
                    # Save data to both Apify dataset and database
                    if category_listings:
                        # Save to Apify dataset
                        await Actor.push_data(category_listings)
                        Actor.log.info(f"Saved {len(category_listings)} listings to Apify dataset from category {category}")
                        
                        # Save to database if available
                        if db_manager_available:
                            try:
                                # Check connection health before inserting
                                Actor.log.debug("Inserting listings to database...")
                                db_manager.insert_listings(category_listings)
                                saved_count += len(category_listings)
                                Actor.log.info(f"Saved {len(category_listings)} listings to database from category {category}")
                            except Exception as e:
                                Actor.log.error(f"Failed to save listings to database: {e}")
                                # Try to refresh the connection pool and retry once
                                try:
                                    Actor.log.info("Attempting to refresh connection pool and retry database operation")
                                    db_manager.refresh_pool()
                                    db_manager.insert_listings(category_listings)
                                    saved_count += len(category_listings)
                                    Actor.log.info(f"Successfully saved {len(category_listings)} listings to database after retry")
                                except Exception as retry_e:
                                    Actor.log.error(f"Failed to save listings to database even after retry: {retry_e}")
                                    incomplete_reasons.append(
                                        f"{label}: database save failed after retry ({_short_error(retry_e)})"
                                    )
                    
                    Actor.log.info(f"=== Completed category: {category} ===")
                    
                except Exception as e:
                    Actor.log.error(f"Error scraping category {category}: {e}")
                    incomplete_reasons.append(f"{label}: category failed ({_short_error(e)})")
                    continue
            
            # Final summary and database cleanup
            Actor.log.info(f"=== SCRAPING COMPLETED ===")
            Actor.log.info(f"Total listings scraped: {len(all_listings)}")
            Actor.log.info(f"Listings saved to database: {saved_count}")
            Actor.log.info(f"Categories processed: {len(categories)}")
            if include_detailed_data:
                Actor.log.info(f"Detail pages failed (saved from their card): {scraper.detail_failures}")
            
            # Zero listings almost always means upstream breakage (site change,
            # consent wall, block) rather than an empty market — mark the run
            # 'failed' so sold-detection ignores it instead of flagging every
            # previously-seen listing as sold. A run that stopped early is
            # 'failed' for the same reason: every listing it didn't reach would
            # otherwise look sold.
            run_status = 'failed' if incomplete_reasons or not saved_count else 'completed'
            for reason in incomplete_reasons:
                Actor.log.error(f"Run incomplete — {reason}")

            # Update actor run status in database
            if db_manager_available:
                try:
                    db_manager.update_actor_run_status(run_status, saved_count)
                    db_manager.close_pool()
                    Actor.log.info("Database connection closed successfully")
                except Exception as e:
                    Actor.log.error(f"Failed to update actor run status: {e}")
            
            # Set status message (an API call on the platform, so it must never
            # break the run after the DB status has been written)
            if incomplete_reasons:
                more = f" (+{len(incomplete_reasons) - 1} more)" if len(incomplete_reasons) > 1 else ""
                status_message = f"Stopped early: {incomplete_reasons[0]}{more}"
            elif not saved_count:
                status_message = f"Failed: no listings saved to the database ({len(all_listings)} scraped)"
            else:
                status_message = f"Completed: {len(all_listings)} listings scraped from {len(categories)} categories"
            try:
                await Actor.set_status_message(status_message)
            except Exception as e:
                Actor.log.warning(f"Could not set the run's status message: {e}")


if __name__ == '__main__':
    asyncio.run(main())
