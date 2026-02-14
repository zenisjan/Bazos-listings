"""
Bazos.cz Scraper - Comprehensive scraping tool for bazos.cz classified ads.

This Actor scrapes listings from bazos.cz across multiple categories and extracts
detailed information from individual listing pages with full pagination support.
Now includes PostgreSQL database integration.
"""

from __future__ import annotations

import os
import asyncio
import re
import sys
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime

from apify import Actor
from bs4 import BeautifulSoup
from httpx import AsyncClient, HTTPStatusError

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


class BazosScraper:
    """Main scraper class for Bazos.cz listings with enhanced pagination."""
    
    def __init__(self, client: AsyncClient):
        self.client = client
        self.scraped_listings = set()
        
    async def scrape_category_listings(
        self, 
        category: str,
        max_listings: int = 0,
        search_query: Optional[str] = None,
        location: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Scrape listings from a specific category with full pagination support."""
        
        domain = CATEGORY_DOMAINS.get(category, "www.bazos.cz")
        base_url = f"https://{domain}"
        
        listings = []
        page_offset = 0
        page_number = 1
        total_pages_scraped = 0
        
        Actor.log.info(f"Starting to scrape category: {category}")
        
        while True:
            # Build URL with filters and pagination
            url = self._build_search_url(
                base_url, page_offset, search_query, location, price_min, price_max
            )
            
            Actor.log.info(f"Scraping page {page_number} (offset {page_offset}): {url}")
            
            try:
                response = await self.client.get(url, follow_redirects=True)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'lxml')
                
                # Extract listings from current page
                page_listings = self._extract_listings_from_page(soup, category, base_url)
                
                if not page_listings:
                    Actor.log.info(f"No more listings found for category {category} on page {page_number}")
                    break
                    
                listings.extend(page_listings)
                total_pages_scraped += 1
                
                Actor.log.info(f"Extracted {len(page_listings)} listings from page {page_number} (total so far: {len(listings)})")
                
                # Check if we've reached the maximum
                if max_listings > 0 and len(listings) >= max_listings:
                    listings = listings[:max_listings]
                    Actor.log.info(f"Reached maximum listings limit ({max_listings}) after {total_pages_scraped} pages")
                    break
                    
                # Check if there's a next page using multiple methods
                has_next, next_offset = self._check_next_page(soup, page_offset)
                if not has_next:
                    Actor.log.info(f"No next page found after page {page_number}")
                    break
                    
                page_offset = next_offset
                page_number += 1
                
                # Rate limiting between pages
                await asyncio.sleep(1)
                
            except HTTPStatusError as e:
                Actor.log.error(f"HTTP error while scraping {url}: {e}")
                break
            except Exception as e:
                Actor.log.error(f"Error while scraping {url}: {e}")
                break
                
        Actor.log.info(f"Scraped {len(listings)} listings from category {category} across {total_pages_scraped} pages")
        return listings
    
    def _build_search_url(
        self,
        base_url: str,
        page_offset: int,
        search_query: Optional[str] = None,
        location: Optional[str] = None,
        price_min: Optional[int] = None,
        price_max: Optional[int] = None
    ) -> str:
        """Build search URL with filters and pagination."""
        
        if page_offset == 0:
            url = f"{base_url}/"
        else:
            url = f"{base_url}/{page_offset}/"
            
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
        
        # Location
        location_div = container.find('div', class_='inzeratylok')
        location = location_div.get_text(strip=True) if location_div else ""
        
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
    
    async def scrape_detailed_data(self, listing: Dict[str, Any]) -> Dict[str, Any]:
        """Scrape detailed data from individual listing page."""
        
        try:
            response = await self.client.get(listing['url'], follow_redirects=True)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Extract additional details
            details = self._extract_detailed_data(soup)
            
            # Merge with existing listing data
            detailed_listing = {**listing, **details}
            
            Actor.log.debug(f"Scraped detailed data for listing {listing['id']}")
            return detailed_listing
            
        except Exception as e:
            Actor.log.warning(f"Error scraping detailed data for {listing['url']}: {e}")
            return listing
    
    def _extract_detailed_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract detailed information from listing page."""
        
        details = {}
        
        # Full description
        desc_div = soup.find('div', class_='popisdetail')
        if desc_div:
            details['full_description'] = desc_div.get_text(strip=True)
        
        # Contact information
        contact_table = soup.find('table', width="100%")
        if contact_table:
            # Extract contact name
            name_cell = contact_table.find('td', string=re.compile(r'Jméno:'))
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
        
        # Similar/related listings
        similar_section = soup.find('div', class_='podobne')
        if similar_section:
            similar_listings = []
            for listing_div in similar_section.find_all('div', class_='inzeraty inzeratyflex'):
                similar_title_link = listing_div.find('a')
                if similar_title_link:
                    similar_listings.append({
                        'title': similar_title_link.get_text(strip=True),
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
                return int(price_match.group(1).replace(' ', ''))
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
        
        # Method 3: Check for "Zobrazeno X-Y inzerátů z Z" pattern
        listainzerat = soup.find('div', class_='listainzerat inzeratyflex')
        if listainzerat:
            text = listainzerat.get_text()
            # Look for pattern like "Zobrazeno 1-20 inzerátů z 421474"
            match = re.search(r'Zobrazeno \d+-\d+ inzerátů z (\d+)', text)
            if match:
                total_listings = int(match.group(1))
                current_end = current_offset + 20
                if current_end < total_listings:
                    return True, current_offset + 20
        
        return False, current_offset


async def main() -> None:
    """Main entry point for the Bazos.cz scraper with enhanced pagination and database integration."""
    
    async with Actor:
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
        actor_run_id = os.environ.get("ACTOR_RUN_ID")
        if not actor_run_id:
            actor_run_id = f"local-{datetime.now().strftime("%Y%m%d-%H%M%S")}"
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
            
            all_listings = []
            total_pages_scraped = 0
            
            # Scrape each category
            for category_index, category in enumerate(categories):
                if category not in CATEGORY_DOMAINS:
                    Actor.log.warning(f"Unknown category: {category}, skipping")
                    continue
                
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
                    
                    # Scrape detailed data if requested
                    if include_detailed_data and category_listings:
                        Actor.log.info(f"Scraping detailed data for {len(category_listings)} listings from {category}")
                        
                        detailed_listings = []
                        for i, listing in enumerate(category_listings):
                            detailed_listing = await scraper.scrape_detailed_data(listing)
                            detailed_listings.append(detailed_listing)
                            
                            # Progress update
                            if (i + 1) % 10 == 0:
                                Actor.log.info(f"Scraped detailed data for {i + 1}/{len(category_listings)} listings")
                            
                            # Rate limiting for detailed scraping
                            await asyncio.sleep(0.5)
                        
                        category_listings = detailed_listings
                    
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
                                Actor.log.info(f"Saved {len(category_listings)} listings to database from category {category}")
                            except Exception as e:
                                Actor.log.error(f"Failed to save listings to database: {e}")
                                # Try to refresh the connection pool and retry once
                                try:
                                    Actor.log.info("Attempting to refresh connection pool and retry database operation")
                                    db_manager.refresh_pool()
                                    db_manager.insert_listings(category_listings)
                                    Actor.log.info(f"Successfully saved {len(category_listings)} listings to database after retry")
                                except Exception as retry_e:
                                    Actor.log.error(f"Failed to save listings to database even after retry: {retry_e}")
                    
                    Actor.log.info(f"=== Completed category: {category} ===")
                    
                except Exception as e:
                    Actor.log.error(f"Error scraping category {category}: {e}")
                    continue
            
            # Final summary and database cleanup
            Actor.log.info(f"=== SCRAPING COMPLETED ===")
            Actor.log.info(f"Total listings scraped: {len(all_listings)}")
            Actor.log.info(f"Categories processed: {len(categories)}")
            
            # Update actor run status in database
            if db_manager_available:
                try:
                    db_manager.update_actor_run_status('completed', len(all_listings))
                    db_manager.close_pool()
                    Actor.log.info("Database connection closed successfully")
                except Exception as e:
                    Actor.log.error(f"Failed to update actor run status: {e}")
            
            # Set status message
            await Actor.set_status_message(f"Completed: {len(all_listings)} listings scraped from {len(categories)} categories")


if __name__ == '__main__':
    asyncio.run(main())
