# Bazos.cz Scraper

A comprehensive Apify Actor for scraping classified advertisements from Bazos.cz, the largest Czech classified ads platform.

## Features

- **Multi-category scraping**: Supports all Bazos.cz categories (auto, deti, reality, etc.)
- **Detailed data extraction**: Scrapes both listing previews and detailed information from individual pages
- **Advanced filtering**: Search by keywords, location, price range
- **Image extraction**: Downloads multiple images from listings
- **Contact information**: Extracts seller contact details when available
- **Location data**: Includes GPS coordinates and location details
- **Rate limiting**: Respectful scraping with built-in delays
- **Robust error handling**: Continues scraping even if individual listings fail

## Input Configuration

### Required Parameters

- **categories** (array): List of categories to scrape
  - Available: `auto`, `deti`, `dum`, `elektro`, `foto`, `hudba`, `knihy`, `mobil`, `motorky`, `nabytek`, `obleceni`, `pc`, `prace`, `reality`, `sluzby`, `sport`, `stroje`, `vstupenky`, `zvirata`, `ostatni`

### Optional Parameters

- **maxListings** (integer): Maximum listings per category (default: 100, 0 = unlimited)
- **includeDetailedData** (boolean): Whether to scrape detailed info from individual pages (default: true)
- **searchQuery** (string): Search term to filter listings
- **location** (string): Location filter (city or postal code)
- **priceMin** (integer): Minimum price filter in CZK
- **priceMax** (integer): Maximum price filter in CZK

## Example Input

```json
{
  "categories": ["auto", "reality"],
  "maxListings": 50,
  "includeDetailedData": true,
  "searchQuery": "škoda",
  "location": "Praha",
  "priceMin": 100000,
  "priceMax": 500000
}
```

## Output Data

### Basic Listing Data
- **id**: Unique listing identifier
- **title**: Listing title
- **url**: Direct link to listing
- **category**: Category name
- **price**: Numeric price in CZK
- **price_text**: Original price text
- **description**: Short description from listing preview
- **location**: Location string
- **views**: Number of views
- **date**: Publication date
- **is_top**: Whether listing is promoted (TOP)
- **image_url**: Main image URL
- **scraped_at**: Timestamp when scraped

### Detailed Data (when includeDetailedData=true)
- **full_description**: Complete listing description
- **contact_name**: Seller's name
- **phone**: Phone number (if available)
- **coordinates**: GPS coordinates (latitude, longitude)
- **images**: Array of all image URLs
- **similar_listings**: Related listings

## Use Cases

### Real Estate Analysis
```json
{
  "categories": ["reality"],
  "maxListings": 200,
  "includeDetailedData": true,
  "location": "Praha"
}
```

### Car Market Research
```json
{
  "categories": ["auto"],
  "maxListings": 100,
  "searchQuery": "BMW",
  "priceMin": 200000,
  "priceMax": 1000000
}
```

### Electronics Price Monitoring
```json
{
  "categories": ["elektro", "pc", "mobil"],
  "maxListings": 50,
  "searchQuery": "iPhone"
}
```

## Technical Details

- **Language**: Python 3.13+
- **Parsing**: BeautifulSoup with lxml parser
- **HTTP Client**: HTTPX for async requests
- **Rate Limiting**: 1-second delay between pages, 0.5-second delay for detailed scraping
- **Error Handling**: Graceful handling of missing elements and network errors

## Performance

- **Speed**: ~20 listings per second for basic data
- **Memory**: Optimized for large datasets with incremental data saving
- **Scalability**: Supports unlimited listings with proper rate limiting

## Legal Notice

This scraper respects Bazos.cz's robots.txt and implements responsible scraping practices:
- Rate limiting to avoid overloading servers
- User-agent identification
- Respectful request frequency

Please ensure compliance with Bazos.cz's Terms of Service and applicable laws when using this Actor.

## Example Output

```json
{
  "id": "207952835",
  "title": "Škoda Fabia 2 combi SPORT 1.4i 16V nová STK KLIMA",
  "url": "https://auto.bazos.cz/inzerat/207952835/...",
  "category": "auto",
  "price": 37990,
  "price_text": "37 990 Kč",
  "description": "Nabízím k prodeji automobil Škoda Fabia 2...",
  "location": "Pelhřimov 395 01",
  "views": 56,
  "date": "15.9. 2025",
  "is_top": true,
  "image_url": "https://www.bazos.cz/img/1t/835/207952835.jpg",
  "full_description": "Nabízím k prodeji automobil Škoda Fabia 2 1.4i 16V combi...",
  "coordinates": {
    "latitude": 49.468124,
    "longitude": 15.037537
  },
  "images": [
    "https://www.bazos.cz/img/1/835/207952835.jpg",
    "https://www.bazos.cz/img/2/835/207952835.jpg",
    "https://www.bazos.cz/img/3/835/207952835.jpg"
  ],
  "scraped_at": "2025-09-15T17:31:57.435030"
}
```

## Support

For issues, feature requests, or questions, please refer to the Actor documentation or contact support through the Apify platform.

## Enhanced Pagination Features

The scraper now includes comprehensive pagination support that automatically:

### **Multi-Method Pagination Detection**
- **Pagination Links**: Detects "Další" (Next) and numbered page links
- **Full Page Detection**: Automatically continues if a page has 20 listings (full page)
- **Total Count Analysis**: Uses "Zobrazeno X-Y inzerátů z Z" pattern to determine if more pages exist
- **Smart Offset Calculation**: Properly calculates page offsets (0, 20, 40, 60, etc.)

### **Pagination Logging**
- **Page Tracking**: Shows current page number and offset
- **Progress Updates**: Displays listings extracted per page and running totals
- **Page Count Summary**: Reports total pages scraped per category

### **Example Pagination Output**
```
[apify] INFO  Scraping page 1 (offset 0): https://auto.bazos.cz/
[apify] INFO  Extracted 20 listings from page 1 (total so far: 20)
[apify] INFO  Scraping page 2 (offset 20): https://auto.bazos.cz/20/
[apify] INFO  Extracted 20 listings from page 2 (total so far: 40)
[apify] INFO  Scraping page 3 (offset 40): https://auto.bazos.cz/40/
[apify] INFO  Extracted 20 listings from page 3 (total so far: 60)
[apify] INFO  Scraped 60 listings from category auto across 3 pages
```

### **Unlimited Scraping**
Set `maxListings: 0` to scrape ALL available listings from a category:
```json
{
  "categories": ["auto"],
  "maxListings": 0,
  "includeDetailedData": false
}
```

This will continue scraping until all pages are exhausted, potentially collecting thousands of listings per category.

### **Rate Limiting**
- **Page Delays**: 1-second delay between pages to be respectful
- **Detailed Scraping**: 0.5-second delay between detailed page requests
- **Error Handling**: Continues scraping even if individual pages fail

### **Robust Error Recovery**
- **Network Errors**: Automatically retries and continues with next page
- **Missing Elements**: Gracefully handles pages with missing pagination elements
- **Invalid URLs**: Skips problematic pages and continues scraping

The enhanced pagination ensures you can scrape complete datasets from Bazos.cz while maintaining respectful scraping practices.

## PostgreSQL Database Integration

The scraper now includes full PostgreSQL database integration for persistent data storage.

### Database Architecture

- **actor_runs**: Parent table tracking each scraping session
- **bazos_listings**: Child table storing individual listings with foreign key to actor_runs
- **Composite Primary Key**: `(listing_id, actor_run_id)` allows same listing in multiple runs
- **Views**: `latest_listings` and `actor_run_stats` for easy querying

### Environment Variables

The scraper uses Apify SDK's environment variable handling, working both locally and on Apify platform:

#### Local Development (.env file):
```env
DB_HOST=postgresql-v1mr4-u45404.vm.elestio.app
DB_PORT=5432
DB_NAME=bazos_scraper
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSL_MODE=require
DB_POOL_SIZE=5
```

#### Apify Platform:
Set these in Actor Settings → Environment Variables in the Apify Console.

### Database Setup

1. **Create Database**:
```sql
CREATE DATABASE bazos_scraper;
```

2. **Run Schema**:
```bash
psql -h your-host -U your-user -d bazos_scraper -f setup_database.sql
```

3. **Test Connection**:
```bash
python test_database.py
```

### Data Storage

- **Dual Storage**: Data saved to both Apify dataset and PostgreSQL database
- **Batch Inserts**: Efficient bulk insertion with conflict resolution
- **Connection Pooling**: Optimized database connections
- **Error Handling**: Graceful fallback if database unavailable

### Query Examples

```sql
-- Latest listings by category
SELECT * FROM latest_listings WHERE category = 'auto' LIMIT 10;

-- Actor run statistics
SELECT * FROM actor_run_stats ORDER BY start_time DESC LIMIT 5;

-- Price analysis
SELECT category, AVG(price), COUNT(*) 
FROM latest_listings 
WHERE price > 0 
GROUP BY category;
```

See `DATABASE_SETUP.md` for complete setup instructions.

## Database Connection Reliability

The scraper includes robust database connection management to handle long-running operations (1000+ results):

### **Connection Health Monitoring**
- **Health Checks**: Automatically verifies connection status before operations
- **Dead Connection Detection**: Identifies and replaces stale connections
- **Connection Pool Management**: Maintains healthy connection pool

### **Automatic Retry Logic**
- **Exponential Backoff**: Retries failed operations with increasing delays
- **Connection Pool Refresh**: Reinitializes pool when connection issues persist
- **Graceful Degradation**: Continues scraping even if database operations fail

### **Long-Run Optimizations**
- **Keepalive Settings**: Prevents server-side connection timeouts
- **Periodic Pool Refresh**: Refreshes connections every few categories
- **Connection Parameters**: Optimized for long-running operations

### **Error Recovery**
- **Automatic Retry**: Failed database operations are automatically retried
- **Pool Reinitialization**: Connection pool is refreshed when issues persist
- **Fallback Handling**: Scraper continues even if database is temporarily unavailable

### **Configuration for Long Runs**
```env
# Optimized for long-running operations
DB_POOL_SIZE=5
DB_KEEPALIVE_IDLE=600
DB_KEEPALIVE_INTERVAL=30
DB_KEEPALIVE_COUNT=3
```

### **Testing Connection Reliability**
```bash
# Test the connection improvements
python test_connection_fix.py
```

This ensures reliable database operations even during extended scraping sessions with thousands of results.
