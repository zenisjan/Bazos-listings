# PostgreSQL Database Setup for Multi-Scraper Listings System

This guide explains how to set up the PostgreSQL database for the multi-scraper listings system, supporting multiple scrapers inserting data into the same database.

## Database Architecture

The database uses a normalized structure with two main tables:

### 1. `actor_runs` (Parent Table)
- **Primary Key**: `id` (SERIAL)
- **Unique Key**: `run_id` (VARCHAR) - Apify Actor Run ID
- Tracks each scraping session with metadata

### 2. `listings` (Child Table)
- **Primary Key**: `(id, actor_run_id, scraper_name)` - Composite key
- **Foreign Key**: `actor_run_id` → `actor_runs.id`
- **Scraper Support**: `scraper_name` field identifies which scraper inserted the data
- Stores individual listing data from multiple scrapers

## Setup Instructions

### 1. Database Server Setup

Connect to your PostgreSQL server:
```bash
psql -h postgresql-v1mr4-u45404.vm.elestio.app -U your_username -d postgres
```

### 2. Create Database

```sql
CREATE DATABASE bazos_scraper;
\c bazos_scraper;
```

### 3. Run Database Schema

For new installations:
```bash
psql -h postgresql-v1mr4-u45404.vm.elestio.app -U your_username -d bazos_scraper -f setup_database_v2.sql
```

For existing installations (migration):
```bash
# Use the safe migration script to avoid view column conflicts
psql -h postgresql-v1mr4-u45404.vm.elestio.app -U your_username -d bazos_scraper -f migrate_database_safe.sql
```

### 4. Environment Variables

#### For Local Development (.env file):
```env
DB_HOST=postgresql-v1mr4-u45404.vm.elestio.app
DB_PORT=5432
DB_NAME=bazos_scraper
DB_USER=your_username
DB_PASSWORD=your_password
DB_SSL_MODE=require
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
SCRAPER_NAME=bazos_scraper
```

#### For Apify Platform:
Set these environment variables in the Apify Console under Actor Settings → Environment Variables:

- `DB_HOST`: postgresql-v1mr4-u45404.vm.elestio.app
- `DB_PORT`: 5432
- `DB_NAME`: bazos_scraper
- `DB_USER`: your_username
- `DB_PASSWORD`: your_password
- `DB_SSL_MODE`: require
- `DB_POOL_SIZE`: 5
- `DB_MAX_OVERFLOW`: 10

## Database Schema Details

### Tables Created

1. **actor_runs**: Tracks scraping sessions
2. **bazos_listings**: Stores listing data
3. **latest_listings**: View for easy querying
4. **actor_run_stats**: View for statistics

### Indexes Created

- Performance indexes on frequently queried columns
- Foreign key indexes
- Composite key indexes

## Usage Examples

### Query Latest Listings
```sql
SELECT * FROM latest_listings 
WHERE category = 'auto' 
ORDER BY scraped_at DESC 
LIMIT 10;
```

### Get Actor Run Statistics
```sql
SELECT * FROM actor_run_stats 
ORDER BY start_time DESC 
LIMIT 5;
```

### Find Listings by Price Range
```sql
SELECT title, price, location, scraped_at 
FROM latest_listings 
WHERE category = 'auto' 
  AND price BETWEEN 100000 AND 500000 
ORDER BY price ASC;
```

### Get Listings from Specific Run
```sql
SELECT bl.*, ar.run_id, ar.start_time 
FROM bazos_listings bl
JOIN actor_runs ar ON bl.actor_run_id = ar.id
WHERE ar.run_id = 'your-run-id'
ORDER BY bl.scraped_at DESC;
```

## Data Flow

1. **Actor Start**: Creates record in `actor_runs` table
2. **Scraping**: Inserts listings into `bazos_listings` table
3. **Actor End**: Updates `actor_runs` status to 'completed'

## Unique Key Strategy

- **Listing ID + Actor Run ID**: Ensures same listing can be scraped multiple times
- **Actor Run Tracking**: Each scraping session is tracked separately
- **Data Integrity**: Foreign key constraints maintain referential integrity

## Performance Considerations

- **Connection Pooling**: Uses psycopg2 connection pool
- **Batch Inserts**: Uses execute_values for efficient bulk inserts
- **Indexes**: Optimized for common query patterns
- **JSON Storage**: Images and similar listings stored as JSONB
- **Multi-Scraper Support**: Supports multiple scrapers with scraper_name field
- **Scraper Statistics**: Built-in views for scraper performance analysis

## Multi-Scraper Configuration

### Scraper Name Configuration

Each scraper should have a unique `SCRAPER_NAME` environment variable:

```env
# For Bazos.cz scraper
SCRAPER_NAME=bazos_scraper

# For other scrapers
SCRAPER_NAME=olx_scraper
SCRAPER_NAME=allegro_scraper
SCRAPER_NAME=marketplace_scraper
```

### Querying by Scraper

```sql
-- Get all listings from a specific scraper
SELECT * FROM latest_listings WHERE scraper_name = 'bazos_scraper';

-- Get scraper statistics
SELECT * FROM scraper_stats WHERE scraper_name = 'bazos_scraper';

-- Compare scrapers
SELECT scraper_name, total_listings, categories_scraped 
FROM scraper_stats 
ORDER BY total_listings DESC;
```

### Adding New Scrapers

1. Set unique `SCRAPER_NAME` environment variable
2. Use the same database connection settings
3. The scraper will automatically be tracked in the `scraper_stats` view

## Troubleshooting

### Connection Issues
- Verify database credentials
- Check SSL mode settings
- Ensure firewall allows connections

### Permission Issues
- Grant necessary privileges to database user
- Check table ownership

### Performance Issues
- Monitor connection pool usage
- Check index usage with EXPLAIN ANALYZE
- Consider partitioning for large datasets
