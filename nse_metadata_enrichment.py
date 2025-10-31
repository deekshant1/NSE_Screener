"""
NSE Metadata Enrichment Module
===============================
Fetches and maintains sector, industry, market cap, and other metadata for NSE symbols.

Features:
- Fetches metadata from NSE API
- Caches in database (no repeated fetches)
- Auto-updates stale data (configurable interval)
- Integrates with existing downloader

Usage:
    python nse_metadata_enrichment.py --update-all
    python nse_metadata_enrichment.py --symbols RELIANCE TCS INFY
    python nse_metadata_enrichment.py --status
"""

import sqlite3
import requests
import pandas as pd
import logging
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger('nse_metadata')


class NSEMetadataEnricher:
    """
    Enriches NSE symbols with metadata (sector, industry, market cap, etc.)
    """
    
    BASE_URL = "https://www.nseindia.com"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    
    # How often to refresh metadata (days)
    REFRESH_INTERVAL_DAYS = 30

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

        # Track session status
        self.session_initialized = self._init_session()
        if not self.session_initialized:
            logger.warning("⚠️ NSE session init incomplete - fetching may be unreliable")

        self._init_metadata_table()
    
    def _init_session(self):
        """Initialize NSE session"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # Step 1: Get cookies from main page
                logger.debug(f"Session init attempt {attempt + 1}/{max_retries}")
                response = self.session.get(self.BASE_URL, timeout=10)

                if response.status_code != 200:
                    logger.warning(f"Main page returned {response.status_code}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue

                # Step 2: Wait for cookies to set
                time.sleep(1)

                # Step 3: Update headers for API calls
                self.session.headers.update({
                    'X-Requested-With': 'XMLHttpRequest',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin'
                })

                # Step 4: Verify session with test API call
                test_url = f"{self.BASE_URL}/api/allIndices"
                test_response = self.session.get(test_url, timeout=5)

                if test_response.status_code == 200:
                    logger.info("✅ NSE session initialized successfully")
                    return True
                else:
                    logger.warning(f"Session test failed: {test_response.status_code}")
                    time.sleep(2 ** attempt)

            except Exception as e:
                logger.error(f"Session init error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        logger.error("❌ Failed to initialize NSE session after all retries")
        return False

    def _ensure_session_valid(self) -> bool:
        """Check and refresh session if needed"""
        try:
            test_url = f"{self.BASE_URL}/api/allIndices"
            response = self.session.get(test_url, timeout=5)

            if response.status_code == 401:
                logger.warning("Session expired, refreshing...")
                return self._init_session()

            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Session health check error: {e}")
            return False

    def _init_metadata_table(self):
        """Create metadata table if not exists"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbol_metadata (
                symbol TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                industry TEXT,
                market_cap REAL,
                isin TEXT,
                face_value REAL,
                listing_date DATE,
                paid_up_value REAL,
                is_fno BOOLEAN,
                is_etf BOOLEAN,
                last_updated TIMESTAMP,
                update_source TEXT,
                metadata_json TEXT
            )
        """)
        
        # Create index
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_metadata_symbol 
            ON symbol_metadata(symbol)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_metadata_sector 
            ON symbol_metadata(sector)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_metadata_industry 
            ON symbol_metadata(industry)
        """)
        
        conn.commit()
        conn.close()
        logger.info("Metadata table initialized")
    
    def fetch_symbol_metadata(self, symbol: str, retries: int = 3) -> Optional[Dict]:
        """
        Fetch metadata for a single symbol from NSE API
        Fetch metadata with automatic session refresh on 401
        Returns dict with:
        - company_name
        - sector
        - industry
        - market_cap
        - isin
        - etc.
        """
        for attempt in range(retries):
            try:
                from nse_data_downloader2 import Config
                time.sleep(Config.RATE_LIMIT_DELAY)  # Rate limiting

                url = f"{self.BASE_URL}/api/quote-equity?symbol={symbol}"
                response = self.session.get(url, timeout=10)

                # ✅ FIX: Handle 401 with session refresh
                if response.status_code == 401:
                    logger.warning(f"Got 401 for {symbol}, refreshing session...")
                    if self._init_session():
                        continue  # Retry with new session
                    else:
                        logger.error("Session refresh failed")
                        return None

                if response.status_code != 200:
                    logger.warning(f"Failed to fetch {symbol}: HTTP {response.status_code}")
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                try:

                    # Parse and return data
                    data = response.json()
                    # Extract metadata
                    info = data.get('info', {})
                    # ✅ VALIDATION: Required fields
                    if not info.get('companyName'):
                        logger.warning(f"Missing company name for {symbol}")
                        return None
                    industry_info = info.get('industryInfo', {})
                    if not industry_info.get('macro') or not industry_info.get('industry'):
                        logger.warning(f"Missing sector/industry for {symbol}")
                        # Still save with "Unknown" values

                    metadata = data.get('metadata', {})
                    security_info = data.get('securityInfo', {})

                    result = {
                        'symbol': symbol,
                        'company_name': info.get('companyName', ''),
                        'sector': industry_info.get('macro', 'Unknown'),
                        'industry': industry_info.get('industry', 'Unknown'),
                        'market_cap': self._extract_market_cap(data),
                        'isin': metadata.get('isin', ''),
                        'face_value': security_info.get('faceValue'),
                        'listing_date': metadata.get('listingDate'),
                        'paid_up_value': security_info.get('paidUpValue'),
                        'is_fno': metadata.get('isFNOSec', False),
                        'is_etf': metadata.get('isETFSec', False),
                        'last_updated': datetime.now().isoformat(),
                        'update_source': 'nse_api',
                        'metadata_json': json.dumps(data)  # Store full response
                    }

                    logger.info(f"Fetched metadata for {symbol}: {result['sector']} / {result['industry']}")
                    return result
                    # return self._parse_metadata(data, symbol) # doesnt exist
                except Exception as e:
                    logger.error(f"Validation failed for {symbol}: {e}")
                    return None

            except Exception as e:
                logger.error(f"Error fetching {symbol}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        return None



    
    def _extract_market_cap(self, data: Dict) -> Optional[float]:
        """Extract market cap from various possible locations"""
        try:
            # Try priceInfo
            price_info = data.get('priceInfo', {})
            if 'marketCap' in price_info:
                return float(price_info['marketCap'])
            
            # Try metadata
            metadata = data.get('metadata', {})
            if 'marketCap' in metadata:
                return float(metadata['marketCap'])
            
            # Calculate from outstanding shares and price
            outstanding = data.get('securityInfo', {}).get('issuedSize')
            price = price_info.get('lastPrice')
            
            if outstanding and price:
                # Market cap = outstanding shares × price
                return float(outstanding) * float(price)
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not extract market cap: {e}")
            return None
    
    def save_metadata(self, metadata: Dict):
        """Save metadata to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO symbol_metadata 
            (symbol, company_name, sector, industry, market_cap, isin, 
             face_value, listing_date, paid_up_value, is_fno, is_etf,
             last_updated, update_source, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                company_name = excluded.company_name,
                sector = excluded.sector,
                industry = excluded.industry,
                market_cap = excluded.market_cap,
                isin = excluded.isin,
                face_value = excluded.face_value,
                listing_date = excluded.listing_date,
                paid_up_value = excluded.paid_up_value,
                is_fno = excluded.is_fno,
                is_etf = excluded.is_etf,
                last_updated = excluded.last_updated,
                update_source = excluded.update_source,
                metadata_json = excluded.metadata_json
        """, (
            metadata['symbol'],
            metadata['company_name'],
            metadata['sector'],
            metadata['industry'],
            metadata['market_cap'],
            metadata['isin'],
            metadata['face_value'],
            metadata['listing_date'],
            metadata['paid_up_value'],
            metadata['is_fno'],
            metadata['is_etf'],
            metadata['last_updated'],
            metadata['update_source'],
            metadata['metadata_json']
        ))
        
        conn.commit()
        conn.close()
    
    def get_symbols_needing_update(self) -> List[str]:
        """Get symbols that need metadata update (new or stale)"""
        conn = sqlite3.connect(self.db_path)
        
        # Get all unique symbols from equity_data
        all_symbols = pd.read_sql("""
            SELECT DISTINCT symbol 
            FROM equity_data 
            WHERE symbol IS NOT NULL
            ORDER BY symbol
        """, conn)
        
        # Get symbols with stale or missing metadata
        cutoff_date = (datetime.now() - timedelta(days=self.REFRESH_INTERVAL_DAYS)).isoformat()
        
        existing_metadata = pd.read_sql("""
            SELECT symbol 
            FROM symbol_metadata 
            WHERE last_updated > ?
        """, conn, params=(cutoff_date,))
        
        conn.close()
        
        # Symbols needing update = all symbols - recently updated
        all_set = set(all_symbols['symbol'])
        existing_set = set(existing_metadata['symbol'])
        
        needs_update = list(all_set - existing_set)
        
        logger.info(f"Total symbols: {len(all_set)}, Need update: {len(needs_update)}")
        return needs_update

    def update_symbols(self, symbols: List[str], batch_size: int = 50):
        """Update metadata with session health checks"""
        total = len(symbols)
        success = 0
        failed = 0

        logger.info(f"Starting metadata update for {total} symbols")

        for idx, symbol in enumerate(symbols, 1):
            # Health check every 10 symbols
            if idx % 10 == 0:
                # Refresh session every 100 symbols #################
                if idx % 100 == 0:
                    logger.info("Refreshing session pool...")
                    self.session.close()
                    self.session = requests.Session()
                    self.session.headers.update(self.HEADERS)
                    self._init_session()
                if not self._ensure_session_valid():
                    logger.error("Session validation failed, stopping")
                    logger.error(f"Processed: {success} success, {failed} failed, {total - idx} remaining")
                    break

            logger.info(f"Processing {symbol} ({idx}/{total})")
            
            metadata = self.fetch_symbol_metadata(symbol)
            
            if metadata:
                self.save_metadata(metadata)
                success += 1
            else:
                failed += 1
            
            # Progress logging
            if idx % 10 == 0:
                logger.info(f"Progress: {idx}/{total} ({success} success, {failed} failed)")
            
            # Batch commit every N symbols for performance
            if idx % batch_size == 0:
                logger.info(f"Batch checkpoint: {idx} symbols processed")
        
        logger.info(f"Update complete: {success} success, {failed} failed")
        return success, failed
    
    def update_all_symbols(self):
        """Update metadata for all symbols needing refresh"""
        symbols = self.get_symbols_needing_update()
        
        if not symbols:
            logger.info("All symbols are up to date")
            return
        
        logger.info(f"Updating {len(symbols)} symbols...")
        return self.update_symbols(symbols)
    
    def get_metadata_stats(self) -> Dict:
        """Get statistics about metadata coverage"""
        conn = sqlite3.connect(self.db_path)
        
        stats = {}
        
        # Total symbols
        stats['total_symbols'] = pd.read_sql(
            "SELECT COUNT(DISTINCT symbol) as cnt FROM equity_data", 
            conn
        )['cnt'].iloc[0]
        
        # Symbols with metadata
        stats['symbols_with_metadata'] = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM symbol_metadata", 
            conn
        )['cnt'].iloc[0]
        
        # Coverage percentage
        stats['coverage_percent'] = (
            stats['symbols_with_metadata'] / stats['total_symbols'] * 100
            if stats['total_symbols'] > 0 else 0
        )
        
        # Sector breakdown
        stats['sectors'] = pd.read_sql("""
            SELECT sector, COUNT(*) as count
            FROM symbol_metadata
            GROUP BY sector
            ORDER BY count DESC
        """, conn).to_dict('records')
        
        # Stale metadata count
        cutoff = (datetime.now() - timedelta(days=self.REFRESH_INTERVAL_DAYS)).isoformat()
        stats['stale_count'] = pd.read_sql(
            f"SELECT COUNT(*) as cnt FROM symbol_metadata WHERE last_updated < '{cutoff}'",
            conn
        )['cnt'].iloc[0]
        
        conn.close()
        
        return stats
    
    def export_metadata_csv(self, output_path: str):
        """Export metadata to CSV for manual editing/backup"""
        conn = sqlite3.connect(self.db_path)
        
        df = pd.read_sql("""
            SELECT symbol, company_name, sector, industry, market_cap,
                   isin, is_fno, is_etf, last_updated
            FROM symbol_metadata
            ORDER BY symbol
        """, conn)
        
        df.to_csv(output_path, index=False)
        conn.close()
        
        logger.info(f"Exported {len(df)} symbols to {output_path}")
    
    def import_metadata_csv(self, csv_path: str):
        """Import metadata from CSV (useful for manual corrections)"""
        df = pd.read_csv(csv_path)
        
        required_cols = ['symbol', 'sector', 'industry']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV must have columns: {required_cols}")
        
        conn = sqlite3.connect(self.db_path)
        
        for _, row in df.iterrows():
            metadata = {
                'symbol': row['symbol'],
                'company_name': row.get('company_name', ''),
                'sector': row['sector'],
                'industry': row['industry'],
                'market_cap': row.get('market_cap'),
                'isin': row.get('isin', ''),
                'face_value': row.get('face_value'),
                'listing_date': row.get('listing_date'),
                'paid_up_value': row.get('paid_up_value'),
                'is_fno': row.get('is_fno', False),
                'is_etf': row.get('is_etf', False),
                'last_updated': datetime.now().isoformat(),
                'update_source': 'csv_import',
                'metadata_json': '{}'
            }
            
            self.save_metadata(metadata)
        
        conn.close()
        logger.info(f"Imported {len(df)} symbols from CSV")


def main():
    """CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NSE Metadata Enrichment')
    parser.add_argument('--db', default='nse_data_production/nse_data.db',
                       help='Database path')
    parser.add_argument('--update-all', action='store_true',
                       help='Update all symbols needing refresh')
    parser.add_argument('--symbols', nargs='+',
                       help='Update specific symbols')
    parser.add_argument('--status', action='store_true',
                       help='Show metadata coverage statistics')
    parser.add_argument('--export', type=str,
                       help='Export metadata to CSV')
    parser.add_argument('--import', type=str, dest='import_csv',
                       help='Import metadata from CSV')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    enricher = NSEMetadataEnricher(args.db)
    
    if args.status:
        print("\n" + "=" * 60)
        print("METADATA COVERAGE STATUS")
        print("=" * 60)
        
        stats = enricher.get_metadata_stats()
        print(f"\nTotal Symbols: {stats['total_symbols']:,}")
        print(f"With Metadata: {stats['symbols_with_metadata']:,}")
        print(f"Coverage: {stats['coverage_percent']:.1f}%")
        print(f"Stale (>{enricher.REFRESH_INTERVAL_DAYS} days): {stats['stale_count']:,}")
        
        print(f"\nTop 10 Sectors:")
        for sector in stats['sectors'][:10]:
            print(f"  {sector['sector']}: {sector['count']:,} symbols")
        
        print("=" * 60 + "\n")
    
    elif args.update_all:
        print("Updating all symbols...")
        success, failed = enricher.update_all_symbols()
        print(f"\n✅ Update complete: {success} success, {failed} failed")
    
    elif args.symbols:
        print(f"Updating {len(args.symbols)} symbols...")
        success, failed = enricher.update_symbols(args.symbols)
        print(f"\n✅ Update complete: {success} success, {failed} failed")
    
    elif args.export:
        enricher.export_metadata_csv(args.export)
        print(f"✅ Exported to {args.export}")
    
    elif args.import_csv:
        enricher.import_metadata_csv(args.import_csv)
        print(f"✅ Imported from {args.import_csv}")
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
