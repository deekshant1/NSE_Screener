"""
Configuration and Scheduler for NSE Data Downloader
Supports both automatic scheduled updates and manual execution
"""

import schedule
import time
import json
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Assuming nse_downloader.py is in the same directory
from nse_downloader import NSEDataDownloader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nse_scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NSEConfig:
    """Configuration manager for NSE downloader"""
    
    DEFAULT_CONFIG = {
        "base_path": "./nse_data",
        "auto_update": False,
        "update_time": "18:30",  # Time to run daily update (after market close)
        "update_days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "historical_start_date": "2000-01-01",
        "rate_limit_delay": 0.35,
        "max_retries": 3,
        "backoff_factor": 2.0,
        "enable_equity": True,
        "enable_derivatives": True,
        "enable_indices": True,
        "save_csv": True,
        "save_parquet": True,
        "save_database": True,
        "parallel_downloads": 5
    }
    
    def __init__(self, config_file: str = "nse_config.json"):
        """
        Initialize configuration
        
        Args:
            config_file: Path to configuration file
        """
        self.config_file = Path(config_file)
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """Load configuration from file or create default"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                logger.info(f"Configuration loaded from {self.config_file}")
                return {**self.DEFAULT_CONFIG, **config}
            except Exception as e:
                logger.error(f"Error loading config: {e}, using defaults")
                return self.DEFAULT_CONFIG.copy()
        else:
            self.save_config(self.DEFAULT_CONFIG)
            logger.info(f"Created default configuration at {self.config_file}")
            return self.DEFAULT_CONFIG.copy()
    
    def save_config(self, config: dict = None):
        """Save configuration to file"""
        if config is None:
            config = self.config
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=4)
            logger.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    def get(self, key: str, default=None):
        """Get configuration value"""
        return self.config.get(key, default)
    
    def set(self, key: str, value):
        """Set configuration value"""
        self.config[key] = value
        self.save_config()
    
    def enable_auto_update(self):
        """Enable automatic updates"""
        self.set('auto_update', True)
        logger.info("Automatic updates enabled")
    
    def disable_auto_update(self):
        """Disable automatic updates"""
        self.set('auto_update', False)
        logger.info("Automatic updates disabled")


class NSEScheduler:
    """Scheduler for automatic NSE data updates"""
    
    def __init__(self, config: NSEConfig):
        """
        Initialize scheduler
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.downloader = NSEDataDownloader(
            base_path=config.get('base_path')
        )
        self.running = False
    
    def daily_update_job(self):
        """Job function for daily updates"""
        try:
            logger.info("=" * 50)
            logger.info("Starting scheduled daily update")
            logger.info("=" * 50)
            
            start_time = datetime.now()
            
            # Run the update
            self.downloader.update_daily()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            
            logger.info(f"Daily update completed in {duration:.2f} minutes")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error during scheduled update: {e}", exc_info=True)
    
    def weekly_full_sync_job(self):
        """Job function for weekly full synchronization"""
        try:
            logger.info("=" * 50)
            logger.info("Starting weekly full synchronization")
            logger.info("=" * 50)
            
            start_time = datetime.now()
            
            # Get the last 30 days to ensure nothing is missed
            start_date = datetime.now() - timedelta(days=30)
            self.downloader._download_equity_history(start_date, datetime.now())
            self.downloader._download_fo_history(start_date, datetime.now())
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            
            logger.info(f"Weekly sync completed in {duration:.2f} minutes")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error during weekly sync: {e}", exc_info=True)
    
    def setup_schedule(self):
        """Setup the schedule based on configuration"""
        update_time = self.config.get('update_time')
        update_days = self.config.get('update_days')
        
        # Clear existing schedule
        schedule.clear()
        
        # Schedule daily updates
        for day in update_days:
            getattr(schedule.every(), day.lower()).at(update_time).do(self.daily_update_job)
        
        # Schedule weekly full sync on Sunday
        schedule.every().sunday.at("20:00").do(self.weekly_full_sync_job)
        
        logger.info(f"Schedule configured: Updates at {update_time} on {', '.join(update_days)}")
        logger.info("Weekly full sync: Sunday at 20:00")
    
    def run(self):
        """Run the scheduler"""
        self.running = True
        self.setup_schedule()
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        self.downloader.close()
        logger.info("Scheduler stopped")


def run_manual_download(config: NSEConfig, args):
    """Run manual download based on command line arguments"""
    downloader = NSEDataDownloader(base_path=config.get('base_path'))
    
    try:
        if args.full_history:
            # Download full history
            start_date_str = config.get('historical_start_date')
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            
            logger.info(f"Starting full history download from {start_date}")
            downloader.download_full_history(start_date=start_date)
            
        elif args.date_range:
            # Download specific date range
            start_date = datetime.strptime(args.date_range[0], '%Y-%m-%d')
            end_date = datetime.strptime(args.date_range[1], '%Y-%m-%d')
            
            logger.info(f"Downloading data from {start_date} to {end_date}")
            
            if config.get('enable_equity'):
                downloader._download_equity_history(start_date, end_date)
            
            if config.get('enable_derivatives'):
                downloader._download_fo_history(start_date, end_date)
            
        elif args.update:
            # Run daily incremental update
            logger.info("Running daily incremental update")
            downloader.update_daily()
        
        elif args.backfill_days:
            # Backfill last N days
            days = args.backfill_days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            logger.info(f"Backfilling last {days} days")
            
            if config.get('enable_equity'):
                downloader._download_equity_history(start_date, end_date)
            
            if config.get('enable_derivatives'):
                downloader._download_fo_history(start_date, end_date)
        
        else:
            logger.error("No valid operation specified. Use --help for usage information.")
    
    except Exception as e:
        logger.error(f"Error during manual download: {e}", exc_info=True)
    
    finally:
        downloader.close()


def run_query(config: NSEConfig, query: str):
    """Run SQL query on the database"""
    downloader = NSEDataDownloader(base_path=config.get('base_path'))
    
    try:
        logger.info(f"Executing query: {query}")
        df = downloader.query_data(query)
        print("\n" + "=" * 80)
        print("QUERY RESULTS")
        print("=" * 80)
        print(df.to_string())
        print("=" * 80)
        print(f"\nTotal rows: {len(df)}")
        
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
    
    finally:
        downloader.close()


def print_status(config: NSEConfig):
    """Print current status and statistics"""
    downloader = NSEDataDownloader(base_path=config.get('base_path'))
    
    try:
        print("\n" + "=" * 80)
        print("NSE DATA DOWNLOADER - STATUS")
        print("=" * 80)
        
        # Configuration
        print("\nCONFIGURATION:")
        print(f"  Base Path: {config.get('base_path')}")
        print(f"  Auto Update: {'Enabled' if config.get('auto_update') else 'Disabled'}")
        print(f"  Update Time: {config.get('update_time')}")
        print(f"  Update Days: {', '.join(config.get('update_days'))}")
        
        # Last update dates
        print("\nLAST UPDATE DATES:")
        equity_last = downloader.get_last_update_date('equity')
        fo_last = downloader.get_last_update_date('derivatives')
        
        print(f"  Equity: {equity_last.strftime('%Y-%m-%d') if equity_last else 'Never'}")
        print(f"  Derivatives: {fo_last.strftime('%Y-%m-%d') if fo_last else 'Never'}")
        
        # Database statistics
        print("\nDATABASE STATISTICS:")
        
        equity_count = downloader.query_data("SELECT COUNT(*) as count FROM equity_data")
        print(f"  Equity Records: {equity_count['count'].iloc[0]:,}")
        
        derivatives_count = downloader.query_data("SELECT COUNT(*) as count FROM derivatives_data")
        print(f"  Derivatives Records: {derivatives_count['count'].iloc[0]:,}")
        
        index_count = downloader.query_data("SELECT COUNT(*) as count FROM index_data")
        print(f"  Index Records: {index_count['count'].iloc[0]:,}")
        
        # Unique symbols
        equity_symbols = downloader.query_data("SELECT COUNT(DISTINCT symbol) as count FROM equity_data")
        print(f"  Unique Equity Symbols: {equity_symbols['count'].iloc[0]:,}")
        
        # Date range
        equity_dates = downloader.query_data("""
            SELECT MIN(date) as min_date, MAX(date) as max_date 
            FROM equity_data
        """)
        
        if not equity_dates.empty and equity_dates['min_date'].iloc[0]:
            print(f"  Equity Date Range: {equity_dates['min_date'].iloc[0]} to {equity_dates['max_date'].iloc[0]}")
        
        print("=" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"Error getting status: {e}", exc_info=True)
    
    finally:
        downloader.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='NSE Data Downloader - Complete solution for NSE historical and daily data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download full history from configured start date
  python nse_scheduler.py --full-history
  
  # Run daily incremental update
  python nse_scheduler.py --update
  
  # Download specific date range
  python nse_scheduler.py --date-range 2024-01-01 2024-12-31
  
  # Backfill last 30 days
  python nse_scheduler.py --backfill-days 30
  
  # Start automatic scheduler
  python nse_scheduler.py --scheduler
  
  # Enable automatic updates
  python nse_scheduler.py --enable-auto
  
  # Check status
  python nse_scheduler.py --status
  
  # Run SQL query
  python nse_scheduler.py --query "SELECT * FROM equity_data WHERE symbol='RELIANCE' LIMIT 10"
        """
    )
    
    # Operation modes
    parser.add_argument('--full-history', action='store_true',
                        help='Download full history from configured start date')
    parser.add_argument('--update', action='store_true',
                        help='Run daily incremental update')
    parser.add_argument('--date-range', nargs=2, metavar=('START_DATE', 'END_DATE'),
                        help='Download data for specific date range (YYYY-MM-DD)')
    parser.add_argument('--backfill-days', type=int, metavar='N',
                        help='Backfill last N days')
    parser.add_argument('--scheduler', action='store_true',
                        help='Start automatic scheduler')
    
    # Configuration
    parser.add_argument('--enable-auto', action='store_true',
                        help='Enable automatic daily updates')
    parser.add_argument('--disable-auto', action='store_true',
                        help='Disable automatic daily updates')
    parser.add_argument('--set-update-time', metavar='TIME',
                        help='Set daily update time (HH:MM format)')
    parser.add_argument('--config', default='nse_config.json',
                        help='Configuration file path')
    
    # Query and status
    parser.add_argument('--query', metavar='SQL',
                        help='Execute SQL query on the database')
    parser.add_argument('--status', action='store_true',
                        help='Display current status and statistics')
    
    args = parser.parse_args()
    
    # Load configuration
    config = NSEConfig(args.config)
    
    # Handle configuration changes
    if args.enable_auto:
        config.enable_auto_update()
        return
    
    if args.disable_auto:
        config.disable_auto_update()
        return
    
    if args.set_update_time:
        config.set('update_time', args.set_update_time)
        logger.info(f"Update time set to {args.set_update_time}")
        return
    
    # Handle status check
    if args.status:
        print_status(config)
        return
    
    # Handle query
    if args.query:
        run_query(config, args.query)
        return
    
    # Handle scheduler
    if args.scheduler:
        if not config.get('auto_update'):
            logger.warning("Auto-update is disabled in configuration. Enable it with --enable-auto")
            response = input("Do you want to enable auto-update now? (y/n): ")
            if response.lower() == 'y':
                config.enable_auto_update()
            else:
                logger.info("Exiting. Enable auto-update to use scheduler.")
                return
        
        scheduler = NSEScheduler(config)
        scheduler.run()
        return
    
    # Handle manual downloads
    if args.full_history or args.update or args.date_range or args.backfill_days:
        run_manual_download(config, args)
        return
    
    # No arguments - show help
    parser.print_help()


if __name__ == "__main__":
    main()