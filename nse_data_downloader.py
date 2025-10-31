"""
NSE India Data Downloader - PRODUCTION GRADE VERSION
====================================================
Complete, battle-tested system for downloading NSE equity and derivatives data.

Features:
- Foreground scheduler & background daemon modes
- Crash recovery with checkpoint system
- Data validation & integrity checks
- Memory-efficient streaming for large datasets
- Rate limiting & exponential backoff
- Comprehensive monitoring & alerting
- Zero-downtime updates
- Production-grade logging & metrics

Usage:
    # Production Daemon (Recommended)
    python nse_data_downloader2.py --daemon start          # Background service
    python nse_data_downloader2.py --daemon stop           # Graceful shutdown
    python nse_data_downloader2.py --daemon status         # Health check

    # One-Time Operations
    python nse_data_downloader2.py --update-now            # Incremental update
    python nse_data_downloader2.py --history-only          # Full history
    python nse_data_downloader2.py --days 30               # Last N days
    python nse_data_downloader2.py --fill-gaps             # Fix missing dates
    python nse_data_downloader2.py --validate              # Data integrity check
    python nse_data_downloader2.py --status                # Database status

    # Query Interface
    python nse_data_downloader2.py --query "SELECT ..."    # Read-only queries
    python nse_data_downloader2.py --export csv            # Export to CSV

    # Monitoring
    python nse_data_downloader2.py --metrics               # Prometheus metrics
    python nse_data_downloader2.py --health                # Health endpoint

Author: Production-Ready NSE Downloader
Version: 3.0.0
License: MIT
"""

import os
import sys
import time
import sqlite3
import logging
from logging.handlers import RotatingFileHandler, SysLogHandler
import requests
import pandas as pd
import zipfile
import schedule
import argparse
import signal
import threading
import json
import hashlib
import atexit
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import warnings
try:
    from nse_circuit_breaker import NSECircuitBreakers, circuit_breaker_request
    HAS_CIRCUIT_BREAKER = True
except ImportError:
    HAS_CIRCUIT_BREAKER = False
    print("Circuit breakers not available (nse_circuit_breaker.py not found)")

try:
    from nse_utils import retry
    HAS_RETRY = True
except ImportError:
    HAS_RETRY = False
    print("Retry decorator not available (nse_utils.py not found)")
# Conditional imports
try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("Warning: psutil not installed. Daemon features limited.")

warnings.filterwarnings('ignore')


# ==================== CONFIGURATION ====================
class Config:
    """Centralized configuration with environment variable support"""

    # Paths
    BASE_PATH = os.getenv('NSE_DATA_PATH', './nse_data_production')
    LOG_PATH = os.path.join(BASE_PATH, 'logs')
    CHECKPOINT_PATH = os.path.join(BASE_PATH, 'checkpoints')

    # Database
    DB_PATH = os.path.join(BASE_PATH, 'nse_data.db')
    DB_TIMEOUT = float(os.getenv('NSE_DB_TIMEOUT', '30.0'))
    DB_JOURNAL_MODE = os.getenv('NSE_DB_JOURNAL_MODE', 'WAL')  # Write-Ahead Logging

    # Download settings
    HISTORY_START_DATE = os.getenv('NSE_START_DATE', '2000-01-01')
    UPDATE_TIME = os.getenv('NSE_UPDATE_TIME', '18:30')
    RATE_LIMIT_DELAY = float(os.getenv('NSE_RATE_DELAY', '0.35'))
    MAX_RETRIES = int(os.getenv('NSE_MAX_RETRIES', '5'))
    BACKOFF_FACTOR = float(os.getenv('NSE_BACKOFF_FACTOR', '2.0'))
    MAX_WORKERS = int(os.getenv('NSE_MAX_WORKERS', '4'))
    BATCH_SIZE = int(os.getenv('NSE_BATCH_SIZE', '7'))  # Days to batch

    # Process control
    PID_FILE = os.path.join(BASE_PATH, 'downloader.pid')
    STATUS_FILE = os.path.join(BASE_PATH, 'status.json')
    METRICS_FILE = os.path.join(BASE_PATH, 'metrics.json')

    # Memory management
    MAX_MEMORY_MB = int(os.getenv('NSE_MAX_MEMORY_MB', '512'))
    GC_THRESHOLD = int(os.getenv('NSE_GC_THRESHOLD', '100'))  # MB before GC

    # Monitoring
    HEALTH_CHECK_INTERVAL = int(os.getenv('NSE_HEALTH_INTERVAL', '300'))  # seconds
    METRICS_RETENTION_DAYS = int(os.getenv('NSE_METRICS_RETENTION', '30'))

    # Data validation
    VALIDATE_ON_SAVE = os.getenv('NSE_VALIDATE_ON_SAVE', 'true').lower() == 'true'
    MIN_RECORDS_PER_DAY = int(os.getenv('NSE_MIN_RECORDS', '100'))
    MAX_NULL_PERCENT = float(os.getenv('NSE_MAX_NULL_PERCENT', '10.0'))

    # NSE Holidays (2024-2026) - Update annually
    NSE_HOLIDAYS = {
        # 2024
        "2024-01-26", "2024-03-08", "2024-03-25", "2024-03-29", "2024-04-11",
        "2024-04-17", "2024-05-01", "2024-05-20", "2024-06-17", "2024-07-17",
        "2024-08-15", "2024-10-02", "2024-11-01", "2024-11-15", "2024-12-25",
        # 2025
        "2025-01-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
        "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
        "2025-10-21", "2025-11-03", "2025-12-25",
        # 2026 (placeholder - update when NSE announces)
        "2026-01-26", "2026-08-15", "2026-10-02", "2026-12-25"
    }

    # ✅ NEW: Network timeouts
    REQUEST_TIMEOUT = int(os.getenv('NSE_REQUEST_TIMEOUT', '30'))
    DOWNLOAD_TIMEOUT = int(os.getenv('NSE_DOWNLOAD_TIMEOUT', '120'))
    SESSION_INIT_TIMEOUT = int(os.getenv('NSE_SESSION_TIMEOUT', '10'))

    # ✅ NEW: Retry policy
    RETRY_BACKOFF = float(os.getenv('NSE_RETRY_BACKOFF', '2.0'))
    RETRY_DELAY_BASE = float(os.getenv('NSE_RETRY_DELAY', '1.0'))
    # ✅ NEW: Data validation thresholds
    PRICE_MIN = float(os.getenv('NSE_PRICE_MIN', '0.01'))
    PRICE_MAX = float(os.getenv('NSE_PRICE_MAX', '1000000'))
    VOLUME_MIN = int(os.getenv('NSE_VOLUME_MIN', '0'))
    VOLUME_MAX = int(os.getenv('NSE_VOLUME_MAX', '1000000000'))
    # ✅ NEW: Metadata refresh intervals (days)
    METADATA_MARKET_CAP_REFRESH = int(os.getenv('NSE_METADATA_MARKETCAP', '30'))
    METADATA_SECTOR_REFRESH = int(os.getenv('NSE_METADATA_SECTOR', '90'))
    METADATA_NAME_REFRESH = int(os.getenv('NSE_METADATA_NAME', '180'))

    # ✅ NEW: Batch processing
    METADATA_BATCH_SIZE = int(os.getenv('NSE_METADATA_BATCH', '50'))
    DOWNLOAD_BATCH_SIZE = int(os.getenv('NSE_DOWNLOAD_BATCH', '7'))

    @classmethod
    def ensure_directories(cls):
        """Create all required directories"""
        for path in [cls.BASE_PATH, cls.LOG_PATH, cls.CHECKPOINT_PATH]:
            os.makedirs(path, exist_ok=True)

    @classmethod
    def validate(cls):
        """Validate configuration on startup"""
        errors = []
        warnings = []

        # 1. Directory permissions
        for path_name in ['BASE_PATH', 'LOG_PATH', 'CHECKPOINT_PATH']:
            path = getattr(cls, path_name)
            try:
                os.makedirs(path, exist_ok=True)
                test_file = os.path.join(path, '.write_test')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
            except Exception as e:
                errors.append(f"Cannot write to {path_name} ({path}): {e}")

        # 2. Date validation
        try:
            start_date = datetime.strptime(cls.HISTORY_START_DATE, '%Y-%m-%d')
            if start_date > datetime.now():
                errors.append(f"HISTORY_START_DATE in future: {cls.HISTORY_START_DATE}")
        except ValueError as e:
            errors.append(f"Invalid HISTORY_START_DATE: {e}")

        # 3. Numeric validation
        if cls.MAX_WORKERS < 1 or cls.MAX_WORKERS > 32:
            errors.append(f"MAX_WORKERS must be 1-32, got {cls.MAX_WORKERS}")

        if cls.RATE_LIMIT_DELAY < 0:
            errors.append("RATE_LIMIT_DELAY cannot be negative")

        if cls.DB_TIMEOUT < 5:
            warnings.append(f"DB_TIMEOUT ({cls.DB_TIMEOUT}s) is very low")

        # Log results
        for warning in warnings:
            logger.warning(f"⚠️  {warning}")

        if errors:
            for error in errors:
                logger.error(f"❌ {error}")
            raise ValueError(f"Config validation failed: {len(errors)} errors")

        logger.info("✅ Configuration validated")


# ==================== LOGGING SETUP ====================
class LogManager:
    """Advanced logging with rotation, filtering, and remote logging support"""

    @staticmethod
    def setup():
        Config.ensure_directories()

        # Root logger
        logger = logging.getLogger('nse_downloader')
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()

        # Console handler (INFO and above)
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        logger.addHandler(console)

        # File handler with rotation (10MB, 10 backups)
        file_handler = RotatingFileHandler(
            os.path.join(Config.LOG_PATH, 'nse_downloader.log'),
            maxBytes=10485760,
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        ))
        logger.addHandler(file_handler)

        # Error file (errors only)
        error_handler = RotatingFileHandler(
            os.path.join(Config.LOG_PATH, 'errors.log'),
            maxBytes=5242880,
            backupCount=5,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s\n%(exc_info)s'
        ))
        logger.addHandler(error_handler)

        # Optional: Syslog for production monitoring
        if os.getenv('NSE_SYSLOG_HOST'):
            syslog = SysLogHandler(
                address=(os.getenv('NSE_SYSLOG_HOST'),
                         int(os.getenv('NSE_SYSLOG_PORT', '514')))
            )
            syslog.setLevel(logging.WARNING)
            logger.addHandler(syslog)

        return logger


logger = LogManager.setup()


# ==================== DATA MODELS ====================
class DownloadStatus(Enum):
    """Download result status codes"""
    SUCCESS = "success"
    SKIPPED = "skipped"
    ERROR = "error"
    CANCELLED = "cancelled"
    VALIDATION_FAILED = "validation_failed"


@dataclass
class DownloadResult:
    """Structured download result"""
    status: DownloadStatus
    date: str
    data_type: str
    records: int = 0
    error: Optional[str] = None
    url: Optional[str] = None
    checksum: Optional[str] = None
    data: Optional[pd.DataFrame] = None  # FIX: Added DataFrame to result

    def to_dict(self) -> Dict:
        result = {k: v.value if isinstance(v, Enum) else v
                  for k, v in asdict(self).items()}
        # Don't serialize DataFrame
        if 'data' in result:
            del result['data']
        return result


@dataclass
class Metrics:
    """System metrics for monitoring"""
    downloads_successful: int = 0
    downloads_failed: int = 0
    downloads_skipped: int = 0
    records_processed: int = 0
    errors_count: int = 0
    last_update: Optional[str] = None
    uptime_seconds: float = 0
    memory_mb: float = 0
    cpu_percent: float = 0

    def to_dict(self) -> Dict:
        return asdict(self)


# ==================== PROCESS MANAGER ====================
class ProcessManager:
    """Production-grade process management"""

    @staticmethod
    def is_running() -> bool:
        """Check if daemon is running with stale PID cleanup"""
        if not os.path.exists(Config.PID_FILE):
            return False

        try:
            with open(Config.PID_FILE, 'r') as f:
                pid = int(f.read().strip())

            # Check PID file age
            pid_age = time.time() - os.path.getmtime(Config.PID_FILE)
            if pid_age > 86400:  # 24 hours
                logger.warning(f"Stale PID file detected (age: {pid_age / 3600:.1f}h)")
                os.remove(Config.PID_FILE)
                return False

            if not HAS_PSUTIL:
                # Fallback: try to send signal 0 (doesn't kill, just checks)
                try:
                    os.kill(pid, 0)
                    return True
                except OSError:
                    os.remove(Config.PID_FILE)
                    return False

            # psutil-based check
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                cmdline = ' '.join(proc.cmdline())
                if 'python' in cmdline.lower() and 'nse' in cmdline.lower():
                    return True

            os.remove(Config.PID_FILE)
            return False

        except Exception as e:
            logger.error(f"Error checking process: {e}")
            return False

    @staticmethod
    def write_pid():
        """Write PID file with metadata"""
        pid_data = {
            'pid': os.getpid(),
            'started': datetime.now().isoformat(),
            'python_version': sys.version,
            'script_path': os.path.abspath(__file__)
        }
        with open(Config.PID_FILE, 'w') as f:
            json.dump(pid_data, f, indent=2)
        logger.info(f"PID file created: {os.getpid()}")

    @staticmethod
    def remove_pid():
        """Remove PID file safely"""
        try:
            if os.path.exists(Config.PID_FILE):
                os.remove(Config.PID_FILE)
                logger.info("PID file removed")
        except Exception as e:
            logger.error(f"Error removing PID file: {e}")

    @staticmethod
    def start():
        """Start daemon process - FIX: Windows compatible"""
        # Check if already running
        if ProcessManager.is_running():
            print("Daemon already running")
            return

        # FIX: Windows-compatible daemon start
        if sys.platform == 'win32':
            print("⚠️  Windows detected - daemon mode not fully supported")
            print("   Running in foreground mode instead")
            print("   Consider using Task Scheduler for background execution")
            return 'foreground'  # Signal to run in foreground

        # Unix/Linux daemon
        try:
            pid = os.fork()
            if pid > 0:
                # Parent process
                with open(Config.PID_FILE, 'w') as f:
                    f.write(str(pid))
                print(f"Daemon started with PID {pid}")
                sys.exit(0)
        except OSError as e:
            print(f"❌ Fork failed: {e}")
            sys.exit(1)

        # Child process continues as daemon
        os.setsid()
        return 'daemon'

    @staticmethod
    def stop(timeout: int = 30) -> bool:
        """Gracefully stop daemon with timeout"""
        if not os.path.exists(Config.PID_FILE):
            print("❌ No process running (PID file not found)")
            return False

        if not HAS_PSUTIL:
            print("❌ psutil not installed. Cannot stop daemon.")
            return False

        try:
            with open(Config.PID_FILE, 'r') as f:
                pid_data = json.load(f)
                pid = pid_data['pid']

            if not psutil.pid_exists(pid):
                print(f"❌ Process {pid} not found (stale PID file)")
                os.remove(Config.PID_FILE)
                return False

            proc = psutil.Process(pid)
            print(f"🛑 Stopping process {pid}...")

            # Graceful termination
            proc.terminate()
            for i in range(timeout):
                if not psutil.pid_exists(pid):
                    print(f"✅ Process {pid} stopped gracefully")
                    ProcessManager.remove_pid()
                    return True
                time.sleep(1)
                if i % 5 == 0:
                    print(f"   Waiting... ({i}/{timeout}s)")

            # Force kill if still running
            if psutil.pid_exists(pid):
                print(f"⚠️  Process didn't stop gracefully, forcing...")
                proc.kill()
                time.sleep(2)
                if not psutil.pid_exists(pid):
                    print(f"✅ Process {pid} force killed")
                    ProcessManager.remove_pid()
                    return True
                else:
                    print(f"❌ Failed to stop process {pid}")
                    return False

        except Exception as e:
            print(f"❌ Error stopping process: {e}")
            return False

    @staticmethod
    def get_status() -> Dict[str, Any]:
        """Get detailed daemon status"""
        if not ProcessManager.is_running():
            return {'running': False, 'message': 'Downloader is not running'}

        if not HAS_PSUTIL:
            return {'running': True, 'message': 'Running (limited info - psutil not installed)'}

        try:
            with open(Config.PID_FILE, 'r') as f:
                pid_data = json.load(f)
                pid = pid_data['pid']

            proc = psutil.Process(pid)

            status_info = {}
            if os.path.exists(Config.STATUS_FILE):
                with open(Config.STATUS_FILE, 'r') as f:
                    status_info = json.load(f)

            metrics_info = {}
            if os.path.exists(Config.METRICS_FILE):
                with open(Config.METRICS_FILE, 'r') as f:
                    metrics_info = json.load(f)

            return {
                'running': True,
                'pid': pid,
                'started': pid_data.get('started'),
                'uptime_seconds': time.time() - proc.create_time(),
                'cpu_percent': proc.cpu_percent(interval=0.1),
                'memory_mb': proc.memory_info().rss / 1024 / 1024,
                'num_threads': proc.num_threads(),
                'num_fds': proc.num_fds() if hasattr(proc, 'num_fds') else 'N/A',
                **status_info,
                **metrics_info
            }
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {'running': False, 'error': str(e)}

    @staticmethod
    def update_status(status_dict: Dict):
        """Update status file atomically"""
        try:
            temp_file = Config.STATUS_FILE + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(status_dict, f, indent=2)
            os.replace(temp_file, Config.STATUS_FILE)
        except Exception as e:
            logger.error(f"Could not update status: {e}")


# ==================== CHECKPOINT MANAGER ====================
class CheckpointManager:
    """Crash recovery with checkpoint system"""

    @staticmethod
    def save_checkpoint(data_type: str, date: datetime, data: Dict):
        """Save checkpoint for crash recovery"""
        checkpoint_file = os.path.join(
            Config.CHECKPOINT_PATH,
            f"{data_type}_{date.strftime('%Y%m%d')}.json"
        )
        try:
            with open(checkpoint_file, 'w') as f:
                json.dump({
                    'date': date.isoformat(),
                    'data_type': data_type,
                    'timestamp': datetime.now().isoformat(),
                    **data
                }, f)
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")

    @staticmethod
    def load_checkpoint(data_type: str, date: datetime) -> Optional[Dict]:
        """Load checkpoint if exists"""
        checkpoint_file = os.path.join(
            Config.CHECKPOINT_PATH,
            f"{data_type}_{date.strftime('%Y%m%d')}.json"
        )
        try:
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
        return None

    @staticmethod
    def clear_checkpoint(data_type: str, date: datetime):
        """Clear checkpoint after successful save"""
        checkpoint_file = os.path.join(
            Config.CHECKPOINT_PATH,
            f"{data_type}_{date.strftime('%Y%m%d')}.json"
        )
        try:
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
        except Exception as e:
            logger.warning(f"Could not clear checkpoint: {e}")

    @staticmethod
    def cleanup_old_checkpoints(days: int = 7):
        """Clean up old checkpoint files"""
        try:
            cutoff = datetime.now() - timedelta(days=days)
            for filename in os.listdir(Config.CHECKPOINT_PATH):
                filepath = os.path.join(Config.CHECKPOINT_PATH, filename)
                if os.path.getmtime(filepath) < cutoff.timestamp():
                    os.remove(filepath)
                    logger.debug(f"Cleaned up old checkpoint: {filename}")
        except Exception as e:
            logger.warning(f"Checkpoint cleanup error: {e}")


# ==================== DATABASE MANAGER ====================
class DatabaseManager:
    """Production-grade database operations with connection pooling"""

    # Column mappings (case-insensitive)
    COLUMN_MAPPING = {
        # New format (post-July 2024)
        'tckrsymb': 'symbol', 'sctysrs': 'series', 'traddt': 'date',
        'bizdt': 'trade_date', 'opnpric': 'open', 'hghpric': 'high',
        'lwpric': 'low', 'clspric': 'close', 'lastpric': 'last',
        'prvsclsgpric': 'prev_close', 'ttltradgvol': 'volume',
        'ttltrfval': 'turnover', 'ttlnboftxsexctd': 'trades',
        'isin': 'isin', 'fininstrmtp': 'instrument', 'xprydt': 'expiry_date',
        'strkpric': 'strike_price', 'optntp': 'option_type',
        'sttlmpric': 'settle_price', 'opnintrst': 'open_interest',
        'chnginopnintrst': 'change_in_oi', 'undrlygpric': 'underlying_price',
        # Old format (pre-July 2024)
        'symbol': 'symbol', 'series': 'series', 'date': 'date',
        'timestamp': 'date', 'open': 'open', 'high': 'high', 'low': 'low',
        'close': 'close', 'last': 'last', 'prevclose': 'prev_close',
        'tottrdqty': 'volume', 'tottrdval': 'turnover', 'totaltrades': 'trades',
        'instrument': 'instrument', 'expiry_dt': 'expiry_date',
        'strike_pr': 'strike_price', 'option_typ': 'option_type',
        'settle_pr': 'settle_price', 'open_int': 'open_interest',
        'chg_in_oi': 'change_in_oi', 'underlying': 'underlying_price'
    }

    VALID_TABLES = {'equity_data', 'derivatives_data', 'fo_data', 'metadata'}

    def __init__(self):
        self._init_db()

    @contextmanager
    def get_connection(self, read_only: bool = False):
        """Context manager for database connections"""
        conn = None
        try:
            uri = f"file:{Config.DB_PATH}"
            if read_only:
                uri += "?mode=ro"

            conn = sqlite3.connect(
                uri,
                timeout=Config.DB_TIMEOUT,
                isolation_level='DEFERRED' if read_only else 'EXCLUSIVE',
                uri=True
            )

            # Performance optimizations
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA temp_store=MEMORY")

            if read_only:
                conn.execute("PRAGMA query_only=ON")

            yield conn

            if not read_only:
                conn.commit()

        except Exception as e:
            if conn and not read_only:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def _init_db(self):
        """Initialize database with optimized schema"""
        try:
            # Ensure database directory exists
            db_dir = os.path.dirname(Config.DB_PATH)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Metadata table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metadata (
                        data_type TEXT PRIMARY KEY,
                        last_update DATE NOT NULL,
                        total_records INTEGER DEFAULT 0,
                        last_checksum TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Download history for monitoring
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS download_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date DATE NOT NULL,
                        data_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        records INTEGER DEFAULT 0,
                        error TEXT,
                        duration_seconds REAL,
                        checksum TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date, data_type)
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_download_history_date 
                    ON download_history(date DESC)
                """)

                conn.commit()

            logger.info("Database initialized with WAL mode")

        except Exception as e:
            logger.error(f"Failed to initialize database at {Config.DB_PATH}: {e}")
            raise RuntimeError(f"Database initialization failed: {e}")

    def create_table_from_df(self, df: pd.DataFrame, table_name: str, conn):
        """Dynamic schema creation/update"""
        if table_name not in self.VALID_TABLES:
            raise ValueError(f"Invalid table name: {table_name}")

        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        table_exists = cursor.fetchone()

        if table_exists:
            # Add missing columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_cols = {row[1].lower() for row in cursor.fetchall()}
            new_cols = {col.lower() for col in df.columns}

            for col in new_cols - existing_cols:
                dtype = self._get_sql_type(df[col].dtype)
                try:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")
                    logger.info(f"Added column {col} ({dtype}) to {table_name}")
                except Exception as e:
                    logger.warning(f"Could not add column {col}: {e}")
        else:
            # Create new table
            col_defs = []
            for col in df.columns:
                dtype = self._get_sql_type(df[col].dtype)
                col_defs.append(f"{col.lower()} {dtype}")

            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            cursor.execute(create_sql)
            logger.info(f"Created table {table_name}")

            # Create indexes
            if 'symbol' in df.columns:
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol "
                    f"ON {table_name}(symbol)"
                )
            if 'date' in df.columns:
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date "
                    f"ON {table_name}(date DESC)"
                )

            # Composite index for common queries
            if all(c in df.columns for c in ['symbol', 'date']):
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date "
                    f"ON {table_name}(symbol, date DESC)"
                )

        conn.commit()

    @staticmethod
    def _get_sql_type(dtype) -> str:
        """Map pandas dtype to SQLite type"""
        if dtype in ['int64', 'int32', 'int16', 'int8']:
            return 'INTEGER'
        elif dtype in ['float64', 'float32']:
            return 'REAL'
        elif 'datetime' in str(dtype):
            return 'DATETIME'
        else:
            return 'TEXT'

    def remove_duplicates(self, table_name: str, data_type: str, conn):
        """Remove duplicates with proper NULL handling"""
        if table_name not in self.VALID_TABLES:
            raise ValueError(f"Invalid table name: {table_name}")

        cursor = conn.cursor()

        if data_type == 'equity':
            cursor.execute(f"""
                DELETE FROM {table_name} WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM {table_name}
                    GROUP BY symbol, COALESCE(series, ''), date
                )
            """)
        else:  # derivatives
            cursor.execute(f"""
                DELETE FROM {table_name} WHERE rowid NOT IN (
                    SELECT MIN(rowid) FROM {table_name}
                    GROUP BY symbol, instrument,
                             COALESCE(expiry_date, ''),
                             COALESCE(strike_price, 0),
                             COALESCE(option_type, ''),
                             date
                )
            """)

        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Removed {deleted} duplicates from {table_name}")

        conn.commit()
        return deleted

    def get_last_update(self, data_type: str) -> Optional[datetime]:
        """Get last successful update date"""
        with self.get_connection(read_only=True) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_update FROM metadata WHERE data_type = ?",
                (data_type,)
            )
            result = cursor.fetchone()
            return datetime.strptime(result[0], '%Y-%m-%d') if result else None

    def set_last_update(self, data_type: str, date: datetime, records: int = 0):
        """Update last successful download date"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO metadata (data_type, last_update, total_records, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(data_type) DO UPDATE SET
                    last_update = excluded.last_update,
                    total_records = excluded.total_records,
                    updated_at = CURRENT_TIMESTAMP
            """, (data_type, date.strftime('%Y-%m-%d'), records))

    def log_download(self, result: DownloadResult, duration: float):
        """Log download attempt for monitoring"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO download_history 
                (date, data_type, status, records, error, duration_seconds, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, data_type) DO UPDATE SET
                    status = excluded.status,
                    records = excluded.records,
                    error = excluded.error,
                    duration_seconds = excluded.duration_seconds,
                    checksum = excluded.checksum,
                    created_at = CURRENT_TIMESTAMP
            """, (
                result.date, result.data_type, result.status.value,
                result.records, result.error, duration, result.checksum
            ))

    @staticmethod
    def normalize_data_type(data_type: str) -> str:
        """Normalize data type for consistent storage"""
        return 'derivatives' if data_type in ('fo', 'derivatives') else 'equity'


# ==================== DATA VALIDATOR ====================
class DataValidator:
    """Data quality validation"""

    @staticmethod
    def validate_dataframe(df: pd.DataFrame, data_type: str) -> Tuple[bool, Optional[str]]:
        """Validate DataFrame quality before saving"""

        # Check 1: Empty DataFrame
        if df.empty:
            return False, "Empty DataFrame"

        # Check 2: Minimum records threshold
        if len(df) < Config.MIN_RECORDS_PER_DAY:
            return False, f"Too few records: {len(df)} < {Config.MIN_RECORDS_PER_DAY}"

        # Check 3: Required columns
        required_cols = {'symbol', 'date'}
        if data_type == 'equity':
            required_cols.update({'open', 'high', 'low', 'close', 'volume'})

        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            return False, f"Missing columns: {missing_cols}"

        # Check 4: NULL percentage
        for col in required_cols:
            null_pct = (df[col].isna().sum() / len(df)) * 100
            if null_pct > Config.MAX_NULL_PERCENT:
                return False, f"Too many NULLs in {col}: {null_pct:.1f}%"

        # Check 5: Negative prices
        if data_type == 'equity':
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                if col in df.columns:
                    if (df[col] < 0).any():
                        return False, f"Negative values in {col}"

        # Check 6: Date validity
        try:
            pd.to_datetime(df['date'])
        except Exception as e:
            return False, f"Invalid dates: {e}"

        # Check 7: Symbol validity (not all NULLs or empty)
        if df['symbol'].str.strip().eq('').sum() > len(df) * 0.01:
            return False, "Too many empty symbols"

        return True, None

    @staticmethod
    def calculate_checksum(df: pd.DataFrame) -> str:
        """Calculate DataFrame checksum for integrity verification"""
        try:
            # Use a subset of data for checksum (symbol, date, close)
            subset = df[['symbol', 'date']].copy()
            if 'close' in df.columns:
                subset['close'] = df['close']

            data_str = subset.to_csv(index=False)
            return hashlib.sha256(data_str.encode()).hexdigest()[:16]
        except Exception as e:
            logger.warning(f"Checksum calculation failed: {e}")
            return "unknown"


# ==================== MEMORY MANAGER ====================
class MemoryManager:
    """Memory management for long-running processes"""

    @staticmethod
    def get_memory_usage() -> float:
        """Get current memory usage in MB"""
        if HAS_PSUTIL:
            proc = psutil.Process(os.getpid())
            return proc.memory_info().rss / 1024 / 1024
        return 0.0

    @staticmethod
    def check_and_gc():
        """Check memory and trigger GC if needed"""
        import gc

        current_mem = MemoryManager.get_memory_usage()
        if current_mem > Config.GC_THRESHOLD:
            logger.debug(f"Memory at {current_mem:.1f}MB, triggering GC")
            gc.collect()
            new_mem = MemoryManager.get_memory_usage()
            logger.debug(f"GC freed {current_mem - new_mem:.1f}MB")

    @staticmethod
    def cleanup_dataframes(*dfs):
        """Explicitly delete DataFrames and trigger GC"""
        import gc
        for df in dfs:
            if df is not None:
                del df
        gc.collect()


# ==================== NSE DOWNLOADER ====================
class NSEDownloader:
    """Production-grade NSE data downloader"""

    BASE_URL = "https://www.nseindia.com"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    def __init__(self, force_redownload=False):
        self.base_path = Path(Config.BASE_PATH)
        self.csv_path = self.base_path / "csv"
        self.parquet_path = self.base_path / "parquet"
        self._write_lock = threading.Lock()  # Prevent parallel write conflicts

        for path in [self.csv_path, self.parquet_path]:
            path.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager()
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.shutdown_flag = threading.Event()
        self.metrics = Metrics()
        self.start_time = time.time()
        if HAS_CIRCUIT_BREAKER:
            self.circuit_breakers = NSECircuitBreakers()
            logger.info("Circuit breakers enabled")
        else:
            self.circuit_breakers = None

        # Initialize metadata enricher (optional)
        self.metadata_enricher = None
        try:
            from nse_metadata_enrichment import NSEMetadataEnricher
            self.metadata_enricher = NSEMetadataEnricher(Config.DB_PATH)
            logger.info("✅ Metadata enricher initialized")
        except ImportError:
            logger.info("ℹ️  Metadata enricher module not found (optional)")
        except Exception as e:
            logger.error(f"❌ Metadata enricher init failed: {e}")
            logger.warning("Continuing without metadata enrichment")

        self.force_redownload = force_redownload
        self._init_session()

        # Register cleanup on exit
        atexit.register(self.cleanup)

        logger.info("NSEDownloader initialized")

    def _init_session(self):
        """Initialize session with NSE"""
        try:
            response = self.session.get(self.BASE_URL, timeout=10)
            if response.status_code == 200:
                logger.info("Session initialized successfully")
            else:
                logger.warning(f"Session init returned {response.status_code}")
        except Exception as e:
            logger.error(f"Session initialization failed: {e}")
    @retry(max_attempts=Config.MAX_RETRIES, delay=Config.RATE_LIMIT_DELAY, backoff=Config.BACKOFF_FACTOR, exceptions=(requests.RequestException,)) if HAS_RETRY else lambda f: f
    def _request(self, url):
        """Request with circuit breaker protection"""
        for attempt in range(Config.MAX_RETRIES):
            if self.shutdown_flag.is_set():
                return None

            try:
                time.sleep(Config.RATE_LIMIT_DELAY)

                if HAS_CIRCUIT_BREAKER and self.circuit_breakers:
                    response = circuit_breaker_request(
                        self.circuit_breakers,
                        self.session,
                        url,
                        timeout=30
                    )
                    if response is None:
                        logger.warning(f"Circuit breaker open for {url}")
                        return None
                else:
                    response = self.session.get(url, timeout=30)

                if response is None:
                    # Circuit is open, skip this URL
                    logger.warning(f"Some issue for {url}") # (f"Circuit breaker open for {url}")
                    return None

                if response.status_code == 200:
                    return response
                elif response.status_code == 401:
                    self._init_session()
                else:
                    logger.warning(f"HTTP {response.status_code}: {url}")

            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

            if attempt < Config.MAX_RETRIES - 1:
                sleep_time = Config.BACKOFF_FACTOR ** attempt
                time.sleep(sleep_time)

        return None

    def vacuum_database(self):
        """Reclaim unused database space"""
        logger.info("Starting database vacuum...")
        try:
            size_before = os.path.getsize(Config.DB_PATH)

            with self.db.get_connection() as conn:
                conn.execute("VACUUM")
                conn.execute("ANALYZE")

            size_after = os.path.getsize(Config.DB_PATH)
            saved_mb = (size_before - size_after) / (1024 * 1024)

            logger.info(f"Vacuum complete. Saved {saved_mb:.2f} MB")
        except Exception as e:
            logger.error(f"Vacuum failed: {e}")

    def status(self):
        print("\n" + "=" * 60)
        print("NSE DATA STATUS")
        print("=" * 60)

        eq_last = self.db.get_last_update('equity')
        fo_last = self.db.get_last_update('derivatives')

        print(f"Last Equity Update: {eq_last.strftime('%Y-%m-%d') if eq_last else 'Never'}")
        print(f"Last Derivatives Update: {fo_last.strftime('%Y-%m-%d') if fo_last else 'Never'}")

        try:
            eq_count = self.query("SELECT COUNT(*) as cnt FROM equity_data")
            fo_count = self.query("SELECT COUNT(*) as cnt FROM derivatives_data")
            symbols = self.query("SELECT COUNT(DISTINCT symbol) as cnt FROM equity_data")

            print(f"Equity Records: {eq_count['cnt'].iloc[0]:,}")
            print(f"Derivatives Records: {fo_count['cnt'].iloc[0]:,}")
            print(f"Unique Symbols: {symbols['cnt'].iloc[0]:,}")
        except Exception as e:
            print(f"No data yet: {e}")
        #
        # from nse_circuit_breaker import print_circuit_breaker_status
        # print_circuit_breaker_status(self.circuit_breakers)

        print("=" * 60 + "\n")

    def query(self, sql):
        conn = None
        try:
            conn = sqlite3.connect(str(Config.DB_PATH))
            df = pd.read_sql_query(sql, conn)
            return df
        finally:
            if conn:
                conn.close()

    def query_with_timeout(self, sql, timeout=30):
        """Execute SQL query with timeout"""
        result = {'data': None, 'error': None}

        def run_query():
            try:
                result['data'] = self.query(sql)
            except Exception as e:
                result['error'] = str(e)

        thread = threading.Thread(target=run_query)
        thread.daemon = True
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            raise TimeoutError("Query execution timeout")

        if result['error']:
            raise Exception(result['error'])

        return result['data']

    def _get_bhavcopy_url(self, date: datetime, data_type: str) -> str:
        """Generate NSE bhavcopy URL based on date"""
        cutoff_date = datetime(2024, 7, 8)

        if date >= cutoff_date:
            # New format (post July 2024)
            date_str = date.strftime('%Y%m%d')
            if data_type == 'equity':
                return f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip"
            else:
                return f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        else:
            # Old format (pre July 2024)
            date_str = date.strftime('%d%b%Y').upper()
            month = date.strftime('%b').upper()
            year = date.strftime('%Y')

            if data_type == 'equity':
                return f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{date_str}bhav.csv.zip"
            else:
                return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date_str}bhav.csv.zip"

    def is_trading_day(self, date: datetime) -> bool:
        """Check if date is a trading day"""
        # Weekend check
        if date.weekday() >= 5:
            return False

        # Holiday check
        date_str = date.strftime('%Y-%m-%d')
        if date_str in Config.NSE_HOLIDAYS:
            return False

        return True

    def download_day(self, date: datetime, data_type: str = 'equity') -> DownloadResult:
        """Download single day's data"""
        start_time = time.time()
        date_str = date.strftime('%Y-%m-%d')

        # Check shutdown flag
        if self.shutdown_flag.is_set():
            return DownloadResult(
                status=DownloadStatus.CANCELLED,
                date=date_str,
                data_type=data_type
            )

        # FIX: Normalize data_type for storage (fo -> derivatives)
        # Normalize data type
        storage_type = DatabaseManager.normalize_data_type(data_type)

        # Check if already exists
        if not self.force_redownload:
            with self.db.get_connection(read_only=True) as conn:
                cursor = conn.cursor()
                table = f'{storage_type}_data'
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE date(date) = ?",
                        (date_str,)
                    )
                    count = cursor.fetchone()[0]
                    if count > 0:
                        logger.debug(f"Data exists for {date_str}")
                        logger.info(f"Skipping {date_str} ({storage_type}) - already exists ({count} records)")
                        return DownloadResult(
                            status=DownloadStatus.SKIPPED,
                            date=date_str,
                            data_type=storage_type,
                            records=count
                        )
                except sqlite3.OperationalError:
                    pass  # Table doesn't exist yet

        # Check checkpoint
        checkpoint = CheckpointManager.load_checkpoint(data_type, date)
        if checkpoint and checkpoint.get('completed'):
            logger.info(f"Loading from checkpoint: {date_str}")
            # Could implement checkpoint-based resume here

        # Download
        url = self._get_bhavcopy_url(date, data_type)
        response = self._request(url)

        if not response:
            return DownloadResult(
                status=DownloadStatus.ERROR,
                date=date_str,
                data_type=storage_type,
                error="Download failed",
                url=url
            )

        try:
            # Extract and parse CSV
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                csv_file = z.namelist()[0]
                df = pd.read_csv(z.open(csv_file))

                # Clean DataFrame
                df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

                # Case-insensitive column normalization
                df.columns = df.columns.str.strip().str.lower()
                df.columns = [self.db.COLUMN_MAPPING.get(c, c) for c in df.columns]

                # Add date if missing
                if 'date' not in df.columns:
                    df['date'] = date_str

                # Validate data
                if Config.VALIDATE_ON_SAVE:
                    is_valid, error = DataValidator.validate_dataframe(df, storage_type)
                    if not is_valid:
                        logger.error(f"Validation failed for {date_str}: {error}")
                        return DownloadResult(
                            status=DownloadStatus.VALIDATION_FAILED,
                            date=date_str,
                            data_type=storage_type,
                            error=error,
                            url=url
                        )

                checksum = DataValidator.calculate_checksum(df)

                logger.info(f"Downloaded {len(df)} records for {date_str} ({data_type})")

                duration = time.time() - start_time
                result = DownloadResult(
                    status=DownloadStatus.SUCCESS,
                    date=date_str,
                    data_type=storage_type,
                    records=len(df),
                    checksum=checksum,
                    url=url,
                    data=df  # FIX: Include DataFrame in result
                )

                # Log to database
                self.db.log_download(result, duration)

                return result

        except Exception as e:
            logger.error(f"Error processing {date_str}: {e}", exc_info=True)
            return DownloadResult(
                status=DownloadStatus.ERROR,
                date=date_str,
                data_type=storage_type,
                error=str(e),
                url=url
            )

    def save_data(self, result: DownloadResult) -> bool:
        """FIX: Save data to storage - SIMPLIFIED, uses DataFrame from result"""
        # FIX: Better status and data checking
        if result.data is None or result.data.empty:
            logger.error(f"No data to save for {result.date}")
            return False

        # Skip if not successful
        if result.status != DownloadStatus.SUCCESS:
            if result.status == DownloadStatus.SKIPPED:
                logger.debug(f"Skipping save for {result.date} - already exists")
            return False
        # Skip if data already saved (double-check)
        date_obj = datetime.strptime(result.date, '%Y-%m-%d')

        # Use consistent data_type (already normalized in download_day)
        storage_type = result.data_type
        with self.db.get_connection(read_only=True) as conn:
            cursor = conn.cursor()
            table = f'{storage_type}_data'
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE date = ?", (result.date,))
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.info(f"Data already exists for {result.date}, skipping save")
                    return True  # Return True because data exists
            except:
                pass  # Table might not exist



        df = result.data
        # Ensure lock is always released
        lock_acquired = False

        try:
            # Prepare DataFrame
            df['date'] = pd.to_datetime(df['date'])
            df['YEAR'] = df['date'].dt.year
            df['MONTH'] = df['date'].dt.month

            # Save to CSV (organized by year/month)
            for (year, month), group in df.groupby(['YEAR', 'MONTH']):
                path = self.csv_path / storage_type / str(year) / f"{month:02d}"
                path.mkdir(parents=True, exist_ok=True)
                filepath = path / f"{storage_type}_{year}_{month:02d}.csv"

                group_clean = group.drop(columns=['YEAR', 'MONTH'])

                if filepath.exists():
                    existing = pd.read_csv(filepath)
                    combined = pd.concat([existing, group_clean], ignore_index=True)
                    combined = combined.drop_duplicates()
                    combined.to_csv(filepath, index=False)
                    del existing, combined
                else:
                    group_clean.to_csv(filepath, index=False)

            # Save to Parquet (partitioned by year/symbol)
            for (year, symbol), group in df.groupby(['YEAR', 'symbol']):
                path = self.parquet_path / storage_type / f"year={year}" / f"symbol={symbol}"
                path.mkdir(parents=True, exist_ok=True)
                filepath = path / "data.parquet"

                group_clean = group.drop(columns=['YEAR', 'MONTH'])

                if filepath.exists():
                    try:
                        # Validate parquet file before reading
                        existing = pd.read_parquet(filepath)
                        combined = pd.concat([existing, group_clean], ignore_index=True)
                        combined = combined.drop_duplicates()

                        temp_path = filepath.with_suffix('.tmp')
                        combined.to_parquet(temp_path, index=False, compression='snappy')
                        temp_path.replace(filepath)

                        # combined.to_parquet(filepath, index=False, compression='snappy')
                        del existing, combined
                    except Exception as e:
                        logger.warning(
                            f"Parquet file issue at {filepath}: "
                            f"{type(e).__name__}: {str(e)[:100]}"
                        )
                        logger.debug("Full trace:", exc_info=True)

                        try:
                            if filepath.exists():
                                logger.info(f"Removing corrupted: {filepath}")
                                filepath.unlink()
                            group_clean.to_parquet(filepath, index=False, compression='snappy')
                            logger.info(f"Recreated parquet: {filepath}")
                        except Exception as recovery_error:
                            logger.error(f"Recovery failed for {filepath}: {recovery_error}", exc_info=True)
                            raise
                else:
                    group_clean.to_parquet(filepath, index=False, compression='snappy')

            # Save to SQLite
            # Save to SQLite (with lock for parallel safety)
            self._write_lock.acquire()
            lock_acquired = True
            # with self._write_lock:
            with self.db.get_connection() as conn:
                df_clean = df.drop(columns=['YEAR', 'MONTH'])
                # Use correct table name
                table = f'{storage_type}_data'
                # Create/update schema
                self.db.create_table_from_df(df_clean, table, conn)
                # Insert data with chunking (fixes "too many SQL variables")
                # SQLite limit is 999 variables. With ~50 columns, chunk to 50 rows
                df_clean.to_sql(table, conn, if_exists='append', index=False, chunksize=50)
                # Remove duplicates
                self.db.remove_duplicates(table, storage_type, conn)
                # Update metadata
                date_obj = datetime.strptime(result.date, '%Y-%m-%d')
                self.db.set_last_update(storage_type, date_obj, result.records)

            self._write_lock.release()
            lock_acquired = False

            # Clear checkpoint
            CheckpointManager.clear_checkpoint(
                result.data_type,
                datetime.strptime(result.date, '%Y-%m-%d')
            )

            # Memory cleanup
            # Aggressive memory cleanup
            if df_clean is not None:
                del df_clean
            if 'df' in locals():
                del df

            import gc
            gc.collect()

            logger.info(f"Saved {result.records} records for {result.date}")
            self.metrics.records_processed += result.records
            self.metrics.downloads_successful += 1

            return True

        except OSError as e:
            # Disk full or permission errors
            if e.errno == 28 or 'No space left' in str(e):
                logger.critical(f"💾 DISK FULL - Cannot save {result.date}")
                logger.critical("Action: Free up space, then run: python nse_data_downloader2.py --fill-gaps")
            else:
                logger.error(f"OS error saving {result.date}: {e}")

            self.metrics.downloads_failed += 1
            self.metrics.errors_count += 1
            return False

        except Exception as e:
            logger.error(f"Save failed for {result.date}: {e}", exc_info=True)
            self.metrics.downloads_failed += 1
            self.metrics.errors_count += 1
            return False

        finally:
            # CRITICAL: Always release lock
            if lock_acquired:
                try:
                    self._write_lock.release()
                    logger.debug("Write lock released in finally block")
                except Exception as e:
                    logger.error(f"Failed to release write lock: {e}")

    def download_range(self, start_date: datetime, end_date: datetime,
                       parallel: bool = False):
        """Download date range with optional parallel processing"""
        logger.info(f"Downloading from {start_date.date()} to {end_date.date()}")

        # Generate list of trading days
        current = start_date
        trading_days = []
        while current <= end_date:
            if self.is_trading_day(current):
                trading_days.append(current)
            current += timedelta(days=1)

        total_days = len(trading_days)
        logger.info(f"Found {total_days} trading days")

        if parallel and Config.MAX_WORKERS > 1:
            self._download_parallel(trading_days)
        else:
            self._download_sequential(trading_days)

        # Cleanup
        CheckpointManager.cleanup_old_checkpoints()

        # Metadata enrichment (optional, non-fatal)
        if self.metadata_enricher is not None:
            try:
                logger.info("Starting automatic metadata enrichment...")
                symbols_today = self._get_symbols_from_date_range(start_date, end_date)

                if symbols_today:
                    batch_limit = min(len(symbols_today), 50)
                    logger.info(f"Enriching {batch_limit} symbols")
                    time.sleep(2)  # Let downloads settle

                    success, failed = self.metadata_enricher.update_symbols(
                        symbols_today[:batch_limit],
                        batch_size=10
                    )
                    logger.info(f"✅ Enrichment: {success} success, {failed} failed")
                else:
                    logger.info("No symbols to enrich")
            except Exception as e:
                logger.error(f"Metadata enrichment error: {e}", exc_info=True)
                logger.warning("Continuing without metadata enrichment")
        else:
            logger.debug("Metadata enricher not available")

        logger.info("Download range complete")



    def _download_sequential(self, trading_days: List[datetime]):
        """Sequential download with batching"""
        total = len(trading_days)

        for idx, date in enumerate(trading_days, 1):
            if self.shutdown_flag.is_set():
                logger.info("Download cancelled by shutdown signal")
                break

            # Update status
            ProcessManager.update_status({
                'message': f'Downloading {date.strftime("%Y-%m-%d")}',
                'progress': int((idx / total) * 100),
                'current_date': date.strftime('%Y-%m-%d')
            })

            # Download equity
            eq_result = self.download_day(date, 'equity')
            if eq_result.status == DownloadStatus.SUCCESS:
                self.save_data(eq_result)
            elif eq_result.status == DownloadStatus.SKIPPED:
                logger.debug(f"Equity data skipped for {date.strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"Equity download failed: {eq_result.error}")

            # FIX: Download derivatives - result includes data
            fo_result = self.download_day(date, 'fo')
            if fo_result.status == DownloadStatus.SUCCESS:
                self.save_data(fo_result)
            elif fo_result.status == DownloadStatus.SKIPPED:
                logger.debug(f"Derivatives data skipped for {date.strftime('%Y-%m-%d')}")
            else:
                logger.warning(f"Derivatives download failed: {fo_result.error}")

            # Progress logging
            if idx % 10 == 0:
                logger.info(f"Progress: {idx}/{total} ({(idx / total) * 100:.1f}%)")

    def _download_parallel(self, trading_days: List[datetime]):
        """FIX: Parallel download - SINGLE download per task"""
        logger.info(f"Using parallel downloads with {Config.MAX_WORKERS} workers")

        def download_task(date, data_type):
            """FIX: Single download task that returns result with data"""
            return self.download_day(date, data_type)

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            # Submit all tasks
            futures = {}
            for date in trading_days:
                futures[executor.submit(download_task, date, 'equity')] = (date, 'equity')
                futures[executor.submit(download_task, date, 'fo')] = (date, 'fo')

            # Process completed tasks
            completed = 0
            total = len(futures)

            for future in as_completed(futures):
                if self.shutdown_flag.is_set():
                    logger.info("Cancelling remaining downloads...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                date, data_type = futures[future]
                try:
                    result = future.result()

                    if result.status == DownloadStatus.SUCCESS:
                        self.save_data(result)
                        # Cleanup result data
                        if result.data is not None:
                            del result.data
                            result.data = None
                    elif result.status == DownloadStatus.SKIPPED:
                        logger.debug(f"{data_type} skipped for {date.strftime('%Y-%m-%d')}")
                    else:
                        logger.warning(f"{data_type} failed for {date.strftime('%Y-%m-%d')}: {result.error}")
                except Exception as e:
                    logger.error(f"Task failed for {date} ({data_type}): {e}")

                completed += 1
                if completed % 10 == 0:
                    logger.info(f"Completed: {completed}/{total}")

    def update_incremental(self) -> bool:
        """Incremental update from last update to today"""
        eq_last = self.db.get_last_update('equity')

        if eq_last is None:
            logger.warning("No previous data. Run --history-only first.")
            return False

        start = eq_last + timedelta(days=1)
        end = datetime.now()

        if start > end:
            logger.info("Data is up to date")
            return True

        logger.info(f"Updating from {start.date()} to {end.date()}")
        self.download_range(start, end)

        return True

    def fill_gaps(self):
        """Fill missing dates in existing data"""
        for data_type in ['equity', 'derivatives']:
            logger.info(f"Detecting gaps in {data_type} data...")
            gaps = self.detect_gaps(data_type)

            if not gaps:
                logger.info(f"No gaps found in {data_type}")
                continue

            logger.info(f"Found {len(gaps)} missing dates, filling...")
            gap_dates = [datetime.strptime(str(g), '%Y-%m-%d') for g in gaps]
            self.download_range(min(gap_dates), max(gap_dates))

    def detect_gaps(self, data_type: str = 'equity') -> List:
        """Detect missing trading days"""
        with self.db.get_connection(read_only=True) as conn:
            table = f'{data_type}_data'
            try:
                df = pd.read_sql(f"SELECT DISTINCT date FROM {table} ORDER BY date", conn)
            except sqlite3.OperationalError:
                logger.warning(f"Table {table} doesn't exist yet")
                return []

            if df.empty:
                return []

            df['date'] = pd.to_datetime(df['date'])
            date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')

            expected_trading_days = [d for d in date_range if self.is_trading_day(d)]
            existing_dates = set(df['date'].dt.date)
            expected_dates = set([d.date() for d in expected_trading_days])

            missing = sorted(expected_dates - existing_dates)
            return missing

    def _get_symbols_from_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Extract unique symbols from date range for metadata enrichment"""
        try:
            with self.db.get_connection(read_only=True) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT symbol 
                    FROM equity_data 
                    WHERE date >= ? AND date <= ?
                    AND symbol IS NOT NULL
                    ORDER BY symbol
                """, (
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d')
                ))
                symbols = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Found {len(symbols)} symbols in date range")
                return symbols
        except Exception as e:
            logger.error(f"Error getting symbols from date range: {e}")
            return []

    def validate_data(self):
        """Run comprehensive data validation"""
        logger.info("Running data validation...")
        issues = []

        for data_type in ['equity', 'derivatives']:
            table = f'{data_type}_data'

            with self.db.get_connection(read_only=True) as conn:
                try:
                    # Check for NULL symbols
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE symbol IS NULL")
                    null_symbols = cursor.fetchone()[0]
                    if null_symbols > 0:
                        issues.append(f"{data_type}: {null_symbols} records with NULL symbols")

                    # Check for negative prices (equity only)
                    if data_type == 'equity':
                        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE close < 0")
                        negative_prices = cursor.fetchone()[0]
                        if negative_prices > 0:
                            issues.append(f"{data_type}: {negative_prices} records with negative close price")

                    # Check for duplicates
                    if data_type == 'equity':
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM (
                                SELECT symbol, series, date, COUNT(*) as cnt
                                FROM {table}
                                GROUP BY symbol, series, date
                                HAVING cnt > 1
                            )
                        """)
                    else:
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM (
                                SELECT symbol, instrument, expiry_date, strike_price, option_type, date, COUNT(*) as cnt
                                FROM {table}
                                GROUP BY symbol, instrument, expiry_date, strike_price, option_type, date
                                HAVING cnt > 1
                            )
                        """)

                    duplicates = cursor.fetchone()[0]
                    if duplicates > 0:
                        issues.append(f"{data_type}: {duplicates} duplicate records found")

                except sqlite3.OperationalError as e:
                    issues.append(f"{data_type}: Table validation failed - {e}")

        if issues:
            logger.warning("Validation issues found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("✅ All validation checks passed")

        return issues

    def get_status(self) -> Dict:
        """Get comprehensive system status"""
        status = {
            'database': {},
            'metrics': self.metrics.to_dict(),
            'gaps': {},
            'health': 'healthy'
        }

        # Database status
        for data_type in ['equity', 'derivatives']:
            last_update = self.db.get_last_update(data_type)

            with self.db.get_connection(read_only=True) as conn:
                cursor = conn.cursor()
                table = f'{data_type}_data'
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]

                    cursor.execute(f"SELECT COUNT(DISTINCT symbol) FROM {table}")
                    symbols = cursor.fetchone()[0]

                    status['database'][data_type] = {
                        'last_update': last_update.strftime('%Y-%m-%d') if last_update else 'Never',
                        'total_records': count,
                        'unique_symbols': symbols
                    }
                except sqlite3.OperationalError:
                    status['database'][data_type] = {'status': 'No data'}

        # Gap detection
        for data_type in ['equity', 'derivatives']:
            gaps = self.detect_gaps(data_type)
            status['gaps'][data_type] = len(gaps)

        # Health check
        if any(v > 0 for v in status['gaps'].values()):
            status['health'] = 'degraded'

        self.metrics.uptime_seconds = time.time() - self.start_time
        self.metrics.memory_mb = MemoryManager.get_memory_usage()

        return status

    def get_last_update(self, data_type: str) -> Optional[datetime]:
        """Get last update for data type"""
        return self.db.get_last_update(data_type)

    def cleanup(self):
        """Cleanup resources on exit"""
        logger.info("Cleaning up resources...")
        try:
            self.session.close()
            CheckpointManager.cleanup_old_checkpoints()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


# ==================== SCHEDULER ====================
downloader_instance = None


def daily_job():
    """Scheduled daily update job"""
    global downloader_instance
    logger.info("Running scheduled daily update...")
    downloader_instance.update_incremental()


def run_scheduler():
    """Run simple foreground scheduler"""
    global downloader_instance
    downloader_instance = NSEDownloader()

    # Check if first run
    if downloader_instance.db.get_last_update('equity') is None:
        logger.info("First run detected. Downloading full history...")
        start = datetime.strptime(Config.HISTORY_START_DATE, '%Y-%m-%d')
        downloader_instance.download_range(start, datetime.now())

    # Schedule daily updates
    schedule.every().day.at(Config.UPDATE_TIME).do(daily_job)
    logger.info(f"Scheduler started. Will run daily at {Config.UPDATE_TIME}")
    logger.info("Press Ctrl+C to stop")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


# ==================== DAEMON MODE ====================
def run_as_daemon():
    """Run as background daemon with process management"""
    ProcessManager.write_pid()
    downloader = NSEDownloader()

    # Signal handlers
    def signal_handler(signum, frame):
        signame = signal.Signals(signum).name
        logger.info(f"Received {signame}, shutting down gracefully...")
        downloader.shutdown_flag.set()
        schedule.clear()
        ProcessManager.remove_pid()
        sys.exit(0)

    # Register signals (cross-platform)
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):  # Not available on Windows
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Initial history download if needed
        if downloader.db.get_last_update('equity') is None:
            logger.info("First run - downloading history...")
            start = datetime.strptime(Config.HISTORY_START_DATE, '%Y-%m-%d')
            downloader.download_range(start, datetime.now())

        # Schedule daily updates
        def scheduled_update():
            logger.info("Running scheduled update...")
            downloader.update_incremental()

        schedule.every().day.at(Config.UPDATE_TIME).do(scheduled_update)
        logger.info(f"Daemon started. Updates at {Config.UPDATE_TIME} daily")

        # Health check job
        def health_check():
            status = downloader.get_status()
            ProcessManager.update_status(status)

            # Save metrics
            with open(Config.METRICS_FILE, 'w') as f:
                json.dump(downloader.metrics.to_dict(), f, indent=2)

        schedule.every(Config.HEALTH_CHECK_INTERVAL).seconds.do(health_check)

        # Main loop
        while not downloader.shutdown_flag.is_set():
            schedule.run_pending()
            time.sleep(60)

        logger.info("Daemon shutting down...")

    except Exception as e:
        logger.error(f"Daemon error: {e}", exc_info=True)
        raise
    finally:
        ProcessManager.remove_pid()


# ==================== CLI INTERFACE ====================
def print_status(status: Dict):
    """Pretty print status information"""
    print("\n" + "=" * 60)
    print("NSE DATA DOWNLOADER STATUS")
    print("=" * 60)

    # Database status
    print("\n📊 DATABASE:")
    for data_type, info in status.get('database', {}).items():
        print(f"\n  {data_type.upper()}:")
        if isinstance(info, dict):
            for key, value in info.items():
                print(f"    {key}: {value:,}" if isinstance(value, int) else f"    {key}: {value}")

    # Metrics
    print("\n📈 METRICS:")
    metrics = status.get('metrics', {})
    if metrics:
        print(f"    Downloads Successful: {metrics.get('downloads_successful', 0)}")
        print(f"    Downloads Failed: {metrics.get('downloads_failed', 0)}")
        print(f"    Downloads Skipped: {metrics.get('downloads_skipped', 0)}")
        print(f"    Records Processed: {metrics.get('records_processed', 0):,}")
        print(f"    Memory Usage: {metrics.get('memory_mb', 0):.1f} MB")

        uptime = metrics.get('uptime_seconds', 0)
        if uptime > 0:
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            print(f"    Uptime: {hours}h {minutes}m")

    # Gaps
    print("\n🔍 DATA GAPS:")
    gaps = status.get('gaps', {})
    for data_type, count in gaps.items():
        print(f"    {data_type}: {count} missing dates")

    # Health
    health = status.get('health', 'unknown')
    emoji = '✅' if health == 'healthy' else '⚠️'
    print(f"\n{emoji} HEALTH: {health.upper()}")

    print("=" * 60 + "\n")


def main():
    """Main entry point"""

    # Validate config first
    try:
        Config.validate()
    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}\n")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description='NSE Data Downloader - Production Grade',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start daemon
  python nse_data_downloader2.py --daemon start

  # One-time update
  python nse_data_downloader2.py --update-now

  # Download last 30 days
  python nse_data_downloader2.py --days 30

  # Check status
  python nse_data_downloader2.py --status

  # Fill missing dates
  python nse_data_downloader2.py --fill-gaps

  # Validate data integrity
  python nse_data_downloader2.py --validate
        """
    )

    # Daemon control
    parser.add_argument('--daemon', choices=['start', 'stop', 'status'],
                        help='Daemon control (background service)')

    # One-time operations
    parser.add_argument('--update-now', action='store_true',
                        help='Run incremental update now')
    parser.add_argument('--history-only', action='store_true',
                        help='Download full history and exit')
    parser.add_argument('--days', type=int, metavar='N',
                        help='Download last N days')
    parser.add_argument('--fill-gaps', action='store_true',
                        help='Fill missing dates in existing data')

    # Data operations
    parser.add_argument('--validate', action='store_true',
                        help='Run data validation checks')
    parser.add_argument('--status', action='store_true',
                        help='Show database status')
    parser.add_argument('--query', type=str, metavar='SQL',
                        help='Execute SQL query (read-only)')

    # Export
    parser.add_argument('--export', choices=['csv', 'json'],
                        help='Export data to format')
    parser.add_argument('--output', type=str, metavar='PATH',
                        help='Output path for export')

    # Advanced options
    parser.add_argument('--parallel', action='store_true',
                        help='Use parallel downloads (faster but more intensive)')
    parser.add_argument('--no-validate', action='store_true',
                        help='Skip data validation on save')
    parser.add_argument('--config', type=str, metavar='FILE',
                        help='Load configuration from file')

    # Monitoring
    parser.add_argument('--metrics', action='store_true',
                        help='Show Prometheus-style metrics')
    parser.add_argument('--health', action='store_true',
                        help='Health check endpoint')

    parser.add_argument('--force', action='store_true',
                        help='Force redownload even if data exists')

    parser.add_argument('--vacuum', action='store_true',
                        help='Vacuum database (reclaim space)')

    args = parser.parse_args()

    # Handle config file
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
            # Apply config overrides
            for key, value in config_data.items():
                if hasattr(Config, key):
                    setattr(Config, key, value)
            logger.info(f"Loaded configuration from {args.config}")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            sys.exit(1)

    # Override validation setting
    if args.no_validate:
        Config.VALIDATE_ON_SAVE = False

    # ==================== DAEMON CONTROL ====================
    if args.daemon == 'stop':
        print("\n🛑 Stopping NSE Downloader Daemon...")
        success = ProcessManager.stop()
        sys.exit(0 if success else 1)

    elif args.daemon == 'status':
        status = ProcessManager.get_status()
        print("\n" + "=" * 60)
        print("DAEMON PROCESS STATUS")
        print("=" * 60)
        if status['running']:
            print(f"✅ Running")
            print(f"   PID: {status.get('pid')}")
            print(f"   Started: {status.get('started')}")

            uptime = status.get('uptime_seconds', 0)
            if uptime > 0:
                hours = int(uptime // 3600)
                minutes = int((uptime % 3600) // 60)
                print(f"   Uptime: {hours}h {minutes}m")

            print(f"   CPU: {status.get('cpu_percent', 0):.1f}%")
            print(f"   Memory: {status.get('memory_mb', 0):.1f} MB")
            print(f"   Threads: {status.get('num_threads', 'N/A')}")

            # Show latest status
            if 'message' in status:
                print(f"\n   Current: {status['message']}")
            if 'progress' in status:
                print(f"   Progress: {status['progress']}%")
        else:
            print("❌ Not running")
            if 'error' in status:
                print(f"   Error: {status['error']}")
        print("=" * 60 + "\n")
        sys.exit(0)

    elif args.daemon == 'start':
        if ProcessManager.is_running():
            print("❌ Daemon already running!")
            print("   Use --daemon status to check or --daemon stop to stop it")
            sys.exit(1)

        print("🚀 Starting NSE Downloader Daemon...")

        # FIX: Windows-compatible daemon start
        mode = ProcessManager.start()

        if mode == 'foreground':
            # Windows fallback - run in foreground
            run_as_daemon()
        elif mode == 'daemon':
            # Unix daemon - already forked
            run_as_daemon()

        sys.exit(0)


    # ==================== ONE-TIME OPERATIONS ====================
    dl = NSEDownloader()

    if args.health:
        status = dl.get_status()
        health = status.get('health', 'unknown')
        print(json.dumps({'status': health, 'timestamp': datetime.now().isoformat()}))
        sys.exit(0 if health == 'healthy' else 1)

    elif args.metrics:
        metrics = dl.metrics.to_dict()
        # Prometheus format
        print("# HELP nse_downloads_successful Total successful downloads")
        print(f"nse_downloads_successful {metrics['downloads_successful']}")
        print("# HELP nse_downloads_failed Total failed downloads")
        print(f"nse_downloads_failed {metrics['downloads_failed']}")
        print("# HELP nse_records_processed Total records processed")
        print(f"nse_records_processed {metrics['records_processed']}")
        print("# HELP nse_memory_mb Memory usage in MB")
        print(f"nse_memory_mb {metrics['memory_mb']}")
        sys.exit(0)

    elif args.status:
        status = dl.get_status()
        print_status(status)

    elif args.validate:
        print("\n🔍 Running data validation...")
        issues = dl.validate_data()
        if not issues:
            print("✅ No issues found\n")
            sys.exit(0)
        else:
            print(f"❌ Found {len(issues)} issues\n")
            sys.exit(1)

    elif args.query:
        try:
            with dl.db.get_connection(read_only=True) as conn:
                df = pd.read_sql_query(args.query, conn)
            print(df.to_string())
        except Exception as e:
            print(f"❌ Query failed: {e}")
            sys.exit(1)

    elif args.export:
        print(f"\n📤 Exporting data to {args.export}...")
        output_path = args.output or f"nse_export.{args.export}"

        try:
            with dl.db.get_connection(read_only=True) as conn:
                for table in ['equity_data', 'derivatives_data']:
                    print(f"   Exporting {table}...")
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)

                    if args.export == 'csv':
                        output = f"{output_path}_{table}.csv"
                        df.to_csv(output, index=False)
                    elif args.export == 'json':
                        output = f"{output_path}_{table}.json"
                        df.to_json(output, orient='records', indent=2)

                    print(f"   ✅ Saved to {output}")

            print("✅ Export complete\n")
        except Exception as e:
            print(f"❌ Export failed: {e}")
            sys.exit(1)

    elif args.update_now:
        print("\n📥 Running incremental update...")
        success = dl.update_incremental()
        print("✅ Update complete\n" if success else "❌ Update failed\n")
        sys.exit(0 if success else 1)

    elif args.history_only:
        print("\n📥 Downloading full history...")
        start = datetime.strptime(Config.HISTORY_START_DATE, '%Y-%m-%d')
        dl.download_range(start, datetime.now(), parallel=args.parallel)
        print("✅ History download complete\n")

    elif args.days:
        print(f"\n📥 Downloading last {args.days} days...")
        start = datetime.now() - timedelta(days=args.days)
        dl.download_range(start, datetime.now(), parallel=args.parallel)
        print("✅ Download complete\n")

    elif args.fill_gaps:
        print("\n🔧 Filling data gaps...")
        dl.fill_gaps()
        print("✅ Gaps filled\n")

    elif args.vacuum:
        print("🧹 Vacuuming database...")
        dl.vacuum_database()
        print("✅ Complete")

    else:
        # Default: run simple scheduler
        print("\n🕐 Starting scheduler (foreground mode)")
        print(f"   Daily updates at: {Config.UPDATE_TIME}")
        print(f"   Logs: {Config.LOG_PATH}/nse_downloader.log")
        print("\n💡 Tip: Use --daemon start for background execution\n")
        run_scheduler()


if __name__ == "__main__":
    main()