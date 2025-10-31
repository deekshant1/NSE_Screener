"""
NSE Data Downloader - Enhanced Flask Web UI
Run: python nse_web_ui.py
Then open: http://localhost:5000
"""

from flask import Flask, render_template_string, jsonify, request, session
import sqlite3
import pandas as pd
import threading
import time
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
from functools import lru_cache, wraps
from hashlib import sha256

# Import the downloader from the main script
try:
    from nse_data_downloader2 import (NSEDownloader, Config, DatabaseManager, DownloadStatus, logger)
except ImportError:
    print("ERROR: Cannot import NSEDownloader. Make sure nse_data_downloader.py is in the same directory!")
    exit(1)

BASE_PATH = Config.BASE_PATH
HISTORY_START_DATE = Config.HISTORY_START_DATE

# Query cache
query_cache = {}
CACHE_TTL = 300  # 5 minutes


# Rate limiting
rate_limit_store = {}

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(32)  # Secure secret key

app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=2)

# Global state
download_state = {
    'running': False,
    'task': None,
    'progress': 0,
    'message': '',
    'current': 0,
    'total': 0,
    'actual_downloaded': 0,  # Track actual downloads vs skipped
    'start_time': None,
    'errors': []
}

downloader = None


def init_downloader():
    global downloader
    if downloader is None:
        downloader = NSEDownloader()


def rate_limit(requests_per_minute=30):
    """Rate limit decorator"""

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            client_ip = request.remote_addr
            now = time.time()

            if client_ip in rate_limit_store:
                rate_limit_store[client_ip] = [
                    t for t in rate_limit_store[client_ip]
                    if now - t < 60
                ]
            else:
                rate_limit_store[client_ip] = []

            if len(rate_limit_store[client_ip]) >= requests_per_minute:
                return jsonify({'error': 'Rate limit exceeded. Try again in 1 minute.'}), 429

            rate_limit_store[client_ip].append(now)
            return f(*args, **kwargs)

        return wrapper

    return decorator


def get_db_connection(read_only=True):
    """Get database connection with proper WAL mode and read-only settings"""
    db_path = Path(BASE_PATH) / "nse_data.db"

    if not db_path.exists():
        return None

    uri = f"file:{db_path}"
    if read_only:
        uri += "?mode=ro"

    conn = sqlite3.connect(uri, timeout=30.0, uri=True)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA query_only=ON") if read_only else None

    return conn

@contextmanager
def db_connection(read_only=True):
    """Context manager for safe database connections"""
    conn = get_db_connection(read_only)
    try:
        yield conn
    finally:
        if conn:
            conn.close()


def get_stats():
    """Get database statistics with retry logic"""
    init_downloader()

    with db_connection(read_only=True) as conn:
        if conn is None:
            return {
                'equity_count': 0,
                'derivatives_count': 0,
                'equity_last_update': None,
                'derivatives_last_update': None,
                'symbols_count': 0,
                'date_range': None
            }

        try:
            eq_count = int(pd.read_sql("SELECT COUNT(*) as cnt FROM equity_data", conn).iloc[0]['cnt'])
        except:
            eq_count = 0

        try:
            deriv_count = int(pd.read_sql("SELECT COUNT(*) as cnt FROM derivatives_data", conn).iloc[0]['cnt'])
        except:
            deriv_count = 0

        try:
            symbols_count = int(
                pd.read_sql("SELECT COUNT(DISTINCT symbol) as cnt FROM equity_data", conn).iloc[0]['cnt'])
        except:
            symbols_count = 0

        eq_last = downloader.get_last_update('equity')
        deriv_last = downloader.get_last_update('derivatives')

        try:
            date_range = pd.read_sql("SELECT MIN(date) as min_date, MAX(date) as max_date FROM equity_data", conn)
            min_date = date_range.iloc[0]['min_date']
            max_date = date_range.iloc[0]['max_date']
            date_range_str = f"{min_date} to {max_date}" if min_date else None
        except:
            date_range_str = None

        return {
            'equity_count': eq_count,
            'derivatives_count': deriv_count,
            'equity_last_update': eq_last.strftime('%Y-%m-%d') if eq_last else None,
            'derivatives_last_update': deriv_last.strftime('%Y-%m-%d') if deriv_last else None,
            'symbols_count': symbols_count,
            'date_range': date_range_str
        }

def background_download(task_type, days=None):
    """Background download task - all progress and error tracking issues"""
    global download_state

    try:
        # download_state['running'] = True
        download_state.update(running=True)
        download_state['errors'] = []  # Clear old errors
        download_state['start_time'] = time.time()
        download_state['current'] = 0
        download_state['actual_downloaded'] = 0

        init_downloader()

        if task_type == 'full_history':
            download_state['message'] = 'Downloading full history...'
            start = datetime.strptime(HISTORY_START_DATE, '%Y-%m-%d')
            end = datetime.now()

            # Count trading days only
            current = start
            trading_days = 0
            while current <= end:
                if downloader.is_trading_day(current):
                    trading_days += 1
                current += timedelta(days=1)

            download_state['total'] = trading_days * 2  # equity + derivatives

            original_download_day = downloader.download_day

            def tracked_download_day(date, data_type):
                result = original_download_day(date, data_type)
                download_state['current'] += 1

                # # FIX: Handle DownloadResult object, not dict
                # if result.status == DownloadStatus.SUCCESS:
                #     download_state['actual_downloaded'] += 1
                #     download_state['message'] = f'✓ Downloaded {data_type} for {date.strftime("%Y-%m-%d")}'
                # elif result.status == DownloadStatus.SKIPPED:
                #     download_state[
                #         'message'] = f'⊘ Skipped {data_type} for {date.strftime("%Y-%m-%d")} (already exists)'
                # elif result.status == DownloadStatus.ERROR:
                #     error_msg = f"{data_type} on {date.strftime('%Y-%m-%d')}: {result.error or 'unknown'}"
                #     download_state['errors'].append(error_msg)
                #     download_state['message'] = f'✗ Failed {data_type} for {date.strftime("%Y-%m-%d")}'
                #
                # if download_state['total'] > 0:
                #     download_state['progress'] = min(100,
                #                                      int((download_state['current'] / download_state['total']) * 100))

                # Handle DownloadResult object or dict
                if hasattr(result, 'status'):
                    status = result.status.value if hasattr(result.status, 'value') else str(result.status)
                    is_success = (status == 'success')
                    is_skipped = (status == 'skipped')
                    error_msg = getattr(result, 'error', None)
                else:
                    is_success = (result.get('status') == 'success')
                    is_skipped = (result.get('status') == 'skipped')
                    error_msg = result.get('error')

                if is_success:
                    download_state['actual_downloaded'] += 1
                    download_state['message'] = f'✓ Downloaded {data_type} for {date.strftime("%Y-%m-%d")}'
                elif is_skipped:
                    download_state['message'] = f'⊘ Skipped {data_type} for {date.strftime("%Y-%m-%d")}'
                else:
                    if error_msg:
                        download_state['errors'].append(f"{data_type} on {date.strftime('%Y-%m-%d')}: {error_msg}")
                    download_state['message'] = f'✗ Failed {data_type} for {date.strftime("%Y-%m-%d")}'

                if download_state['total'] > 0:
                    download_state['progress'] = min(100,
                                                     int((download_state['current'] / download_state['total']) * 100))
                else:
                    download_state['progress'] = 0
                return result

            downloader.download_day = tracked_download_day
            downloader.download_range(start, end)
            downloader.download_day = original_download_day

        elif task_type == 'incremental':
            download_state['message'] = 'Running incremental update...'
            download_state['total'] = -1  # Indeterminate

            original_download_day = downloader.download_day

            def tracked_download_day(date, data_type):
                result = original_download_day(date, data_type)
                download_state['current'] += 1

                if result.get('status') == 'success':
                    download_state['actual_downloaded'] += 1
                    download_state['message'] = f'✓ Updated {data_type} for {date.strftime("%Y-%m-%d")}'
                elif result.get('status') == 'skipped':
                    download_state['message'] = f'⊘ Skipped {data_type} for {date.strftime("%Y-%m-%d")}'
                elif result.get('status') == 'error':
                    error_msg = f"{data_type} on {date.strftime('%Y-%m-%d')}: {result.get('reason', 'unknown')}"
                    download_state['errors'].append(error_msg)

                return result

            downloader.download_day = tracked_download_day
            success = downloader.update_incremental()
            downloader.download_day = original_download_day

            if success:
                download_state['progress'] = 100
            else:
                download_state['message'] = 'No previous data found. Please run full history first.'
                download_state['errors'].append('Database not initialized. Run full history download.')

        elif task_type == 'last_n_days' and days:
            download_state['message'] = f'Downloading last {days} days...'
            start = datetime.now() - timedelta(days=days)
            end = datetime.now()

            # Count trading days
            current = start
            trading_days = 0
            while current <= end:
                if downloader.is_trading_day(current):
                    trading_days += 1
                current += timedelta(days=1)

            download_state['total'] = trading_days * 2

            original_download_day = downloader.download_day

            def tracked_download_day(date, data_type):
                result = original_download_day(date, data_type)
                download_state['current'] += 1

                # FIX: Handle DownloadResult object
                if result.status == DownloadStatus.SUCCESS:
                    download_state['actual_downloaded'] += 1
                    download_state['message'] = f'✓ Downloaded {data_type} for {date.strftime("%Y-%m-%d")}'
                elif result.status == DownloadStatus.SKIPPED:
                    download_state['message'] = f'⊘ Skipped {data_type} for {date.strftime("%Y-%m-%d")}'
                elif result.status == DownloadStatus.ERROR:
                    error_msg = f"{data_type} on {date.strftime('%Y-%m-%d')}: {result.error or 'unknown'}"
                    download_state['errors'].append(error_msg)

                if download_state['total'] > 0:
                    download_state['progress'] = min(100,
                                                     int((download_state['current'] / download_state['total']) * 100))
                else:
                    download_state['progress'] = 0
                return result

            downloader.download_day = tracked_download_day
            downloader.download_range(start, end)
            downloader.download_day = original_download_day

        download_state['message'] = f'Download complete! {download_state["actual_downloaded"]} new records added.'
        download_state['progress'] = 100

    except Exception as e:
        download_state['message'] = f'Error: {str(e)}'
        download_state['errors'].append(str(e))

    finally:
        download_state['running'] = False
        download_state['task'] = None


@app.route('/')
def index():
    session.permanent = True  # Extend session for long downloads
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
    return render_template_string(HTML_TEMPLATE, csrf_token=session['csrf_token'])


@app.route('/api/stats')
def api_stats():
    """Stats with caching"""

    @lru_cache(maxsize=1)
    def get_cached_stats(cache_key):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return get_stats()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.5)

    # Cache key changes every 5 minutes
    cache_key = int(time.time() / 300)
    try:
        stats = get_cached_stats(cache_key)
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/status')
def api_status():
    elapsed = 0
    if download_state['start_time']:
        elapsed = int(time.time() - download_state['start_time'])

    return jsonify({
        'running': download_state['running'],
        'progress': download_state['progress'],
        'message': download_state['message'],
        'current': download_state['current'],
        'total': download_state['total'],
        'actual_downloaded': download_state['actual_downloaded'],
        'elapsed': elapsed,
        'errors': download_state['errors']
    })

# Standard response wrapper
def api_response(success: bool, data=None, error=None, metadata=None):
    """Standardized API response format"""
    response = {
    'success': success,
    'timestamp': datetime.now().isoformat(), 'data': data,
    'error': error, 'metadata': metadata or {}
    }
    return jsonify(response)

@app.route('/api/check_gaps')
def api_check_gaps():
    """Check for missing dates in data"""
    try:
        init_downloader()
        eq_gaps = downloader.detect_gaps('equity')
        fo_gaps = downloader.detect_gaps('derivatives')

        return jsonify({
            'equity_gaps': [str(d) for d in eq_gaps[:50]],
            'equity_total': len(eq_gaps),
            'derivatives_gaps': [str(d) for d in fo_gaps[:50]],
            'derivatives_total': len(fo_gaps)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/start_download', methods=['POST'])
def api_start_download():
    global download_state
    # Verify backend config before starting
    if not Config.VALIDATE_ON_SAVE:
        logger.warning("Backend validation is disabled - may allow invalid data")
    # Basic CSRF check
    csrf_token = request.json.get('csrf_token')
    if csrf_token != session.get('csrf_token'):
        return jsonify({'error': 'Invalid request'}), 403

    if download_state['running']:
        return jsonify({'error': 'Download already running'}), 400

    data = request.json
    task_type = data.get('type')
    days = data.get('days')

    # Server-side validation
    if task_type == 'last_n_days':
        if not days or days < 1 or days > 365:
            return jsonify({'error': 'Days must be between 1 and 365'}), 400

    download_state = {
        'running': True,
        'task': None,
        'progress': 0,
        'message': 'Starting...',
        'current': 0,
        'total': 0,
        'actual_downloaded': 0,
        'start_time': None,
        'errors': []
    }

    thread = threading.Thread(target=background_download, args=(task_type, days))
    thread.daemon = True
    thread.start()
    download_state['task'] = thread

    return jsonify({'status': 'started'})


@app.route('/api/stop_download', methods=['POST'])
def api_stop_download():
    # CSRF check
    csrf_token = request.json.get('csrf_token')
    if csrf_token != session.get('csrf_token'):
        return jsonify({'error': 'Invalid request'}), 403

    download_state['running'] = False
    download_state['message'] = 'Stopped by user'
    return jsonify({'status': 'stopped'})


@app.route('/api/query', methods=['POST'])
@rate_limit(requests_per_minute=30)
def api_query():
    """Execute custom SQL query - Cross-platform timeout"""
    try:
        init_downloader()
        sql = request.json.get('sql', '')

        if not sql.strip():
            return jsonify({'error': 'Empty query'}), 400

        if not sql.strip().upper().startswith('SELECT'):
            return jsonify({'error': 'Only SELECT queries allowed'}), 400

        # ✅ AUTO-JOIN metadata if querying equity_data
        if 'equity_data' in sql.lower() and 'symbol_metadata' not in sql.lower():
            # Enhance query to include metadata
            sql = sql.replace(
                'FROM equity_data',
                '''FROM equity_data 
                   LEFT JOIN symbol_metadata USING (symbol)'''
            )
            logger.info("Auto-joined symbol_metadata")

        # Use timeout-protected query
        try:
            df = downloader.query_with_timeout(sql, timeout=30)
        except TimeoutError:
            return jsonify({
                'error': 'Query timeout (30s). Try adding LIMIT clause or simplifying the query.',
                'suggestion': 'Example: Add "LIMIT 1000" to your query'
            }), 500

        result = {
            'columns': df.columns.tolist(),
            'data': df.values.tolist(),
            'rows': len(df)
        }
        # return jsonify(df.to_dict('records'))

        # Explicit cleanup for large result sets
        del df
        if result['rows'] > 1000:
            import gc
            gc.collect()

        return jsonify(result)

    except TimeoutError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/tables')
def api_tables():
    """Get list of tables and their columns - Add validation"""
    try:
        init_downloader()
        db_path = Path(BASE_PATH) / "nse_data.db"

        if not db_path.exists():
            return jsonify({
                'tables': [],
                'message': 'Database not found. Please download data first.'
            })

        conn = get_db_connection(read_only=True) # sqlite3.connect(str(db_path))
        if conn is None:
            return jsonify({
                'tables': [],
                'message': 'Database not found. Please download data first.'
            })

        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = cursor.fetchall()

        if not tables:
            conn.close()
            return jsonify({
                'tables': [],
                'message': 'No data tables found. Please download data first.'
            })

        result = []
        for (table_name,) in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            try:
                count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn).iloc[0]['cnt']
                count = int(count)
            except:
                count = 0

            result.append({
                'name': table_name,
                'columns': [{'name': col[1], 'type': col[2]} for col in columns],
                'row_count': count
            })

        conn.close()
        return jsonify({'tables': result})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/filter-options')
def api_filter_options():
    """Get filter options including metadata"""
    try:
        init_downloader()

        with db_connection(read_only=True) as conn:
            # Get sectors from metadata table
            sectors = pd.read_sql("""
                SELECT DISTINCT sector 
                FROM symbol_metadata 
                WHERE sector IS NOT NULL AND sector != 'Unknown'
                ORDER BY sector
            """, conn)

            # Get industries
            industries = pd.read_sql("""
                SELECT DISTINCT industry 
                FROM symbol_metadata 
                WHERE industry IS NOT NULL AND industry != 'Unknown'
                ORDER BY industry
            """, conn)

            return jsonify({
                'sectors': sectors['sector'].tolist(),
                'industries': industries['industry'].tolist()
            })
    except Exception as e:
        logger.error(f"Error fetching filter options: {e}")
        return jsonify({'sectors': [], 'industries': []})

@app.route('/api/add_indicators', methods=['POST'])
def api_add_indicators():
    """Add technical indicators to database"""
    try:
        from nse_indicators import add_indicators_to_database

        data = request.json
        symbols = data.get('symbols')  # None = all symbols
        indicators = data.get('indicators', ['sma', 'rsi', 'macd'])

        # Run in background thread
        def add_indicators_task():
            add_indicators_to_database(
                Config.DB_PATH,
                table='equity_data',
                symbols=symbols,
                indicators=indicators
            )

        thread = threading.Thread(target=add_indicators_task)
        thread.daemon = True
        thread.start()

        return jsonify({'status': 'started', 'indicators': indicators})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    checks = {}

    # Database connectivity
    try:
        conn = sqlite3.connect(Config.DB_PATH, timeout=5)
        conn.execute("SELECT 1")
        conn.close()
        checks['database'] = 'healthy'
    except:
        checks['database'] = 'unhealthy'

    # Disk space
    import shutil
    stat = shutil.disk_usage(Config.BASE_PATH)
    free_gb = stat.free / (1024 ** 3)
    checks['disk_space_gb'] = round(free_gb, 2)
    checks['disk_status'] = 'healthy' if free_gb > 10 else 'warning'

    # Data freshness
    try:
        with db_connection(read_only=True) as conn:
            df = pd.read_sql("SELECT MAX(date) as last_date FROM equity_data", conn)
            last_date = df['last_date'][0]
            age_days = (datetime.now() - datetime.strptime(last_date, '%Y-%m-%d')).days
            checks['data_freshness_days'] = age_days
            checks['data_status'] = 'healthy' if age_days <= 2 else 'stale'
    except:
        checks['data_status'] = 'no_data'

    overall = 'healthy' if all(
        v in ['healthy', 'pass'] for k, v in checks.items() if '_status' in k
    ) else 'unhealthy'

    checks['overall'] = overall
    checks['timestamp'] = datetime.now().isoformat()

    return jsonify(checks), 200 if overall == 'healthy' else 503


# HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Data Downloader - Professional Edition</title>
    <style>
        /* REUSING ALL EXISTING STYLES FROM PREVIOUS VERSION */
        /* (Keeping CSS exactly the same to save space - no changes needed in styling) */
        * { margin: 0; padding: 0; box-sizing: border-box; }
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --success: #10b981;
            --success-dark: #059669;
            --warning: #f59e0b;
            --error: #ef4444;
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-tertiary: #334155;
            --text-primary: #f1f5f9;
            --text-secondary: #cbd5e1;
            --text-muted: #94a3b8;
            --border: #334155;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.6;
        }
        .header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 1.5rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(10px);
        }
        .header-content {
            max-width: 1600px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 1rem;
        }
        .logo {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        .logo-icon {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--primary) 0%, #8b5cf6 100%);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
            box-shadow: var(--shadow);
        }
        h1 {
            font-size: 1.75rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .subtitle {
            font-size: 0.875rem;
            color: var(--text-muted);
            font-weight: 500;
        }
        .header-actions {
            display: flex;
            gap: 0.75rem;
            align-items: center;
        }
        .container {
            max-width: 1600px;
            margin: 0 auto;
            padding: 2rem;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        .stat-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.5rem;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: linear-gradient(180deg, var(--primary) 0%, #8b5cf6 100%);
        }
        .stat-card:hover {
            transform: translateY(-4px);
            box-shadow: var(--shadow-lg);
            border-color: var(--primary);
        }
        .stat-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1rem;
        }
        .stat-icon {
            width: 40px;
            height: 40px;
            background: rgba(99, 102, 241, 0.1);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }
        .stat-badge {
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            background: rgba(16, 185, 129, 0.1);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }
        .stat-label {
            font-size: 0.875rem;
            color: var(--text-muted);
            font-weight: 500;
            margin-bottom: 0.5rem;
        }
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 0.25rem;
        }
        .stat-change {
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        .section {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 2rem;
        }
        .section-header {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1.5rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
        }
        .section-icon {
            width: 36px;
            height: 36px;
            background: rgba(99, 102, 241, 0.1);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
        }
        .section-title {
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--text-primary);
            flex: 1;
        }
        .section-description {
            color: var(--text-secondary);
            margin-bottom: 1.5rem;
            font-size: 0.9375rem;
        }
        .btn-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        .btn {
            background: linear-gradient(135deg, var(--primary) 0%, #8b5cf6 100%);
            color: white;
            border: none;
            padding: 1rem 1.5rem;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.9375rem;
            cursor: pointer;
            transition: all 0.3s ease;
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: 0.5rem;
            box-shadow: var(--shadow);
            position: relative;
            overflow: hidden;
        }
        .btn::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: linear-gradient(135deg, rgba(255,255,255,0.1) 0%, rgba(255,255,255,0) 100%);
            opacity: 0;
            transition: opacity 0.3s ease;
        }
        .btn:hover:not(:disabled)::before {
            opacity: 1;
        }
        .btn:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }
        .btn:active:not(:disabled) {
            transform: translateY(0);
        }
        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        .btn-secondary {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
        }
        .btn-success {
            background: linear-gradient(135deg, var(--success) 0%, var(--success-dark) 100%);
        }
        .btn-small {
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            flex-direction: row;
            align-items: center;
            justify-content: center;
        }
        .btn-hint {
            font-size: 0.8125rem;
            font-weight: 500;
            opacity: 0.8;
        }
        .btn-icon {
            margin-right: 0.5rem;
        }
        .btn-loading {
            pointer-events: none;
            opacity: 0.7;
        }
        .input-group {
            display: flex;
            gap: 1rem;
            align-items: stretch;
            margin-bottom: 1rem;
            flex-wrap: wrap;
        }
        .input-group label {
            min-width: 120px;
            display: flex;
            align-items: center;
            font-weight: 600;
            color: var(--text-secondary);
            font-size: 0.9375rem;
        }
        .input-group input {
            flex: 1;
            min-width: 200px;
            padding: 0.75rem 1rem;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 10px;
            color: var(--text-primary);
            font-size: 1rem;
            transition: all 0.3s ease;
        }
        .input-group input:focus {
            outline: none;
            border-color: var(--primary);
            background: var(--bg-secondary);
        }
        .progress-container {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 2rem;
            display: none;
        }
        .progress-container.active {
            display: block;
            animation: slideDown 0.3s ease;
        }
        @keyframes slideDown {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .progress-header {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        .progress-pulse {
            width: 12px;
            height: 12px;
            background: var(--success);
            border-radius: 50%;
            animation: pulse 2s ease-in-out infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.6; transform: scale(1.2); }
        }
        .progress-message {
            font-size: 1rem;
            font-weight: 600;
            flex: 1;
            color: var(--text-primary);
        }
        .progress-bar {
            background: var(--bg-tertiary);
            height: 12px;
            border-radius: 10px;
            overflow: hidden;
            margin-bottom: 1.5rem;
            position: relative;
        }
        .progress-fill {
            background: linear-gradient(90deg, var(--success) 0%, var(--success-dark) 100%);
            height: 100%;
            transition: width 0.3s ease;
            border-radius: 10px;
            position: relative;
            overflow: hidden;
        }
        .progress-fill::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            bottom: 0;
            right: 0;
            background: linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.3) 50%, transparent 100%);
            animation: shimmer 2s infinite;
        }
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        .progress-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        .progress-stat {
            background: var(--bg-tertiary);
            padding: 1rem;
            border-radius: 10px;
            text-align: center;
        }
        .progress-stat-label {
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }
        .progress-stat-value {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--text-primary);
        }
        .progress-errors {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            padding: 1rem;
            border-radius: 10px;
            margin-bottom: 1rem;
            display: none;
            max-height: 200px;
            overflow-y: auto;
        }
        .progress-errors.active {
            display: block;
        }
        .progress-errors h4 {
            color: var(--error);
            margin-bottom: 0.5rem;
            font-size: 0.875rem;
        }
        .progress-errors ul {
            list-style: none;
            font-size: 0.8125rem;
            color: var(--text-secondary);
        }
        .progress-errors li {
            padding: 0.25rem 0;
        }
        .query-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1rem;
            flex-wrap: wrap;
            gap: 1rem;
        }
        textarea {
            width: 100%;
            min-height: 150px;
            padding: 1rem;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 12px;
            color: var(--text-primary);
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
            font-size: 0.9375rem;
            resize: vertical;
            transition: all 0.3s ease;
            line-height: 1.6;
        }
        textarea:focus {
            outline: none;
            border-color: var(--primary);
            background: var(--bg-secondary);
        }
        .query-actions {
            display: flex;
            gap: 1rem;
            margin-top: 1rem;
            flex-wrap: wrap;
        }
        .query-examples {
            display: flex;
            gap: 0.75rem;
            flex-wrap: wrap;
            margin-top: 1rem;
        }
        .example-btn {
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.3s ease;
            font-weight: 500;
        }
        .example-btn:hover {
            background: var(--bg-secondary);
            border-color: var(--primary);
        }
        .results-container {
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            padding: 1.5rem;
            border-radius: 12px;
            margin-top: 1.5rem;
            max-height: 600px;
            overflow: auto;
            display: none;
        }
        .results-container.active {
            display: block;
            animation: fadeIn 0.3s ease;
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        .results-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border);
            flex-wrap: wrap;
            gap: 1rem;
        }
        .results-actions {
            display: flex;
            gap: 0.75rem;
            align-items: center;
            flex-wrap: wrap;
        }
        .quick-filter {
            padding: 0.5rem 1rem;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            color: var(--text-primary);
            font-size: 0.875rem;
            min-width: 200px;
        }
        table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            background: var(--bg-secondary);
            border-radius: 10px;
            overflow: hidden;
        }
        th, td {
            padding: 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }
        th {
            background: var(--bg-tertiary);
            font-weight: 700;
            position: sticky;
            top: 0;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
            z-index: 10;
            cursor: pointer;
            user-select: none;
        }
        th:hover {
            background: var(--bg-primary);
        }
        th .sort-indicator {
            margin-left: 0.5rem;
            font-size: 0.7rem;
            opacity: 0.5;
        }
        th.sorted .sort-indicator {
            opacity: 1;
        }
        tr:hover td {
            background: rgba(99, 102, 241, 0.05);
        }
        td {
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        .schema-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 1.5rem;
            margin-top: 1.5rem;
        }
        .schema-card {
            background: var(--bg-tertiary);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            transition: all 0.3s ease;
        }
        .schema-card:hover {
            background: var(--bg-secondary);
            border-color: var(--primary);
        }
        .schema-header {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1rem;
        }
        .schema-title {
            font-size: 1.125rem;
            font-weight: 700;
            color: var(--primary);
            flex: 1;
        }
        .schema-count {
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-bottom: 1rem;
        }
        .schema-columns {
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 0.875rem;
            max-height: 300px;
            overflow-y: auto;
        }
        .schema-column {
            padding: 0.5rem 0;
            color: var(--text-secondary);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .schema-column-name {
            color: var(--success);
            font-weight: 600;
        }
        .spinner {
            border: 4px solid var(--border);
            border-top: 4px solid var(--primary);
            border-radius: 50%;
            width: 48px;
            height: 48px;
            animation: spin 1s linear infinite;
            margin: 2rem auto;
            display: none;
        }
        .spinner.active {
            display: block;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .error-message {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            padding: 1rem 1.5rem;
            border-radius: 10px;
            margin-top: 1rem;
            display: none;
            color: var(--error);
        }
        .error-message.active {
            display: flex;
            align-items: center;
            gap: 1rem;
            animation: shake 0.5s;
        }
        @keyframes shake {
            0%, 100% { transform: translateX(0); }
            25% { transform: translateX(-10px); }
            75% { transform: translateX(10px); }
        }
        .toast {
            position: fixed;
            bottom: 2rem;
            right: 2rem;
            background: var(--success);
            color: white;
            padding: 1rem 1.5rem;
            border-radius: 12px;
            display: none;
            z-index: 10000;
            box-shadow: var(--shadow-lg);
            animation: slideUp 0.3s ease;
        }
        .toast.show {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        .toast.error {
            background: var(--error);
        }
        @keyframes slideUp {
            from { transform: translateY(20px); opacity: 0; }
            to { transform: translateY(0); opacity: 1; }
        }
        .pagination {
            display: flex;
            gap: 0.5rem;
            align-items: center;
            margin-top: 1rem;
            flex-wrap: wrap;
        }
        .pagination-btn {
            padding: 0.5rem 1rem;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            color: var(--text-primary);
            border-radius: 8px;
            cursor: pointer;
            min-width: 40px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        .pagination-btn:hover:not(:disabled) {
            background: var(--bg-secondary);
            border-color: var(--primary);
        }
        .pagination-btn:disabled {
            opacity: 0.3;
            cursor: not-allowed;
        }
        .pagination-btn.active {
            background: var(--primary);
            border-color: var(--primary);
        }
        ::-webkit-scrollbar {
            width: 10px;
            height: 10px;
        }
        ::-webkit-scrollbar-track {
            background: var(--bg-tertiary);
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb {
            background: var(--border);
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: var(--text-muted);
        }
        @media (max-width: 768px) {
            .container {
                padding: 1rem;
            }
            .header {
                padding: 1rem;
            }
            h1 {
                font-size: 1.5rem;
            }
            .stats-grid {
                grid-template-columns: 1fr;
            }
            .btn-grid {
                grid-template-columns: 1fr;
            }
            .section {
                padding: 1.5rem;
            }
            .input-group {
                flex-direction: column;
            }
            .schema-grid {
                grid-template-columns: 1fr;
            }
        }
        .empty-state {
            text-align: center;
            padding: 3rem 2rem;
            color: var(--text-secondary);
        }
        .empty-state-icon {
            font-size: 4rem;
            margin-bottom: 1rem;
            opacity: 0.3;
        }
        .empty-state-text {
            font-size: 1.125rem;
            margin-bottom: 0.5rem;
        }
        .empty-state-subtext {
            font-size: 0.9375rem;
            opacity: 0.7;
        }
        .column-filter-btn {
            margin-left: 0.5rem;
            font-size: 0.7rem;
            opacity: 0.5;
            cursor: pointer;
            transition: opacity 0.2s;
        }
        .column-filter-btn:hover {
            opacity: 1;
        }
        .column-filter-btn.active {
            opacity: 1;
            color: var(--primary);
        }
        .filter-dropdown {
            position: absolute;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 0.75rem;
            z-index: 1000;
            box-shadow: var(--shadow-lg);
            max-height: 400px;
            overflow-y: auto;
            min-width: 250px;
            display: none;
        }
        .filter-dropdown.active {
            display: block;
        }
        .filter-search {
            width: 100%;
            padding: 0.5rem;
            background: var(--bg-tertiary);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text-primary);
            margin-bottom: 0.75rem;
            font-size: 0.875rem;
        }
        .filter-options {
            max-height: 250px;
            overflow-y: auto;
        }
        .filter-option {
            padding: 0.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            cursor: pointer;
            border-radius: 4px;
            transition: background 0.2s;
            font-size: 0.875rem;
        }
        .filter-option:hover {
            background: var(--bg-tertiary);
        }
        .filter-option input[type="checkbox"] {
            cursor: pointer;
        }
        .filter-actions {
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid var(--border);
            display: flex;
            gap: 0.5rem;
        }
        .filter-count {
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
            padding: 0.25rem 0.5rem;
            background: var(--bg-tertiary);
            border-radius: 4px;
        }
        /* New Styles for Professional Edition Features */
        .filter-section {
            margin-bottom: 20px;
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }
        
        .filter-section-header {
            display: flex;
            justify-content: space-between;
            padding: 15px 20px;
            background: var(--bg-tertiary);
            cursor: pointer;
            user-select: none;
        }
        
        .filter-section-header:hover {
            background: rgba(99, 102, 241, 0.1);
        }
        
        .collapse-icon {
            transition: transform 0.3s;
        }
        
        .filter-section.collapsed .collapse-icon {
            transform: rotate(-90deg);
        }
        
        .filter-section-content {
            padding: 20px;
            display: block;
        }
        
        .filter-section.collapsed .filter-section-content {
            display: none;
        }
        
        .range-error {
            border-color: var(--error) !important;
            animation: shake 0.3s;
        }
        
        @keyframes shake {
            0%, 100% { transform: translateX(0); }
            25% { transform: translateX(-5px); }
            75% { transform: translateX(5px); }
        }
        
        .range-error-message {
            font-size: 0.75rem;
            color: var(--error);
            margin-top: 4px;
            display: none;
        }
        
        .range-error-message.show {
            display: block;
        }
        
        .filter-active-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            background: var(--success);
            border-radius: 50%;
            margin-left: 8px;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        /* Collapsible sections */
        .collapsible-section {
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 15px;
            overflow: hidden;
        }
        
        .collapsible-header {
            display: flex;
            justify-content: space-between;
            padding: 12px 20px;
            background: var(--bg-tertiary);
            cursor: pointer;
            user-select: none;
            transition: background 0.2s;
        }
        
        .collapsible-header:hover {
            background: rgba(99, 102, 241, 0.1);
        }
        
        .collapsible-header.collapsed .arrow {
            transform: rotate(-90deg);
        }
        
        .arrow {
            transition: transform 0.3s;
        }
        
        .collapsible-content {
            padding: 15px 20px;
            display: block;
        }
        
        .collapsible-header.collapsed + .collapsible-content {
            display: none;
        }
        /* Modal styles (reusing existing modal pattern) */
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.8);
            z-index: 10000;
            padding: 2rem;
            overflow: auto;
        }
        .modal.active {
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .modal-content {
            max-width: 600px;
            width: 100%;
            background: var(--bg-secondary);
            border-radius: 16px;
            padding: 2rem;
            border: 1px solid var(--border);
        }
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
        }
        .modal-title {
            color: var(--text-primary);
            font-size: 1.5rem;
            font-weight: 700;
        }
        .modal-close {
            background: none;
            border: none;
            color: var(--text-primary);
            font-size: 2rem;
            cursor: pointer;
            padding: 0;
            width: 40px;
            height: 40px;
        }
        .modal-body {
            color: var(--text-secondary);
            line-height: 1.8;
        }
        .modal-body ul {
            margin-left: 1.5rem;
            margin-top: 1rem;
        }
        .modal-body li {
            margin-bottom: 0.75rem;
        }
        .modal-body kbd {
            background: var(--bg-tertiary);
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 0.875rem;
            border: 1px solid var(--border);
        }
        /* TODO: Add animation for modal entrance/exit for better UX */
    </style>
</head>
<body>
    <!-- Header -->
    <div class="header">
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">📊</div>
                <div>
                    <h1>NSE Data Downloader</h1>
                    <div class="subtitle">Professional Historical Data Management</div>
                </div>
            </div>
            <div class="header-actions">
                <!-- Keyboard shortcuts help button -->
                <button class="btn btn-small btn-secondary" onclick="showShortcutsModal()" title="Keyboard Shortcuts">
                    <span class="btn-icon">⌨️</span> Shortcuts
                </button>
                <button class="btn btn-small btn-secondary" onclick="loadStats()" id="btnRefreshStats">
                    <span class="btn-icon">🔄</span> Refresh
                </button>
            </div>
        </div>
    </div>

    <div class="container">
        <!-- Stats Dashboard -->
        <div class="stats-grid">
            <div class="stat-card" onclick="setStatsQuery('equity')" style="cursor: pointer;">
                <div class="stat-header">
                    <div class="stat-icon">📈</div>
                    <div class="stat-badge">Equity</div>
                </div>
                <div class="stat-label">Total Equity Records</div>
                <div class="stat-value" id="equityCount">0</div>
                <div class="stat-change" id="equityLastUpdate">Never updated</div>
            </div>
            <div class="stat-card" onclick="setStatsQuery('derivatives')" style="cursor: pointer;">
                <div class="stat-header">
                    <div class="stat-icon">📉</div>
                    <div class="stat-badge" style="background: rgba(245, 158, 11, 0.1); color: var(--warning); border-color: rgba(245, 158, 11, 0.2);">Derivatives</div>
                </div>
                <div class="stat-label">Total Derivatives Records</div>
                <div class="stat-value" id="derivativesCount">0</div>
                <div class="stat-change" id="derivativesLastUpdate">Never updated</div>
            </div>
            <div class="stat-card" onclick="setStatsQuery('symbols')" style="cursor: pointer;">
                <div class="stat-header">
                    <div class="stat-icon">🏢</div>
                    <div class="stat-badge" style="background: rgba(139, 92, 246, 0.1); color: #a78bfa; border-color: rgba(139, 92, 246, 0.2);">Symbols</div>
                </div>
                <div class="stat-label">Unique Symbols</div>
                <div class="stat-value" id="symbolsCount">0</div>
                <div class="stat-change" id="dateRange">No data available</div>
            </div>
        </div>

        <!-- Progress Container -->
        <div class="progress-container" id="progressContainer">
            <div class="progress-header">
                <div class="progress-pulse"></div>
                <div class="progress-message" id="progressMessage">Processing...</div>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%;"></div>
            </div>
            <div class="progress-errors" id="progressErrors">
                <h4>⚠️ Errors Encountered:</h4>
                <ul id="progressErrorsList"></ul>
            </div>
            <div class="progress-stats" id="progressStats"></div>
            <button class="btn btn-secondary" style="width: 100%;" onclick="confirmStopDownload()">
                <span class="btn-icon">⏸️</span> Stop Download
            </button>
        </div>

        <!-- Quick Actions Section -->
        <div class="section">
            <div class="section-header">
                <div class="section-icon">⚡</div>
                <h2 class="section-title">Quick Actions</h2>
            </div>
            <p class="section-description">
                Start downloading NSE data with one click. Incremental updates are fast (few minutes), full history takes 20-30 minutes.
            </p>
            <div class="btn-grid">
                <button class="btn btn-success" onclick="startDownload('incremental')" id="btnIncremental">
                    <span><span class="btn-icon">🔄</span> Incremental Update</span>
                    <span class="btn-hint">Updates only missing dates since last download</span>
                </button>
                <button class="btn" onclick="startDownload('full_history')" id="btnFullHistory">
                    <span><span class="btn-icon">📚</span> Full History Download</span>
                    <span class="btn-hint">Downloads complete data from 2000 to today (~30 min)</span>
                </button>
                <button class="btn btn-secondary" onclick="checkDataGaps()" id="btnCheckGaps">
                    <span><span class="btn-icon">🔍</span> Check for Missing Dates</span>
                    <span class="btn-hint">Detect gaps in downloaded data</span>
                </button>
            </div>
        </div>

        <!-- Custom Download Section -->
        <div class="section">
            <div class="section-header">
                <div class="section-icon">🎯</div>
                <h2 class="section-title">Custom Download</h2>
            </div>
            <p class="section-description">
                Download a specific number of recent trading days for quick analysis or testing.
            </p>
            <div class="input-group">
                <label>Last N Days:</label>
                <!-- Better validation attributes -->
                <input type="number" id="daysInput" placeholder="e.g., 30 for last month" value="30" min="1" max="365" required>
                <button class="btn btn-small" onclick="startCustomDownload()" id="btnCustom" style="min-width: 150px;">
                    <span class="btn-icon">▶️</span> Download
                </button>
            </div>
        </div>

        <!-- SQL Query Explorer -->
        <div class="section">
            <div class="section-header">
                <div class="section-icon">🔍</div>
                <h2 class="section-title">SQL Query Explorer</h2>
                <div class="stat-badge" style="background: rgba(59, 130, 246, 0.1); color: #60a5fa; border-color: rgba(59, 130, 246, 0.2);">SELECT Only</div>
            </div>
            <textarea id="sqlQuery" placeholder="SELECT * FROM equity_data WHERE symbol = 'RELIANCE' ORDER BY date DESC LIMIT 100"></textarea>
            <div style="margin-top: 0.5rem;">
                <select id="queryHistoryDropdown" onchange="loadQueryFromHistory()" 
                    style="padding: 0.5rem 1rem; background: var(--bg-tertiary); border: 1px solid var(--border); border-radius: 8px; color: var(--text-primary); font-size: 0.875rem; min-width: 300px;">
                    <option value="">-- Recent Queries --</option>
                </select>
            </div>

            <div class="query-actions">
                <button class="btn btn-success" onclick="executeQuery()" id="btnExecuteQuery" style="flex: 1;">
                    <span class="btn-icon">▶️</span> Execute Query
                </button>
                <button class="btn btn-secondary" onclick="clearResults()" style="min-width: 120px;">
                    <span class="btn-icon">🗑️</span> Clear
                </button>
            </div>

            <div class="query-examples">
                <button class="example-btn" onclick="setExampleQuery('top10')">📊 Top 10 by Volume</button>
                <button class="example-btn" onclick="setExampleQuery('recent')">🕐 Recent Entries</button>
                <button class="example-btn" onclick="setExampleQuery('symbols')">🏢 All Symbols</button>
                <button class="example-btn" onclick="setExampleQuery('stats')">📈 Daily Stats</button>
                <button class="example-btn" onclick="setExampleQuery('gainers')">🚀 Top Gainers</button>
            </div>

            <div class="spinner" id="querySpinner"></div>
            <div class="error-message" id="errorMessage">
                <span style="font-size: 20px;">⚠️</span>
                <span id="errorText"></span>
            </div>

            <div class="results-container" id="resultsContainer">
                <div class="results-header">
                    <div id="resultsCount">0 results</div>
                    <div class="results-actions">
                        <input type="text" class="quick-filter" id="quickFilter" placeholder="🔍 Global search..." onkeyup="quickFilterTable()">
                        <button class="btn btn-small btn-secondary" onclick="clearAllFilters()" id="btnClearFilters" title="Clear all column and text filters">
                            <span class="btn-icon">✖️</span> Clear Filters
                        </button>
                        <button class="btn btn-small btn-secondary" onclick="copyTableToClipboard()">
                            <span class="btn-icon">📋</span> Copy
                        </button>
                        <button class="btn btn-small btn-secondary" onclick="showExportOptions()">
                            <span class="btn-icon">💾</span> Export CSV
                        </button>
                    </div>
                </div>
                <div id="tableContainer"></div>
                <div class="pagination" id="pagination"></div>
            </div>
        </div>

        <!-- Database Schema Browser -->
        <div class="section">
            <div class="section-header">
                <div class="section-icon">📚</div>
                <h2 class="section-title">Database Schema Browser</h2>
                <button class="btn btn-small btn-secondary" onclick="loadSchema()" id="btnRefreshSchema" style="margin-left: auto;">
                    <span class="btn-icon">🔄</span> Refresh
                </button>
            </div>
            <div class="schema-grid" id="schemaContainer">
                <div class="spinner active" style="grid-column: 1 / -1;"></div>
            </div>
        </div>
    </div>

    <!-- Accessible modal for gaps display instead of alert() -->
    <div id="gapsModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title">Data Gap Analysis</h2>
                <button class="modal-close" onclick="closeGapsModal()">&times;</button>
            </div>
            <div class="modal-body" id="gapsModalBody">
                <!-- Content will be populated by JavaScript -->
            </div>
        </div>
    </div>

    <!-- Keyboard shortcuts modal -->
    <div id="shortcutsModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title">⌨️ Keyboard Shortcuts</h2>
                <button class="modal-close" onclick="closeShortcutsModal()">&times;</button>
            </div>
            <div class="modal-body">
                <p style="margin-bottom: 1rem;">Use these keyboard shortcuts to navigate faster:</p>
                <ul>
                    <li><kbd>Ctrl</kbd> + <kbd>K</kbd> - Focus SQL query box</li>
                    <li><kbd>Ctrl</kbd> + <kbd>Enter</kbd> - Execute query (when query box is focused)</li>
                    <li><kbd>Ctrl</kbd> + <kbd>E</kbd> - Clear query results</li>
                    <li><kbd>Ctrl</kbd> + <kbd>S</kbd> - Export current results to CSV</li>
                    <li><kbd>Alt</kbd> + <kbd>←</kbd> - Previous page in results</li>
                    <li><kbd>Alt</kbd> + <kbd>→</kbd> - Next page in results</li>
                    <li><kbd>Esc</kbd> - Close any open modal</li>
                </ul>
                <p style="margin-top: 1.5rem; font-size: 0.875rem; opacity: 0.7;">
                    💡 Tip: Click on any stat card to quickly query that data type!
                </p>
            </div>
        </div>
    </div>

    <!-- Row details modal (existing) -->
    <div id="rowModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 class="modal-title">Row Details</h2>
                <button class="modal-close" onclick="closeRowModal()">&times;</button>
            </div>
            <div class="modal-body" id="rowModalContent" style="font-family: 'SF Mono', Monaco, monospace; font-size: 0.9375rem;">
                <!-- Content populated by JS -->
            </div>
        </div>
    </div>

    <div class="toast" id="toast"></div>

    <script>
        // Global state
        let statusInterval = null;
        let currentPage = 1;
        let rowsPerPage = 100;
        let tableFilterText = '';
        let allTableRows = [];
        let visibleRows = [];
        let sortCol = null;
        let sortDir = 'asc';
        let currentData = null;
        let columnFilters = {};
        let uniqueColumnValues = {};
        const csrfToken = '{{ csrf_token }}';  

        // Query History Management
        let queryHistory = JSON.parse(localStorage.getItem('nseQueryHistory') || '[]');

        // XSS protection function
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        // Initialize on page load
        window.addEventListener('load', () => {
            loadStats();
            loadSchema();
            updateQueryHistoryDropdown();
        });

        function setStatsQuery(type) {
            const queries = {
                'equity': 'SELECT * FROM equity_data ORDER BY date DESC, volume DESC LIMIT 100',
                'derivatives': 'SELECT * FROM derivatives_data ORDER BY date DESC, open_interest DESC LIMIT 100',
                'symbols': 'SELECT symbol, COUNT(*) as trading_days, AVG(close) as avg_price, MAX(volume) as max_volume FROM equity_data GROUP BY symbol ORDER BY trading_days DESC LIMIT 50'
            };
            
            document.getElementById('sqlQuery').value = queries[type];
            document.getElementById('sqlQuery').scrollIntoView({ behavior: 'smooth', block: 'center' });
            showToast(`Query set for ${type} data - click Execute to run`);
        }
        
        function saveQueryToHistory(sql) {
            if (!sql.trim()) return;
            
            // Remove if already exists
            queryHistory = queryHistory.filter(q => q !== sql);
            
            // Add to front
            queryHistory.unshift(sql);
            
            // Keep only last 10
            queryHistory = queryHistory.slice(0, 10);
            
            localStorage.setItem('nseQueryHistory', JSON.stringify(queryHistory));
            updateQueryHistoryDropdown();
        }
        
        // XSS protection in dropdown
        function updateQueryHistoryDropdown() {
            const dropdown = document.getElementById('queryHistoryDropdown');
            if (!dropdown) return;
            
            if (queryHistory.length === 0) {
                dropdown.innerHTML = '<option value="">No history</option>';
                dropdown.disabled = true;
                return;
            }
            
            dropdown.disabled = false;
            dropdown.innerHTML = '<option value="">-- Recent Queries --</option>' +
                queryHistory.map((q, i) => {
                    const preview = q.substring(0, 60) + (q.length > 60 ? '...' : '');
                    return `<option value="${i}">${escapeHtml(preview)}</option>`;
                }).join('');
        }
        
        function loadQueryFromHistory() {
            const dropdown = document.getElementById('queryHistoryDropdown');
            const index = parseInt(dropdown.value);
            
            if (isNaN(index)) return;
            
            document.getElementById('sqlQuery').value = queryHistory[index];
            showToast('Query loaded from history');
        }

        // Stats loading with retry logic
        async function loadStats() {
            const btn = document.getElementById('btnRefreshStats');
            btn.classList.add('btn-loading');
            btn.disabled = true;

            let retries = 3;
            while (retries > 0) {
                try {
                    const response = await fetch('/api/stats');
                    const data = await response.json();

                    if (data.error) {
                        throw new Error(data.error);
                    }

                    document.getElementById('equityCount').textContent = data.equity_count.toLocaleString();
                    document.getElementById('derivativesCount').textContent = data.derivatives_count.toLocaleString();
                    document.getElementById('symbolsCount').textContent = data.symbols_count.toLocaleString();

                    if (data.equity_last_update) {
                        document.getElementById('equityLastUpdate').textContent = `Last updated: ${data.equity_last_update}`;
                    } else {
                        document.getElementById('equityLastUpdate').textContent = 'Never updated';
                    }

                    if (data.derivatives_last_update) {
                        document.getElementById('derivativesLastUpdate').textContent = `Last updated: ${data.derivatives_last_update}`;
                    } else {
                        document.getElementById('derivativesLastUpdate').textContent = 'Never updated';
                    }

                    if (data.date_range) {
                        document.getElementById('dateRange').textContent = `Range: ${data.date_range}`;
                    } else {
                        document.getElementById('dateRange').textContent = 'No data available';
                    }
                    
                    break; // Success, exit retry loop
                } catch (error) {
                    retries--;
                    if (retries === 0) {
                        console.error('Failed to load stats:', error);
                        showToast('Failed to load statistics. Retrying...', 'error');
                    } else {
                        await new Promise(resolve => setTimeout(resolve, 500));
                    }
                }
            }
            
            btn.classList.remove('btn-loading');
            btn.disabled = false;
        }

        // Confirmation dialog for long downloads
        async function startDownload(type) {
            // Add confirmation for full history
            if (type === 'full_history') {
                if (!confirm('⚠️ Full History Download\\n\\nThis will download 25 years of NSE data (2000-2025).\\n\\n• Expected time: 20-30 minutes\\n• Data size: Several GB\\n• Internet connection required throughout\\n\\nContinue?')) {
                    return;
                }
            }
            
            try {
                const response = await fetch('/api/start_download', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: type, csrf_token: csrfToken }) 
                });

                if (!response.ok) {
                    const error = await response.json();
                    showToast(error.error || 'Failed to start download', 'error');
                    return;
                }

                disableButtons(true);
                document.getElementById('progressContainer').classList.add('active');
                document.getElementById('progressFill').style.width = '0%';
                document.getElementById('progressErrors').classList.remove('active');
                startStatusPolling();
                showToast('Download started successfully');

            } catch (error) {
                showToast('Error: ' + error.message, 'error');
                disableButtons(false);
            }
        }

        // Client-side validation with server backup
        async function startCustomDownload() {
            const daysInput = document.getElementById('daysInput');
            const days = parseInt(daysInput.value);

            // Enhanced validation
            if (!days || isNaN(days)) {
                showToast('Please enter a valid number', 'error');
                daysInput.focus();
                return;
            }
            
            if (days < 1) {
                showToast('Days must be at least 1', 'error');
                daysInput.focus();
                return;
            }
            
            if (days > 365) {
                showToast('Maximum 365 days allowed. Use Full History for older data.', 'error');
                daysInput.focus();
                return;
            }

            try {
                const response = await fetch('/api/start_download', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'last_n_days', days: days, csrf_token: csrfToken }) 
                });

                if (!response.ok) {
                    const error = await response.json();
                    showToast(error.error || 'Failed to start download', 'error');
                    return;
                }

                disableButtons(true);
                document.getElementById('progressContainer').classList.add('active');
                document.getElementById('progressFill').style.width = '0%';
                document.getElementById('progressErrors').classList.remove('active');
                startStatusPolling();
                showToast(`Downloading last ${days} days`);

            } catch (error) {
                showToast('Error: ' + error.message, 'error');
                disableButtons(false);
            }
        }

        function confirmStopDownload() {
            if (confirm('Are you sure you want to stop the download?\\n\\nProgress will be saved, but the download will be incomplete.')) {
                stopDownload();
            }
        }

        async function stopDownload() {
            try {
                await fetch('/api/stop_download', { 
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ csrf_token: csrfToken }) 
                });
                stopStatusPolling();
                disableButtons(false);
                setTimeout(() => {
                    document.getElementById('progressContainer').classList.remove('active');
                }, 500);
                showToast('Download stopped');
            } catch (error) {
                console.error('Failed to stop:', error);
                showToast('Failed to stop download', 'error');
            }
        }

        function startStatusPolling() {
            if (statusInterval) clearInterval(statusInterval);

            statusInterval = setInterval(async () => {
                try {
                    const response = await fetch('/api/status');
                    const status = await response.json();

                    updateProgress(status);

                    if (!status.running) {
                        stopStatusPolling();
                        disableButtons(false);

                        await loadStats();
                        await loadSchema();

                        setTimeout(() => {
                            document.getElementById('progressContainer').classList.remove('active');
                        }, 3000);

                        if (status.errors && status.errors.length > 0) {
                            showToast(`Download completed with ${status.errors.length} errors`, 'error');
                        } else {
                            showToast('Download completed successfully!');
                        }
                    }
                } catch (error) {
                    console.error('Status poll error:', error);
                }
            }, 1000);
        }

        function stopStatusPolling() {
            if (statusInterval) {
                clearInterval(statusInterval);
                statusInterval = null;
            }
        }

        // Improved progress update with actual download tracking
        function updateProgress(status) {
            // Defensive checks
            if (!status) {
                console.error('Invalid status object received');
                return;
            }
            
            const message = status.message || 'Processing...';
            const progress = Math.min(100, Math.max(0, status.progress || 0));
            const current = status.current || 0;
            const total = status.total || 0;
            const elapsed = status.elapsed || 0;
            const actualDownloaded = status.actual_downloaded || 0;
            const errors = status.errors || [];
            
            document.getElementById('progressMessage').textContent = message;
            document.getElementById('progressFill').style.width = progress + '%';
    
            const formatTime = (seconds) => {
                const h = Math.floor(seconds / 3600);
                const m = Math.floor((seconds % 3600) / 60);
                const s = seconds % 60;
                return h > 0 ? `${h}h ${m}m ${s}s` : m > 0 ? `${m}m ${s}s` : `${s}s`;
            };

            // Handle edge cases in ETA calculation
            const estimateETA = (current, total, elapsed) => {
                if (current === 0 || total <= 0 || elapsed === 0) return '—';
                const rate = current / elapsed;
                const remaining = (total - current) / rate;
                return formatTime(Math.round(remaining));
            };

            const progressLabel = status.total > 0 ? `${status.progress}%` : 'In Progress...';
            const itemsLabel = status.total > 0 ? `${status.current} / ${status.total}` : `${status.current} processed`;
            const etaLabel = status.total > 0 ? estimateETA(status.current, status.total, status.elapsed) : '—';

            // Show actual downloads vs skipped
            const statsHtml = `
                <div class="progress-stat">
                    <div class="progress-stat-label">Progress</div>
                    <div class="progress-stat-value">${progressLabel}</div>
                </div>
                <div class="progress-stat">
                    <div class="progress-stat-label">Processed</div>
                    <div class="progress-stat-value">${itemsLabel}</div>
                </div>
                <div class="progress-stat">
                    <div class="progress-stat-label">Downloaded</div>
                    <div class="progress-stat-value" style="color: var(--success)">${status.actual_downloaded || 0}</div>
                </div>
                <div class="progress-stat">
                    <div class="progress-stat-label">Elapsed</div>
                    <div class="progress-stat-value">${formatTime(status.elapsed)}</div>
                </div>
                <div class="progress-stat">
                    <div class="progress-stat-label">ETA</div>
                    <div class="progress-stat-value">${etaLabel}</div>
                </div>
                <div class="progress-stat">
                    <div class="progress-stat-label">Errors</div>
                    <div class="progress-stat-value" style="color: ${status.errors.length > 0 ? 'var(--error)' : 'var(--success)'}">
                        ${status.errors.length}
                    </div>
                </div>
            `;

            document.getElementById('progressStats').innerHTML = statsHtml;

            // Display errors
            const errorsContainer = document.getElementById('progressErrors');
            const errorsList = document.getElementById('progressErrorsList');

            if (status.errors && status.errors.length > 0) {
                errorsContainer.classList.add('active');
                errorsList.innerHTML = status.errors.slice(-5).map(err => `<li>• ${escapeHtml(err)}</li>`).join('');
            } else {
                errorsContainer.classList.remove('active');
            }
        }

        function disableButtons(disable) {
            document.getElementById('btnIncremental').disabled = disable;
            document.getElementById('btnFullHistory').disabled = disable;
            document.getElementById('btnCustom').disabled = disable;
        }

        // Use accessible modal instead of alert()
        async function checkDataGaps() {
            const btn = document.getElementById('btnCheckGaps');
            btn.classList.add('btn-loading');
            btn.disabled = true;
            
            try {
                const response = await fetch('/api/check_gaps');
                const data = await response.json();
                
                if (data.error) {
                    showToast(data.error, 'error');
                    return;
                }
                
                // Build modal content
                let content = `
                    <div style="margin-bottom: 1.5rem;">
                        <h3 style="margin-bottom: 0.5rem; color: var(--text-primary);">📊 Gap Analysis Results</h3>
                        <p>Missing trading days in your downloaded data:</p>
                    </div>
                    
                    <div style="background: var(--bg-tertiary); padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                            <strong>Equity Data:</strong>
                            <span style="color: ${data.equity_total > 0 ? 'var(--error)' : 'var(--success)'}">${data.equity_total} missing dates</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <strong>Derivatives Data:</strong>
                            <span style="color: ${data.derivatives_total > 0 ? 'var(--error)' : 'var(--success)'}">${data.derivatives_total} missing dates</span>
                        </div>
                    </div>
                `;
                
                if (data.equity_total > 0) {
                    content += `
                        <div style="margin-top: 1rem;">
                            <h4 style="margin-bottom: 0.5rem; color: var(--text-primary);">First few missing equity dates:</h4>
                            <div style="background: var(--bg-tertiary); padding: 0.75rem; border-radius: 6px; font-family: monospace; font-size: 0.875rem;">
                                ${data.equity_gaps.slice(0, 10).map(d => `<div>${d}</div>`).join('')}
                                ${data.equity_total > 10 ? `<div style="margin-top: 0.5rem; opacity: 0.7;">...and ${data.equity_total - 10} more</div>` : ''}
                            </div>
                        </div>
                    `;
                }
                
                if (data.equity_total === 0 && data.derivatives_total === 0) {
                    content += `
                        <div style="text-align: center; padding: 2rem; color: var(--success);">
                            <div style="font-size: 3rem; margin-bottom: 1rem;">✓</div>
                            <div style="font-size: 1.125rem; font-weight: 600;">No gaps found!</div>
                            <div style="opacity: 0.7; margin-top: 0.5rem;">Your data is complete.</div>
                        </div>
                    `;
                } else {
                    content += `
                        <div style="margin-top: 1.5rem; padding: 1rem; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.3); border-radius: 8px;">
                            <strong>💡 Tip:</strong> Run an incremental update to fill these gaps automatically.
                        </div>
                    `;
                }
                
                document.getElementById('gapsModalBody').innerHTML = content;
                document.getElementById('gapsModal').classList.add('active');
                
                if (data.equity_total === 0 && data.derivatives_total === 0) {
                    showToast('No gaps found! Data is complete.');
                } else {
                    showToast(`Found ${data.equity_total + data.derivatives_total} missing dates`, 'error');
                }
                
            } catch (error) {
                showToast('Failed to check gaps: ' + error.message, 'error');
            } finally {
                btn.classList.remove('btn-loading');
                btn.disabled = false;
            }
        }
        
        function closeGapsModal() {
            document.getElementById('gapsModal').classList.remove('active');
        }
        
        // Keyboard shortcuts modal
        function showShortcutsModal() {
            document.getElementById('shortcutsModal').classList.add('active');
        }
        
        function closeShortcutsModal() {
            document.getElementById('shortcutsModal').classList.remove('active');
        }

        async function executeQuery() {
            const sql = document.getElementById('sqlQuery').value.trim();

            if (!sql) {
                showToast('Please enter a SQL query', 'error');
                return;
            }

            // Basic schema validation
            const hasEquityTable = sql.toLowerCase().includes('equity_data');
            const hasDerivativesTable = sql.toLowerCase().includes('derivatives_data');
            
            if ((hasEquityTable || hasDerivativesTable) && currentData === null) {
                // First query - check if tables exist
                try {
                    const tablesResponse = await fetch('/api/tables');
                    const tablesData = await tablesResponse.json();
                    
                    if (tablesData.tables && tablesData.tables.length === 0) {
                        showToast('No data tables found. Please download data first.', 'error');
                        return;
                    }
                } catch (e) {
                    // Continue anyway
                }
            }

            const spinner = document.getElementById('querySpinner');
            const errorMsg = document.getElementById('errorMessage');
            const errorText = document.getElementById('errorText');
            const resultsContainer = document.getElementById('resultsContainer');
            const executeBtn = document.getElementById('btnExecuteQuery');

            spinner.classList.add('active');
            errorMsg.classList.remove('active');
            resultsContainer.classList.remove('active');
            executeBtn.classList.add('btn-loading');
            executeBtn.disabled = true;

            try {
                const response = await fetch('/api/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sql: sql })
                });

                const data = await response.json();

                if (!response.ok) {
                    errorText.textContent = data.error || 'Query failed';
                    errorMsg.classList.add('active');
                    return;
                }

                currentData = data;
                renderTable(data);
                resultsContainer.classList.add('active');
                saveQueryToHistory(sql);
                showToast(`Query returned ${data.rows.toLocaleString()} rows`);

            } catch (error) {
                errorText.textContent = 'Error: ' + error.message;
                errorMsg.classList.add('active');
            } finally {
                spinner.classList.remove('active');
                executeBtn.classList.remove('btn-loading');
                executeBtn.disabled = false;
            }
        }
        
        // Collapsible filter sections
        function toggleSection(sectionId) {
            const section = document.getElementById(sectionId);
            if (section) {
                section.classList.toggle('collapsed');
            }
        }
        
        // Update clear button visibility
        function updateInputClearButton(inputId) {
            const input = document.getElementById(inputId);
            const btn = input?.nextElementSibling;
            if (btn && btn.classList.contains('input-clear-btn')) {
                btn.classList.toggle('show', input.value.length > 0);
            }
        }
        
        // Range validation
        function validateRange(minId, maxId) {
            const minInput = document.getElementById(minId);
            const maxInput = document.getElementById(maxId);
            const errorMsg = document.getElementById('error-' + minId);
            
            if (!minInput || !maxInput) return;
            
            const minVal = parseFloat(minInput.value);
            const maxVal = parseFloat(maxInput.value);
            
            const hasError = !isNaN(minVal) && !isNaN(maxVal) && minVal > maxVal;
            
            minInput.classList.toggle('range-error', hasError);
            maxInput.classList.toggle('range-error', hasError);
            if (errorMsg) errorMsg.classList.toggle('show', hasError);
            updateInputClearButton(minId);
            updateInputClearButton(maxId);
        }

        // XSS protection in table rendering
        function renderTable(data) {
            const tableContainer = document.getElementById('tableContainer');

            if (!data || !data.data || data.data.length === 0) {
                tableContainer.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">📋</div>
                        <div class="empty-state-text">No Results Found</div>
                        <div class="empty-state-subtext">Try adjusting your query</div>
                    </div>
                `;
                return;
            }
            
            // Build column filters cache
            uniqueColumnValues = {};
            data.columns.forEach((col, i) => {
                const values = new Set();
                data.data.forEach(row => {
                    const val = row[i];
                    if (val !== null && val !== undefined) {
                        values.add(String(val));
                    } else {
                        values.add('NULL');
                    }
                });
                uniqueColumnValues[i] = Array.from(values).sort();
            });
        
            columnFilters = {};

            let html = '<div class="pagination" id="paginationTop"></div>';
            html += '<div style="overflow-x: auto; position: relative;"><table><thead><tr>';
            
            // Escape column names
            data.columns.forEach((col, i) => {
                html += `<th onclick="sortTable(${i})" id="th-${i}" style="position: relative;">
                    ${escapeHtml(col)}
                    <span class="sort-indicator">▼</span>
                    <span class="column-filter-btn" id="filter-btn-${i}" onclick="event.stopPropagation(); toggleColumnFilter(${i})">
                        🔽
                    </span>
                </th>`;
            });
            html += '</tr></thead><tbody id="tableBody">';

            data.data.forEach(row => {
                html += '<tr>';
                row.forEach(cell => {
                    const displayValue = cell !== null && cell !== undefined ? escapeHtml(String(cell)) : '<span style="opacity: 0.5;">NULL</span>';
                    html += `<td>${displayValue}</td>`;
                });
                html += '</tr>';
            });

            html += '</tbody></table>';
            html += '<div id="filterDropdown" class="filter-dropdown"></div>';
            html += '</div>';
        
            if (data.rows > 1000) {
                html += `<div style="margin-top: 1rem; padding: 1rem; background: rgba(245, 158, 11, 0.1); border: 1px solid rgba(245, 158, 11, 0.3); border-radius: 8px; color: var(--text-secondary);">
                    ⚠️ Showing first 1,000 of ${data.rows.toLocaleString()} rows. Use LIMIT in your query for better performance.
                </div>`;
            }

            tableContainer.innerHTML = html;

            const tbody = document.getElementById('tableBody');
            if (tbody) {
                allTableRows = Array.from(tbody.querySelectorAll('tr'));
                visibleRows = [...allTableRows];
                currentPage = 1;
                tableFilterText = '';
                sortCol = null;
                sortDir = 'asc';
                document.getElementById('quickFilter').value = '';
                renderCurrentPage();
            }

            document.getElementById('resultsCount').textContent = `${data.rows.toLocaleString()} results`;
            document.addEventListener('click', closeAllFilterDropdowns);
        }

        function setExampleQuery(type) {
            const queries = {
                'top10': 'SELECT symbol, date, volume, close, turnover FROM equity_data ORDER BY volume DESC LIMIT 10',
                'recent': 'SELECT * FROM equity_data ORDER BY date DESC LIMIT 20',
                'symbols': 'SELECT DISTINCT symbol FROM equity_data ORDER BY symbol LIMIT 50',
                'stats': 'SELECT date, COUNT(*) as stocks, AVG(close) as avg_price, SUM(volume) as total_volume FROM equity_data GROUP BY date ORDER BY date DESC LIMIT 10',
                'gainers': 'SELECT symbol, date, close, prev_close, ROUND(((close - prev_close) / prev_close * 100), 2) as change_pct FROM equity_data WHERE prev_close > 0 ORDER BY change_pct DESC LIMIT 20'
            };

            document.getElementById('sqlQuery').value = queries[type] || '';
        }

        // Clear button resets ALL filters
        function clearResults() {
            document.getElementById('sqlQuery').value = '';
            document.getElementById('resultsContainer').classList.remove('active');
            document.getElementById('errorMessage').classList.remove('active');
            currentData = null;
            allTableRows = [];
            visibleRows = [];
            columnFilters = {};
            uniqueColumnValues = {};
            tableFilterText = '';
            document.getElementById('quickFilter').value = '';
        }
        
        // Improved filter dropdown positioning
        function toggleColumnFilter(colIndex) {
            const dropdown = document.getElementById('filterDropdown');
            const btn = document.getElementById(`filter-btn-${colIndex}`);
            
            if (dropdown.dataset.column == colIndex && dropdown.classList.contains('active')) {
                dropdown.classList.remove('active');
                return;
            }
            
            const btnRect = btn.getBoundingClientRect();
            const containerRect = document.getElementById('tableContainer').getBoundingClientRect();
            
            // Boundary detection
            const dropdownWidth = 250;
            let left = btnRect.left - containerRect.left;
            
            // Adjust if dropdown would go off-screen
            if (left + dropdownWidth > window.innerWidth - 40) {
                left = window.innerWidth - dropdownWidth - 40 - containerRect.left;
            }
            if (left < 0) left = 10;
            
            dropdown.style.left = left + 'px';
            dropdown.style.top = (btnRect.bottom - containerRect.top + 5) + 'px';
            dropdown.dataset.column = colIndex;
            
            const table = document.querySelector('#resultsContainer table');
            const headers = Array.from(table.querySelectorAll('th'));
            const colName = headers[colIndex].textContent.trim().replace(/[▼▲🔽]/g, '').trim();
            
            const values = uniqueColumnValues[colIndex] || [];
            const activeFilters = columnFilters[colIndex] || new Set(values);
            
            let html = `
                <div style="margin-bottom: 0.75rem; font-weight: 600; color: var(--text-primary);">
                    Filter: ${escapeHtml(colName)}
                </div>
                <input type="text" class="filter-search" id="filterSearch" 
                    placeholder="Search values..." onkeyup="filterColumnValues(${colIndex})">
                <div class="filter-count" id="filterCount">
                    ${activeFilters.size} of ${values.length} selected
                </div>
                <div style="margin-bottom: 0.5rem;">
                    <label style="display: flex; align-items: center; gap: 0.5rem; cursor: pointer; font-size: 0.875rem; color: var(--text-secondary);">
                        <input type="checkbox" id="selectAllFilter" 
                            ${activeFilters.size === values.length ? 'checked' : ''} 
                            onchange="toggleAllColumnFilters(${colIndex})">
                        <span>Select All</span>
                    </label>
                </div>
                <div class="filter-options" id="filterOptions">
            `;
            
            values.forEach(value => {
                const isChecked = activeFilters.has(value);
                const displayValue = value === 'NULL' ? '<em style="opacity: 0.5;">NULL</em>' : escapeHtml(value);
                const escapedValue = value.replace(/'/g, "\\'").replace(/"/g, '\\"');
                html += `
                    <label class="filter-option" data-value="${escapeHtml(value)}">
                        <input type="checkbox" value="${escapeHtml(value)}" 
                            ${isChecked ? 'checked' : ''} 
                            onchange="updateColumnFilter(${colIndex}, '${escapedValue}', this.checked)">
                        <span>${displayValue}</span>
                    </label>
                `;
            });
            
            html += `
                </div>
                <div class="filter-actions">
                    <button class="btn btn-small btn-success" onclick="applyColumnFilter(${colIndex})" style="flex: 1;">
                        Apply
                    </button>
                    <button class="btn btn-small btn-secondary" onclick="clearColumnFilter(${colIndex})">
                        Clear
                    </button>
                </div>
            `;
            
            dropdown.innerHTML = html;
            dropdown.classList.add('active');
        }
        
        function filterColumnValues(colIndex) {
            const searchTerm = document.getElementById('filterSearch').value.toLowerCase();
            const options = document.querySelectorAll('.filter-option');
            
            let visibleCount = 0;
            options.forEach(option => {
                const text = option.textContent.toLowerCase();
                if (text.includes(searchTerm)) {
                    option.style.display = 'flex';
                    visibleCount++;
                } else {
                    option.style.display = 'none';
                }
            });
        }
        
        function updateColumnFilter(colIndex, value, checked) {
            if (!columnFilters[colIndex]) {
                columnFilters[colIndex] = new Set(uniqueColumnValues[colIndex]);
            }
            
            if (checked) {
                columnFilters[colIndex].add(value);
            } else {
                columnFilters[colIndex].delete(value);
            }
            
            const total = uniqueColumnValues[colIndex].length;
            const selected = columnFilters[colIndex].size;
            document.getElementById('filterCount').textContent = `${selected} of ${total} selected`;
            
            document.getElementById('selectAllFilter').checked = (selected === total);
        }
        
        function toggleAllColumnFilters(colIndex) {
            const selectAll = document.getElementById('selectAllFilter').checked;
            const values = uniqueColumnValues[colIndex];
            
            if (selectAll) {
                columnFilters[colIndex] = new Set(values);
            } else {
                columnFilters[colIndex] = new Set();
            }
            
            document.querySelectorAll('.filter-option input[type="checkbox"]').forEach(cb => {
                cb.checked = selectAll;
            });
            
            document.getElementById('filterCount').textContent = 
                `${columnFilters[colIndex].size} of ${values.length} selected`;
        }
        
        function applyColumnFilter(colIndex) {
            const btn = document.getElementById(`filter-btn-${colIndex}`);
            const total = uniqueColumnValues[colIndex].length;
            const selected = columnFilters[colIndex] ? columnFilters[colIndex].size : total;
            
            if (selected === total) {
                btn.classList.remove('active');
            } else {
                btn.classList.add('active');
            }
            
            document.getElementById('filterDropdown').classList.remove('active');
            applyAllFilters();
            
            const filterCount = Object.keys(columnFilters).filter(k => 
                columnFilters[k].size < uniqueColumnValues[k].length
            ).length;
            
            showToast(`${filterCount} column filter${filterCount !== 1 ? 's' : ''} active`);
        }
        
        function clearColumnFilter(colIndex) {
            columnFilters[colIndex] = new Set(uniqueColumnValues[colIndex]);
            
            const btn = document.getElementById(`filter-btn-${colIndex}`);
            btn.classList.remove('active');
            
            document.getElementById('filterDropdown').classList.remove('active');
            applyAllFilters();
            showToast('Column filter cleared');
        }
        
        function applyAllFilters() {
            if (allTableRows.length === 0) return;
            
            currentPage = 1;
            
            visibleRows = allTableRows.filter(row => {
                const cells = Array.from(row.querySelectorAll('td'));
                
                for (let colIndex in columnFilters) {
                    const allowedValues = columnFilters[colIndex];
                    const cellValue = cells[colIndex]?.textContent.trim() || '';
                    const normalizedValue = cellValue === 'NULL' || cellValue.includes('NULL') ? 'NULL' : cellValue;
                    
                    if (!allowedValues.has(normalizedValue)) {
                        return false;
                    }
                }
                
                return true;
            });
            
            if (tableFilterText) {
                visibleRows = visibleRows.filter(row => 
                    row.textContent.toLowerCase().includes(tableFilterText)
                );
            }
            
            renderCurrentPage();
            
            const countEl = document.getElementById('resultsCount');
            const activeColumnFilters = Object.keys(columnFilters).filter(k => 
                columnFilters[k].size < uniqueColumnValues[k].length
            ).length;
            
            let filterDesc = '';
            if (activeColumnFilters > 0) {
                filterDesc += `${activeColumnFilters} column filter${activeColumnFilters !== 1 ? 's' : ''}`;
            }
            if (tableFilterText) {
                filterDesc += (filterDesc ? ' + ' : '') + 'text search';
            }
            
            countEl.textContent = `${visibleRows.length.toLocaleString()} results` + 
                (filterDesc ? ` (${filterDesc})` : '');
            
            if (visibleRows.length === 0) {
                countEl.style.color = 'var(--warning)';
            } else {
                countEl.style.color = '';
            }
        }
        
        function closeAllFilterDropdowns(e) {
            const dropdown = document.getElementById('filterDropdown');
            if (dropdown && !dropdown.contains(e?.target) && !e?.target?.classList.contains('column-filter-btn')) {
                dropdown.classList.remove('active');
            }
        }

        function clearAllFilters() {
            columnFilters = {};
            tableFilterText = '';
            document.getElementById('quickFilter').value = '';
            
            document.querySelectorAll('.column-filter-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            
            visibleRows = [...allTableRows];
            currentPage = 1;
            renderCurrentPage();
            
            document.getElementById('resultsCount').textContent = `${allTableRows.length.toLocaleString()} results`;
            document.getElementById('resultsCount').style.color = '';
            
            showToast('All filters cleared');
        }

        function quickFilterTable() {
            tableFilterText = document.getElementById('quickFilter')?.value.toLowerCase() || '';
            applyAllFilters();
        }

        function renderCurrentPage() {
            if (allTableRows.length === 0) return;

            allTableRows.forEach(row => row.style.display = 'none');

            const start = (currentPage - 1) * rowsPerPage;
            const end = start + rowsPerPage;
            visibleRows.slice(start, end).forEach(row => {
                row.style.display = '';
                row.style.cursor = 'pointer';
                row.onclick = () => showRowDetails(row);
            });
            
            updatePagination();
        }

        function updatePagination() {
            const totalPages = Math.ceil(visibleRows.length / rowsPerPage);
            const pag = document.getElementById('pagination');
            const pagTop = document.getElementById('paginationTop');
            
            if (!pag || !pagTop || totalPages <= 1) {
                if (pag) pag.innerHTML = '';
                if (pagTop) pagTop.innerHTML = '';
                return;
            }
            
            let html = `
                <button class="pagination-btn" onclick="changePage(${currentPage-1})" ${currentPage===1?'disabled':''}>
                    ← Prev
                </button>
            `;

            const maxButtons = 5;
            let startPage = Math.max(1, currentPage - Math.floor(maxButtons / 2));
            let endPage = Math.min(totalPages, startPage + maxButtons - 1);

            if (endPage - startPage < maxButtons - 1) {
                startPage = Math.max(1, endPage - maxButtons + 1);
            }

            if (startPage > 1) {
                html += `<button class="pagination-btn" onclick="changePage(1)">1</button>`;
                if (startPage > 2) {
                    html += `<span style="padding: 0 0.5rem; color: var(--text-muted);">...</span>`;
                }
            }

            for(let i = startPage; i <= endPage; i++) {
                html += `<button class="pagination-btn ${i===currentPage?'active':''}" onclick="changePage(${i})">${i}</button>`;
            }

            if (endPage < totalPages) {
                if (endPage < totalPages - 1) {
                    html += `<span style="padding: 0 0.5rem; color: var(--text-muted);">...</span>`;
                }
                html += `<button class="pagination-btn" onclick="changePage(${totalPages})">${totalPages}</button>`;
            }

            html += `
                <button class="pagination-btn" onclick="changePage(${currentPage+1})" ${currentPage===totalPages?'disabled':''}>
                    Next →
                </button>
            `;

            html += `
                <select onchange="changeRowsPerPage(parseInt(this.value))" 
                    style="padding:0.5rem;background:var(--bg-tertiary);border:1px solid var(--border);color:var(--text-primary);border-radius:8px;margin-left:0.75rem;">
                    <option value="50" ${rowsPerPage===50?'selected':''}>50/page</option>
                    <option value="100" ${rowsPerPage===100?'selected':''}>100/page</option>
                    <option value="250" ${rowsPerPage===250?'selected':''}>250/page</option>
                    <option value="500" ${rowsPerPage===500?'selected':''}>500/page</option>
                </select>
            `;

            pag.innerHTML = html;
            pagTop.innerHTML = html;
        }

        function changePage(p) {
            const totalPages = Math.ceil(visibleRows.length / rowsPerPage);
            if(p < 1 || p > totalPages) return;
            currentPage = p;
            renderCurrentPage();
            document.getElementById('resultsContainer')?.scrollIntoView({behavior:'smooth', block: 'nearest'});
        }

        function changeRowsPerPage(val) {
            rowsPerPage = val;
            currentPage = 1;
            renderCurrentPage();
        }

        function sortTable(colIndex) {
            if (allTableRows.length === 0) return;

            if (sortCol === colIndex) {
                sortDir = sortDir === 'asc' ? 'desc' : 'asc';
            } else {
                sortCol = colIndex;
                sortDir = 'asc';
            }

            allTableRows.sort((a, b) => {
                let aVal = a.children[colIndex]?.textContent.trim() || '';
                let bVal = b.children[colIndex]?.textContent.trim() || '';
                
                const aIsNull = aVal === 'NULL' || aVal.includes('NULL') || aVal === '';
                const bIsNull = bVal === 'NULL' || bVal.includes('NULL') || bVal === '';
        
                if (aIsNull && bIsNull) return 0;
                if (aIsNull) return 1;
                if (bIsNull) return -1;

                const aNum = parseFloat(aVal.replace(/[^0-9.-]/g, ''));
                const bNum = parseFloat(bVal.replace(/[^0-9.-]/g, ''));

                if (!isNaN(aNum) && !isNaN(bNum)) {
                    return sortDir === 'asc' ? (aNum - bNum) : (bNum - aNum);
                }

                return sortDir === 'asc' ? 
                    aVal.localeCompare(bVal) : 
                    bVal.localeCompare(aVal);
            });

            const tbody = document.getElementById('tableBody');
            if (tbody) {
                allTableRows.forEach(row => tbody.appendChild(row));
            }

            document.querySelectorAll('th').forEach((th, i) => {
                const indicator = th.querySelector('.sort-indicator');
                if (indicator) {
                    if (i === colIndex) {
                        th.classList.add('sorted');
                        indicator.textContent = sortDir === 'asc' ? '▲' : '▼';
                    } else {
                        th.classList.remove('sorted');
                        indicator.textContent = '▼';
                    }
                }
            });

            applyAllFilters();
            showToast(`Sorted by column ${colIndex + 1} (${sortDir === 'asc' ? 'ascending' : 'descending'})`);
        }

        function copyTableToClipboard() {
            const table = document.querySelector('#resultsContainer table');
            if (!table) { 
                showToast('No table to copy', 'error'); 
                return; 
            }
        
            const copyAll = confirm(
                `Copy Options:\\n\\n` +
                `OK = Copy all ${visibleRows.length.toLocaleString()} filtered results\\n` +
                `Cancel = Copy only current page`
            );
        
            let text = '';
            const headers = Array.from(table.querySelectorAll('th')).map(th => {
                const txt = th.textContent.trim();
                return txt.replace(/[▼▲🔽]/g, '').trim();
            }).join('\\t') + '\\n';
            
            text += headers;
        
            const rowsToCopy = copyAll ? visibleRows : visibleRows.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);
        
            rowsToCopy.forEach(row => {
                text += Array.from(row.querySelectorAll('td'))
                    .map(c => {
                        const t = c.textContent.trim();
                        return (t === 'NULL' || t.includes('NULL')) ? 'NULL' : t;
                    })
                    .join('\\t') + '\\n';
            });
        
            navigator.clipboard.writeText(text)
                .then(() => {
                    const activeFilters = Object.keys(columnFilters).filter(k => 
                        columnFilters[k].size < uniqueColumnValues[k].length
                    ).length;
                    const filterInfo = activeFilters > 0 || tableFilterText ? 
                        ` with ${activeFilters} column filters applied` : '';
                    showToast(`Copied ${rowsToCopy.length} rows${filterInfo} to clipboard!`);
                })
                .catch(() => showToast('Failed to copy', 'error'));
        }

        // Unique filenames with timestamps
        function showExportOptions() {
            const options = confirm(
                `Export Options:\\n\\n` +
                `OK = Export all ${visibleRows.length.toLocaleString()} filtered results\\n` +
                `Cancel = Export only current page (${Math.min(rowsPerPage, visibleRows.length)} rows)`
            );
            
            if (options) {
                exportAllFiltered();
            } else {
                exportCurrentPage();
            }
        }
        
        function exportCurrentPage() {
            const start = (currentPage - 1) * rowsPerPage;
            const end = start + rowsPerPage;
            const pageRows = visibleRows.slice(start, end);
            
            // Unique filename
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
            exportRows(pageRows, `nse_data_page${currentPage}_${timestamp}.csv`);
        }
        
        function exportAllFiltered() {
            // Unique filename
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
            exportRows(visibleRows, `nse_data_filtered_${timestamp}.csv`);
        }
        
        function exportRows(rows, filename) {
            const table = document.querySelector('#resultsContainer table');
            if (!table) return;
        
            let csv = '';
            
            const headers = Array.from(table.querySelectorAll('th')).map(th => {
                const txt = th.textContent.trim().replace(/[▼▲🔽]/g, '').trim();
                return txt.includes(',') ? `"${txt}"` : txt;
            }).join(',') + '\\n';
            
            csv += headers;
        
            rows.forEach(row => {
                csv += Array.from(row.querySelectorAll('td')).map(c => {
                    const t = c.textContent.trim();
                    if (t === 'NULL' || t.includes('NULL')) return 'NULL';
                    return t.includes(',') ? `"${t}"` : t;
                }).join(',') + '\\n';
            });
        
            const blob = new Blob([csv], { type: 'text/csv' });
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = filename;
            a.click();
            
            const activeFilters = Object.keys(columnFilters).filter(k => 
                columnFilters[k].size < uniqueColumnValues[k].length
            ).length;
            const filterInfo = activeFilters > 0 || tableFilterText ? 
                ` (${activeFilters} column filters + ${tableFilterText ? 'text search' : 'no text search'})` : '';
            
            showToast(`Exported ${rows.length.toLocaleString()} rows${filterInfo} to CSV!`);
        }

        function showToast(msg, type = 'success') {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.className = 'toast show' + (type === 'error' ? ' error' : '');
            setTimeout(() => toast.classList.remove('show'), 3000);
        }

        async function loadSchema() {
            const container = document.getElementById('schemaContainer');
            container.innerHTML = '<div class="spinner active" style="grid-column: 1 / -1;"></div>';

            try {
                const response = await fetch('/api/tables');
                const data = await response.json();

                if (data.error) {
                    throw new Error(data.error);
                }

                if (data.tables && data.tables.length > 0) {
                    let html = '';

                    data.tables.forEach(table => {
                        html += `
                            <div class="schema-card">
                                <div class="schema-header" style="cursor: pointer; user-select: none; display: flex; justify-content: space-between; align-items: center; padding: 1.5rem;" onclick="toggleSchemaCard(this)">
                                    <div style="display: flex; align-items: center; gap: 1rem;">
                                        <span style="font-size: 24px;">📋</span>
                                        <h3 class="schema-title">${escapeHtml(table.name)}</h3>
                                    </div>
                                    <span class="collapse-arrow" style="transition: transform 0.3s; font-size: 1.2em;">▼</span>
                                </div>
                                <div class="schema-content">
                                    <div class="schema-count">
                                        ${table.row_count.toLocaleString()} rows
                                    </div>
                                    <div class="schema-columns" style="display: block;>
                                        ${table.columns.map(col => `
                                            <div class="schema-column">
                                                <span class="schema-column-name">${escapeHtml(col.name)}</span>
                                                <span style="opacity: 0.4;">:</span>
                                                <span style="opacity: 0.6;">${escapeHtml(col.type)}</span>
                                            </div>
                                        `).join('')}
                                    </div>
                                    <button class="btn btn-secondary" style="width: 100%; margin-top: 1rem; padding: 0.75rem; font-size: 0.875rem;"
                                        onclick="event.stopPropagation(); setTableQuery('${escapeHtml(table.name)}')">
                                        <span class="btn-icon">🔍</span> Query This Table
                                    </button>
                                </div>
                            </div>
                        `;
                    });

                    container.innerHTML = html;
                } else {
                    const message = data.message || 'No tables found. Download some data first.';
                    container.innerHTML = `
                        <div class="empty-state" style="grid-column: 1 / -1;">
                            <div class="empty-state-icon">📊</div>
                            <div class="empty-state-text">No Tables Found</div>
                            <div class="empty-state-subtext">${escapeHtml(message)}</div>
                        </div>
                    `;
                }

            } catch (error) {
                container.innerHTML = `
                    <div class="empty-state" style="grid-column: 1 / -1;">
                        <div class="empty-state-icon">⚠️</div>
                        <div class="empty-state-text">Failed to Load Schema</div>
                        <div class="empty-state-subtext">${escapeHtml(error.message)}</div>
                    </div>
                `;
            }
        }
        
        function toggleSchemaCard(header) {
            const content = header.nextElementSibling;
            const arrow = header.querySelector('.collapse-arrow');
            
            if (content.style.display === 'none') {
                content.style.display = 'block';
                arrow.style.transform = 'rotate(0deg)';
            } else {
                content.style.display = 'none';
                arrow.style.transform = 'rotate(-90deg)';
            }
        }

        function setTableQuery(tableName) {
            const query = `SELECT * FROM ${tableName} ORDER BY date DESC LIMIT 100`;
            document.getElementById('sqlQuery').value = query;
            document.getElementById('sqlQuery').scrollIntoView({ behavior: 'smooth', block: 'center' });
            showToast(`Query set for table: ${tableName}`);
        }
        
        function showRowDetails(row) {
            const table = document.querySelector('#resultsContainer table');
            const headers = Array.from(table.querySelectorAll('th')).map(th => 
                th.textContent.trim().replace(/[▼▲🔽]/g, '').trim()
            );
            
            let html = '';
            Array.from(row.querySelectorAll('td')).forEach((cell, i) => {
                const value = cell.textContent.trim();
                html += `
                    <div style="padding: 1rem; border-bottom: 1px solid var(--border); display: grid; grid-template-columns: 200px 1fr; gap: 1rem;">
                        <div style="color: var(--success); font-weight: 600;">${escapeHtml(headers[i])}</div>
                        <div style="color: var(--text-secondary); word-break: break-all;">${escapeHtml(value)}</div>
                    </div>
                `;
            });
            
            document.getElementById('rowModalContent').innerHTML = html;
            document.getElementById('rowModal').classList.add('active');
        }
        
        function closeRowModal() {
            document.getElementById('rowModal').classList.remove('active');
        }
        
        // Close modals on outside click
        document.querySelectorAll('.modal').forEach(modal => {
            modal.addEventListener('click', function(e) {
                if (e.target === this) {
                    this.classList.remove('active');
                }
            });
        });
        
        // Make schema sections collapsible
        document.addEventListener('DOMContentLoaded', () => {
            document.querySelectorAll('.schema-card').forEach(card => {
                const header = card.querySelector('.schema-header');
                if (header) {
                    header.style.cursor = 'pointer';
                    header.addEventListener('click', () => {
                        const content = card.querySelector('.schema-columns');
                        if (content) {
                            content.style.display = content.style.display === 'none' ? 'block' : 'none';
                        }
                    });
                }
            });
        });
        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            // Ctrl+K or Cmd+K - Focus query box
            if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
                e.preventDefault();
                document.getElementById('sqlQuery').focus();
                showToast('Query box focused (Ctrl+K)');
            }
            
            // Ctrl+Enter - Execute query
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                const activeEl = document.activeElement;
                if (activeEl && activeEl.id === 'sqlQuery') {
                    e.preventDefault();
                    executeQuery();
                }
            }
            
            // Ctrl+E - Clear results
            if ((e.ctrlKey || e.metaKey) && e.key === 'e') {
                e.preventDefault();
                clearResults();
                showToast('Results cleared (Ctrl+E)');
            }
            
            // Ctrl+S - Save/Export
            if ((e.ctrlKey || e.metaKey) && e.key === 's') {
                if (document.getElementById('resultsContainer').classList.contains('active')) {
                    e.preventDefault();
                    showExportOptions();
                }
            }
            
            // Arrow keys for pagination
            if (e.key === 'ArrowLeft' && e.altKey) {
                e.preventDefault();
                changePage(currentPage - 1);
            }
            
            if (e.key === 'ArrowRight' && e.altKey) {
                e.preventDefault();
                changePage(currentPage + 1);
            }
            
            // Escape - Close modals
            if (e.key === 'Escape') {
                document.querySelectorAll('.modal.active').forEach(modal => {
                    modal.classList.remove('active');
                });
                closeAllFilterDropdowns();
            }
        });
    </script>
</body>
</html>
"""

if __name__ == '__main__':
    print("=" * 80)
    print("NSE Data Downloader - Enhanced Web UI")
    print("=" * 80)
    print("\nStarting server...")
    print("Open your browser to: http://localhost:5000")
    print("=" * 80)

    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)