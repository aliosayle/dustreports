"""Database connection and loading service"""

import pandas as pd
import threading
import warnings
import time as time_module
import json
import os
from datetime import datetime, time
from config.database import DATABASE_CONFIG, USE_ODBC, get_connection_string

# Try pyodbc for fast ODBC path
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

# Try interbase for direct connection fallback
try:
    import interbase
    INTERBASE_AVAILABLE = True
except ImportError:
    INTERBASE_AVAILABLE = False

# Suppress warnings to match notebook behavior
warnings.filterwarnings('ignore')

# Bulk fetch size: larger = fewer round-trips, faster load (especially with ODBC)
CURSOR_ARRAYSIZE = 50000

# Global cache for dataframes
dataframes = {}
cache_lock = threading.Lock()
cache_loading = False
cache_timestamp = None

# Set at startup: True = using ODBC, False = using direct InterBase
_using_odbc = False


def _load_table_odbc(table_name):
    """Load table via ODBC (faster bulk fetch). Returns DataFrame or None."""
    if not PYODBC_AVAILABLE:
        return None
    try:
        conn = pyodbc.connect(get_connection_string())
        cursor = conn.cursor()
        cursor.arraysize = CURSOR_ARRAYSIZE
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        # pyodbc Row objects -> list of tuples for pandas
        data = [tuple(row) for row in rows]
        df = pd.DataFrame(data, columns=columns)
        conn.close()
        return df
    except Exception:
        return None


def _load_table_direct(table_name):
    """Load table via direct InterBase connection. Returns DataFrame or None."""
    if not INTERBASE_AVAILABLE:
        return None
    try:
        dsn = f"{DATABASE_CONFIG['DATA_SOURCE']}:{DATABASE_CONFIG['DATABASE_PATH']}"
        kwargs = {
            'dsn': dsn,
            'user': DATABASE_CONFIG['USERNAME'],
            'password': DATABASE_CONFIG['PASSWORD'],
            'charset': 'NONE',
        }
        if DATABASE_CONFIG.get('CLIENT_LIBRARY'):
            kwargs['ib_library_name'] = DATABASE_CONFIG['CLIENT_LIBRARY']
        conn = interbase.connect(**kwargs)
        cursor = conn.cursor()
        try:
            cursor.arraysize = CURSOR_ARRAYSIZE
        except Exception:
            pass
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        conn.close()
        return df
    except Exception:
        return None


def connect_and_load_table(table_name):
    """Load a table: ODBC first (faster) when USE_ODBC is True, else direct InterBase."""
    global _using_odbc
    try:
        print(f"🔄 Loading table {table_name}...")
        df = None
        if USE_ODBC and PYODBC_AVAILABLE:
            df = _load_table_odbc(table_name)
            if df is not None:
                print(f"✅ {table_name}: {df.shape[0]:,} rows × {df.shape[1]} columns (ODBC)")
                return df
        if INTERBASE_AVAILABLE:
            df = _load_table_direct(table_name)
            if df is not None:
                print(f"✅ {table_name}: {df.shape[0]:,} rows × {df.shape[1]} columns (direct)")
                return df
        raise RuntimeError("ODBC and direct connection both failed or unavailable")
    except Exception as e:
        print(f"❌ {table_name}: Failed - {e}")
        return None

def load_dataframes():
    """Load all tables with descriptive names (matching notebook exactly).
    Uses ODBC when enabled for best speed.
    NOTE: This function should be called with cache_lock acquired, or it will
    acquire the lock internally to set cache_loading flag atomically.
    """
    global dataframes, cache_loading, cache_timestamp, _using_odbc

    # Ensure we set loading flag atomically
    with cache_lock:
        if cache_loading:
            print("⚠️ Cache loading already in progress, skipping duplicate load")
            return dataframes  # Return existing cache

        cache_loading = True
        print("Loading database tables...")

    # Now do the actual loading outside the lock (to avoid holding lock during I/O)
    try:
        # Test connection: prefer ODBC when enabled (much faster)
        if USE_ODBC and PYODBC_AVAILABLE:
            try:
                print("🔗 Testing ODBC connection (faster path)...")
                conn = pyodbc.connect(get_connection_string())
                conn.close()
                _using_odbc = True
                print("✅ ODBC connection OK — using ODBC for all tables")
            except Exception as e:
                print(f"⚠️ ODBC failed: {e} — falling back to direct InterBase")
                _using_odbc = False
        else:
            _using_odbc = False
            if USE_ODBC and not PYODBC_AVAILABLE:
                print("⚠️ USE_ODBC=1 but pyodbc not installed — using direct InterBase")

        if not _using_odbc and INTERBASE_AVAILABLE:
            try:
                print("🔗 Testing direct InterBase connection...")
                dsn = f"{DATABASE_CONFIG['DATA_SOURCE']}:{DATABASE_CONFIG['DATABASE_PATH']}"
                kwargs = {
                    'dsn': dsn,
                    'user': DATABASE_CONFIG['USERNAME'],
                    'password': DATABASE_CONFIG['PASSWORD'],
                    'charset': 'UTF8',
                }
                if DATABASE_CONFIG.get('CLIENT_LIBRARY'):
                    kwargs['ib_library_name'] = DATABASE_CONFIG['CLIENT_LIBRARY']
                test_conn = interbase.connect(**kwargs)
                test_conn.close()
                print("✅ Direct InterBase connection test successful")
            except Exception as e:
                with cache_lock:
                    cache_loading = False
                raise Exception(f"Database connection failed: {e}")
        elif not _using_odbc:
            with cache_lock:
                cache_loading = False
            raise Exception("Install pyodbc and an InterBase ODBC driver, or the interbase Python package")

        # Load all tables (ODBC or direct per connect_and_load_table)
        sites_df = connect_and_load_table('ALLSTOCK')          # Site/Location master data
        categories_df = connect_and_load_table('DETDESCR')     # Category definitions  
        invoice_headers_df = connect_and_load_table('INVOICE') # Invoice headers
        sales_details_df = connect_and_load_table('ITEMS')     # Sales transaction details
        vouchers_df = connect_and_load_table('PAYM')           # Payment vouchers
        accounts_df = connect_and_load_table('SUB')            # Accounts/Sub-accounts data
        inventory_items_df = connect_and_load_table('STOCK')   # Items/Products master
        inventory_transactions_df = connect_and_load_table('ALLITEM') # All inventory transactions
        
        # Create dataframes dictionary with descriptive names (matching notebook exactly)
        temp_dataframes = {
            'sites': sites_df,
            'categories': categories_df, 
            'invoice_headers': invoice_headers_df,
            'sales_details': sales_details_df,
            'vouchers': vouchers_df,
            'accounts': accounts_df,
            'inventory_items': inventory_items_df,
            'inventory_transactions': inventory_transactions_df
        }
        
        # Remove None values and show summary (matching notebook exactly)
        new_dataframes = {k: v for k, v in temp_dataframes.items() if v is not None}
        print(f"\n✅ Successfully loaded {len(new_dataframes)} tables:")
        for name, df in new_dataframes.items():
            print(f"  {name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        if len(new_dataframes) == 0:
            with cache_lock:
                cache_loading = False
            raise Exception("No tables were loaded successfully")
            
        # Atomically replace cache to prevent inconsistent reads
        with cache_lock:
            dataframes.clear()
            dataframes.update(new_dataframes)
            cache_timestamp = datetime.now()
            cache_loading = False
            print(f"\n🕒 Cache loaded successfully at: {cache_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return dataframes
        
    except Exception as e:
        print(f"❌ Error in load_dataframes: {e}")
        # Ensure loading flag is reset even on error
        with cache_lock:
            cache_loading = False
        raise e

def get_dataframes():
    """Get the cached dataframes"""
    return dataframes

def is_cache_loading():
    """Check if cache is currently loading"""
    return cache_loading

def get_cache_lock():
    """Get the cache lock"""
    return cache_lock

def get_cache_timestamp():
    """Get the cache timestamp"""
    return cache_timestamp

def get_cache_age_seconds():
    """Get cache age in seconds"""
    if cache_timestamp is None:
        return None
    return (datetime.now() - cache_timestamp).total_seconds()

# Scheduled reload configuration
_scheduled_reload_enabled = False
_scheduled_reload_thread = None
_scheduled_reload_times = []  # List of time objects for scheduled reloads

# Config file path for persisting scheduled reload settings
_CONFIG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEDULED_RELOAD_CONFIG_FILE = os.path.join(_CONFIG_DIR, 'scheduled_reload_config.json')

def load_scheduled_reload_config():
    """Load scheduled reload config from JSON file
    
    Returns:
        bool: True if config was loaded successfully, False otherwise
    """
    global _scheduled_reload_times, _scheduled_reload_enabled
    
    try:
        if os.path.exists(SCHEDULED_RELOAD_CONFIG_FILE):
            with open(SCHEDULED_RELOAD_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                enabled = config.get('enabled', False)
                times_str = config.get('reload_times', [])
                
                if times_str:
                    # Convert string times to time objects
                    times = []
                    for t_str in times_str:
                        hour, minute = map(int, t_str.split(':'))
                        times.append(time(hour, minute))
                    _scheduled_reload_times = times
                else:
                    # Default times if none specified
                    _scheduled_reload_times = [time(6, 0), time(12, 0), time(18, 0)]
                
                _scheduled_reload_enabled = enabled
                print(f"✅ Loaded scheduled reload config: enabled={enabled}, times={times_str}")
                return True
        else:
            print("ℹ️ No scheduled reload config file found, using defaults")
            return False
    except Exception as e:
        print(f"⚠️ Failed to load scheduled reload config: {e}")
        return False

def save_scheduled_reload_config():
    """Save scheduled reload config to JSON file
    
    Returns:
        bool: True if config was saved successfully, False otherwise
    """
    global _scheduled_reload_enabled, _scheduled_reload_times
    
    try:
        config = {
            'enabled': _scheduled_reload_enabled,
            'reload_times': [t.strftime('%H:%M') for t in _scheduled_reload_times],
            'last_updated': datetime.now().isoformat()
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(SCHEDULED_RELOAD_CONFIG_FILE), exist_ok=True)
        
        with open(SCHEDULED_RELOAD_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Saved scheduled reload config: enabled={_scheduled_reload_enabled}, times={config['reload_times']}")
        return True
    except Exception as e:
        print(f"❌ Failed to save scheduled reload config: {e}")
        return False

def start_scheduled_reload(reload_times=None):
    """Start background thread for scheduled cache reloads
    
    Args:
        reload_times: List of time objects (e.g., [time(6, 0), time(12, 0), time(18, 0)])
                     If None, uses saved config or default times: 6 AM, 12 PM, 6 PM
    """
    global _scheduled_reload_enabled, _scheduled_reload_thread, _scheduled_reload_times
    
    if _scheduled_reload_enabled:
        print("⚠️ Scheduled reload already running")
        return
    
    if reload_times is None:
        # If no times provided, check if we have saved config
        if not _scheduled_reload_times:
            # Try to load from file, or use defaults
            if not load_scheduled_reload_config():
                # Default: reload at 6 AM, 12 PM, and 6 PM daily
                _scheduled_reload_times = [time(6, 0), time(12, 0), time(18, 0)]
    else:
        _scheduled_reload_times = reload_times
    
    _scheduled_reload_enabled = True
    
    # Save config to file
    save_scheduled_reload_config()
    
    def scheduled_reload_worker():
        """Background thread for scheduled reloads"""
        print(f"🕐 Scheduled reload worker started. Reload times: {[t.strftime('%H:%M') for t in _scheduled_reload_times]}")
        last_reload_minute = -1
        
        while _scheduled_reload_enabled:
            try:
                now = datetime.now()
                current_time = now.time()
                current_minute = now.minute
                
                # Check each scheduled reload time
                for reload_time in _scheduled_reload_times:
                    # Check if it's the right hour and minute (check once per minute)
                    if (current_time.hour == reload_time.hour and 
                        current_time.minute == reload_time.minute and
                        current_minute != last_reload_minute):
                        
                        print(f"🔄 Scheduled reload triggered at {current_time.strftime('%H:%M:%S')}")
                        last_reload_minute = current_minute
                        
                        try:
                            # Check if already loading
                            if not is_cache_loading():
                                print("📊 Starting scheduled cache reload...")
                                load_dataframes()
                                print("✅ Scheduled cache reload completed")
                            else:
                                print("⏳ Cache already loading, skipping scheduled reload")
                        except Exception as e:
                            print(f"❌ Scheduled reload failed: {e}")
                
                # Sleep for 60 seconds before next check
                time_module.sleep(60)
                
            except Exception as e:
                print(f"❌ Error in scheduled reload worker: {e}")
                time_module.sleep(60)  # Continue even on error
    
    _scheduled_reload_thread = threading.Thread(target=scheduled_reload_worker, daemon=True)
    _scheduled_reload_thread.start()
    print("✅ Scheduled reload worker started")

def stop_scheduled_reload():
    """Stop the scheduled reload background thread"""
    global _scheduled_reload_enabled, _scheduled_reload_thread
    
    if not _scheduled_reload_enabled:
        print("⚠️ Scheduled reload not running")
        return
    
    _scheduled_reload_enabled = False
    if _scheduled_reload_thread:
        _scheduled_reload_thread.join(timeout=5)
    
    # Save config to file (with enabled=False)
    save_scheduled_reload_config()
    
    print("🛑 Scheduled reload worker stopped")

def is_scheduled_reload_enabled():
    """Check if scheduled reload is enabled"""
    return _scheduled_reload_enabled

def get_scheduled_reload_times():
    """Get list of scheduled reload times"""
    return [t.strftime('%H:%M') for t in _scheduled_reload_times]
