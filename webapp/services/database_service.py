"""Database connection and loading service"""

import pandas as pd
import threading
import warnings
from config.database import DATABASE_CONFIG

# Try to import interbase for direct InterBase connection
try:
    import interbase
    INTERBASE_AVAILABLE = True
    print("✅ InterBase Python driver available for direct connection")
except ImportError:
    INTERBASE_AVAILABLE = False
    print("⚠️ InterBase Python driver not available, will use ODBC as fallback")

# Suppress warnings to match notebook behavior
warnings.filterwarnings('ignore')

# Global cache for dataframes
dataframes = {}
cache_lock = threading.Lock()
cache_loading = False

def connect_and_load_table(table_name):
    """Load a table from the database using direct InterBase connection only"""
    try:
        print(f"🔄 Connecting to database for table {table_name}...")
        
        # Use direct InterBase connection only (no fallback to ODBC)
        if not INTERBASE_AVAILABLE:
            raise Exception("InterBase Python library not available")
        
        print(f"🔗 Using direct InterBase connection for {table_name}...")
        
        # Build direct connection for InterBase
        # Format: host:database_path
        dsn = f"{DATABASE_CONFIG['DATA_SOURCE']}:{DATABASE_CONFIG['DATABASE_PATH']}"
        print(f"📡 DSN: {dsn}")
        print(f"📚 Client Library: {DATABASE_CONFIG.get('CLIENT_LIBRARY', 'system default')}")
        
        # Connect with explicit client library if specified
        if 'CLIENT_LIBRARY' in DATABASE_CONFIG:
            conn = interbase.connect(
                dsn=dsn,
                user=DATABASE_CONFIG['USERNAME'],
                password=DATABASE_CONFIG['PASSWORD'],
                ib_library_name=DATABASE_CONFIG['CLIENT_LIBRARY'],
                charset='NONE'  # Use UTF-8 charset for better character compatibility
            )
        else:
            conn = interbase.connect(
                dsn=dsn,
                user=DATABASE_CONFIG['USERNAME'],
                password=DATABASE_CONFIG['PASSWORD'],
                charset='NONE'  # Use UTF-8 charset for better character compatibility
            )
        
        print(f"✅ Direct InterBase connection successful for {table_name}")
        
        # Execute query and fetch data
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        conn.close()
        print(f"✅ {table_name}: {df.shape[0]:,} rows × {df.shape[1]} columns (direct connection)")
        return df
        
    except Exception as e:
        print(f"❌ {table_name}: Failed to load - {e}")
        print(f"   Error type: {type(e).__name__}")
        print(f"   DSN attempted: {DATABASE_CONFIG['DATA_SOURCE']}:{DATABASE_CONFIG['DATABASE_PATH']}")
        print(f"   Client Library: {DATABASE_CONFIG.get('CLIENT_LIBRARY', 'system default')}")
        return None

def load_dataframes():
    """Load all tables with descriptive names (matching notebook exactly)"""
    global dataframes, cache_loading
    
    try:
        print("Loading database tables...")
        
        # Test connection first
        try:
            if INTERBASE_AVAILABLE:
                try:
                    print("🔗 Testing direct InterBase connection...")
                    dsn = f"{DATABASE_CONFIG['DATA_SOURCE']}:{DATABASE_CONFIG['DATABASE_PATH']}"
                    print(f"📡 Connecting to DSN: {dsn}")
                    print(f"👤 User: {DATABASE_CONFIG['USERNAME']}")
                    print(f"📚 Client Library: {DATABASE_CONFIG.get('CLIENT_LIBRARY', 'system default')}")
                    
                    # Connect with explicit client library if specified
                    if 'CLIENT_LIBRARY' in DATABASE_CONFIG:
                        test_conn = interbase.connect(
                            dsn=dsn,
                            user=DATABASE_CONFIG['USERNAME'],
                            password=DATABASE_CONFIG['PASSWORD'],
                            ib_library_name=DATABASE_CONFIG['CLIENT_LIBRARY'],
                            charset='UTF8'  # Use UTF-8 charset for better character compatibility
                        )
                    else:
                        test_conn = interbase.connect(
                            dsn=dsn,
                            user=DATABASE_CONFIG['USERNAME'],
                            password=DATABASE_CONFIG['PASSWORD'],
                            charset='UTF8'  # Use UTF-8 charset for better character compatibility
                        )
                    test_conn.close()
                    print("✅ Direct InterBase connection test successful")
                    print("🚀 Will use direct InterBase connection for all tables")
                except Exception as interbase_error:
                    print(f"⚠️ Direct InterBase connection test failed: {interbase_error}")
                    print(f"� Error type: {type(interbase_error).__name__}")
                    print("❌ WILL NOT fallback to ODBC - fixing direct connection...")
                    raise Exception(f"Direct InterBase connection failed: {interbase_error}")
            else:
                print("❌ InterBase library not available")
                raise Exception("InterBase library not available")
        except Exception as e:
            print(f"❌ Database connection test failed: {e}")
            cache_loading = False
            raise Exception(f"Database connection failed: {e}")
        
        sites_df = connect_and_load_table('ALLSTOCK')          # Site/Location master data
        categories_df = connect_and_load_table('DETDESCR')     # Category definitions  
        invoice_headers_df = connect_and_load_table('INVOICE') # Invoice headers
        sales_details_df = connect_and_load_table('ITEMS')     # Sales transaction details
        vouchers_df = connect_and_load_table('PAYM')           # Payment vouchers
        accounts_df = connect_and_load_table('SACCOUNT')       # Statement of accounts
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
        dataframes = {k: v for k, v in temp_dataframes.items() if v is not None}
        print(f"\n✅ Successfully loaded {len(dataframes)} tables:")
        for name, df in dataframes.items():
            print(f"  {name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        if len(dataframes) == 0:
            raise Exception("No tables were loaded successfully")
            
        cache_loading = False
        return dataframes
        
    except Exception as e:
        print(f"❌ Error in load_dataframes: {e}")
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
