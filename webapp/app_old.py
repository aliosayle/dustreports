from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import pyodbc
import numpy as np
from datetime import datetime, timedelta
import io
import json
import threading
import warnings
from werkzeug.exceptions import BadRequest

# Suppress pandas warnings to match notebook behavior
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', category=DeprecationWarning, module='pandas')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'iba-dust-reports-2025'

# Global cache for dataframes
cached_dataframes = {}
cache_timestamp = None
cache_loading = False  # Flag to prevent multiple simultaneous loads
cache_lock = threading.Lock()  # Thread lock for cache operations

# Database connection parameters
DATABASE_CONFIG = {
    'DATA_SOURCE': "100.200.2.1",
    'DATABASE_PATH': r"D:\dolly2008\fer2015.dol",
    'USERNAME': "ALIOSS",
    'PASSWORD': "$9-j[+Mo$AA833C4FA$",
    'CLIENT_LIBRARY': r"C:\Users\User\Downloads\Compressed\ibclient64-14.1_x86-64\ibclient64-14.1.dll"
}

def get_connection_string():
    """Get the database connection string"""
    return (
        f"DRIVER=Devart ODBC Driver for InterBase;"
        f"Data Source={DATABASE_CONFIG['DATA_SOURCE']};"
        f"Database={DATABASE_CONFIG['DATABASE_PATH']};"
        f"User ID={DATABASE_CONFIG['USERNAME']};"
        f"Password={DATABASE_CONFIG['PASSWORD']};"
        f"Client Library={DATABASE_CONFIG['CLIENT_LIBRARY']};"
    )

def connect_and_load_table(table_name):
    """Load a table from the database"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to database (attempt {attempt + 1})...")
            conn = pyodbc.connect(get_connection_string(), timeout=30)
            print(f"Connected successfully, loading {table_name}...")
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()
            print(f"Successfully loaded {table_name}: {len(df)} rows")
            return df
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for table {table_name}: {e}")
            if attempt < max_retries - 1:
                import time
                time.sleep(1)  # Wait 1 second before retry
            else:
                print(f"All attempts failed for table {table_name}")
                return None

def load_all_tables():
    """Load all required tables"""
    global cached_dataframes, cache_timestamp, cache_loading
    
    with cache_lock:
        # Check if we have cached data that's less than 5 minutes old
        if cached_dataframes and cache_timestamp:
            time_diff = datetime.now() - cache_timestamp
            if time_diff.total_seconds() < 300:  # 5 minutes
                print("Using cached dataframes")
                return cached_dataframes
        
        # Check if cache is already being loaded by another request
        if cache_loading:
            print("Cache loading in progress, returning existing cache if available")
            # Return cached data if available, even if old
            if cached_dataframes:
                return cached_dataframes
            # If no cached data, wait a bit and return empty
            return {}
        
        cache_loading = True
    
    try:
        print("Loading fresh dataframes from database...")
        tables = {
            'sites_df': connect_and_load_table('ALLSTOCK'),
            'categories_df': connect_and_load_table('DETDESCR'),
            'invoice_headers_df': connect_and_load_table('INVOICE'),
            'sales_details_df': connect_and_load_table('ITEMS'),
            'vouchers_df': connect_and_load_table('PAYM'),
            'accounts_df': connect_and_load_table('SACCOUNT'),
            'inventory_items_df': connect_and_load_table('STOCK'),
            'inventory_transactions_df': connect_and_load_table('ALLITEM')
        }
        
        with cache_lock:
            # Cache the dataframes and timestamp
            cached_dataframes = tables
            cache_timestamp = datetime.now()
            print("Dataframes cached successfully")
        
        return tables
    finally:
        with cache_lock:
            cache_loading = False

def clear_cache():
    """Clear the dataframes cache"""
    global cached_dataframes, cache_timestamp, cache_loading
    with cache_lock:
        cached_dataframes = {}
        cache_timestamp = None
        cache_loading = False
        print("Cache cleared")

def calculate_stock_and_sales(dataframes, item_code=None, site_code=None, from_date=None, to_date=None, show_details=False):
    """
    Calculate current stock and sales for an item at a specific site or across all sites
    """
    from datetime import datetime
    
    inventory_transactions_df = dataframes.get('inventory_transactions_df')
    sales_details_df = dataframes.get('sales_details_df')
    sites_df = dataframes.get('sites_df')
    inventory_items_df = dataframes.get('inventory_items_df')
    
    if inventory_transactions_df is None:
        return None
    
    # Start with inventory transactions for stock calculation
    df_stock = inventory_transactions_df.copy()
    
    # Filter by item if specified
    if item_code:
        df_stock = df_stock[df_stock['ITEM'] == item_code]
        if df_stock.empty:
            return None
    
    # Filter by site if specified  
    if site_code:
        df_stock = df_stock[df_stock['SITE'] == site_code]
        if df_stock.empty:
            return None
    
    # Fill NaN values with 0 for calculations
    df_stock['DEBITQTY'] = df_stock['DEBITQTY'].fillna(0)
    df_stock['CREDITQTY'] = df_stock['CREDITQTY'].fillna(0)
    
    # Calculate stock by grouping
    if item_code and site_code:
        # Single item, single site
        result_df = pd.DataFrame({
            'SITE': [site_code],
            'ITEM': [item_code],
            'TOTAL_IN': [df_stock['DEBITQTY'].sum()],
            'TOTAL_OUT': [df_stock['CREDITQTY'].sum()],
            'CURRENT_STOCK': [df_stock['DEBITQTY'].sum() - df_stock['CREDITQTY'].sum()],
            'STOCK_TRANSACTIONS': [len(df_stock)]
        })
    elif item_code:
        # Single item, all sites
        result_df = df_stock.groupby('SITE').agg({
            'DEBITQTY': 'sum',
            'CREDITQTY': 'sum',
            'ITEM': 'first'
        }).reset_index()
        result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
        result_df['STOCK_TRANSACTIONS'] = df_stock.groupby('SITE').size().values
        result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
        result_df = result_df[['SITE', 'ITEM', 'TOTAL_IN', 'TOTAL_OUT', 'CURRENT_STOCK', 'STOCK_TRANSACTIONS']]
    elif site_code:
        # All items, single site
        result_df = df_stock.groupby('ITEM').agg({
            'DEBITQTY': 'sum',
            'CREDITQTY': 'sum',
            'SITE': 'first'
        }).reset_index()
        result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
        result_df['STOCK_TRANSACTIONS'] = df_stock.groupby('ITEM').size().values
        result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
        result_df = result_df[['SITE', 'ITEM', 'TOTAL_IN', 'TOTAL_OUT', 'CURRENT_STOCK', 'STOCK_TRANSACTIONS']]
    else:
        # All items, all sites
        result_df = df_stock.groupby(['SITE', 'ITEM']).agg({
            'DEBITQTY': 'sum',
            'CREDITQTY': 'sum'
        }).reset_index()
        result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
        result_df['STOCK_TRANSACTIONS'] = df_stock.groupby(['SITE', 'ITEM']).size().values
        result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
    
    # Calculate sales analytics from sales_details_df (ITEMS table)
    if sales_details_df is not None:
        df_sales = sales_details_df.copy()
        
        # Filter sales by item and site
        if item_code:
            df_sales = df_sales[df_sales['ITEM'] == item_code]
        if site_code:
            df_sales = df_sales[df_sales['SITE'] == site_code]
        
        # Convert FDATE to datetime if it's not already
        if 'FDATE' in df_sales.columns:
            df_sales['FDATE'] = pd.to_datetime(df_sales['FDATE'], errors='coerce')
            
            # Filter by date range if specified
            if from_date:
                from_date_dt = pd.to_datetime(from_date)
                df_sales = df_sales[df_sales['FDATE'] >= from_date_dt]
            if to_date:
                to_date_dt = pd.to_datetime(to_date)
                df_sales = df_sales[df_sales['FDATE'] <= to_date_dt]
          # Filter for sales transactions (FTYPE = 1 for sales, FTYPE = 2 for returns)
        if 'FTYPE' in df_sales.columns:
            sales_only = df_sales[df_sales['FTYPE'].isin([1, 2])].copy()
        else:
            sales_only = df_sales.copy()
        
        # Fill NaN values
        sales_only['QTY'] = sales_only['QTY'].fillna(0)
        
        # Calculate daily sales analytics
        def calculate_sales_analytics(group, from_date_param=None, to_date_param=None):
            if group.empty or 'FDATE' not in group.columns:
                return pd.Series({
                    'MAX_DAILY_SALES': 0,
                    'MIN_DAILY_SALES': 0, 
                    'AVG_DAILY_SALES': 0,
                    'SALES_TRANSACTIONS': 0,
                    'TOTAL_SALES_QTY': 0,
                    'SALES_PERIOD_DAYS': 0
                })
            
            # Group by date and sum quantities
            daily_sales = group.groupby('FDATE')['QTY'].sum()
            
            # Calculate period metrics
            total_sales_qty = group['QTY'].sum()
            
            # Use specified date range if provided, otherwise use actual sales date range
            if from_date_param and to_date_param:
                from_dt = pd.to_datetime(from_date_param)
                to_dt = pd.to_datetime(to_date_param)
                period_days = (to_dt - from_dt).days + 1
            elif from_date_param:
                from_dt = pd.to_datetime(from_date_param)
                max_date = group['FDATE'].max()
                period_days = (max_date - from_dt).days + 1
            elif to_date_param:
                min_date = group['FDATE'].min()
                to_dt = pd.to_datetime(to_date_param)
                period_days = (to_dt - min_date).days + 1
            else:
                # No date range specified, use actual sales period
                min_date = group['FDATE'].min()
                max_date = group['FDATE'].max()
                period_days = (max_date - min_date).days + 1 if min_date != max_date else 1
            
            # Exclude zero sales days for min calculation
            non_zero_sales = daily_sales[daily_sales > 0]
            
            max_sales = daily_sales.max() if not daily_sales.empty else 0
            min_sales = non_zero_sales.min() if not non_zero_sales.empty else 0
            
            # Calculate average daily sales using the full specified period
            avg_sales = total_sales_qty / period_days if period_days > 0 else 0
            
            total_transactions = len(group)
            
            return pd.Series({
                'MAX_DAILY_SALES': max_sales,
                'MIN_DAILY_SALES': min_sales,
                'AVG_DAILY_SALES': avg_sales,
                'SALES_TRANSACTIONS': total_transactions,
                'TOTAL_SALES_QTY': total_sales_qty,
                'SALES_PERIOD_DAYS': period_days
            })
          # Calculate analytics by same grouping as stock
        if item_code and site_code:
            # Single item, single site
            analytics = calculate_sales_analytics(sales_only, from_date, to_date)
            sales_analytics = pd.DataFrame({
                'SITE': [site_code],                'ITEM': [item_code],
                'MAX_DAILY_SALES': [analytics['MAX_DAILY_SALES']],
                'MIN_DAILY_SALES': [analytics['MIN_DAILY_SALES']],
                'AVG_DAILY_SALES': [analytics['AVG_DAILY_SALES']],
                'SALES_TRANSACTIONS': [analytics['SALES_TRANSACTIONS']],
                'TOTAL_SALES_QTY': [analytics['TOTAL_SALES_QTY']],
                'SALES_PERIOD_DAYS': [analytics['SALES_PERIOD_DAYS']]
            })
        elif item_code:
            # Single item, all sites
            sales_analytics = sales_only.groupby('SITE').apply(lambda x: calculate_sales_analytics(x, from_date, to_date), include_groups=False).reset_index()
            sales_analytics['ITEM'] = item_code
        elif site_code:
            # All items, single site
            sales_analytics = sales_only.groupby('ITEM').apply(lambda x: calculate_sales_analytics(x, from_date, to_date), include_groups=False).reset_index()
            sales_analytics['SITE'] = site_code
        else:
            # All items, all sites
            sales_analytics = sales_only.groupby(['SITE', 'ITEM']).apply(lambda x: calculate_sales_analytics(x, from_date, to_date), include_groups=False).reset_index()
        
        # Merge stock and sales analytics
        result_df = result_df.merge(sales_analytics[['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS']], 
                                   on=['SITE', 'ITEM'], how='left')
        result_df['MAX_DAILY_SALES'] = result_df['MAX_DAILY_SALES'].fillna(0)
        result_df['MIN_DAILY_SALES'] = result_df['MIN_DAILY_SALES'].fillna(0)
        result_df['AVG_DAILY_SALES'] = result_df['AVG_DAILY_SALES'].fillna(0)
        result_df['SALES_TRANSACTIONS'] = result_df['SALES_TRANSACTIONS'].fillna(0)
        result_df['TOTAL_SALES_QTY'] = result_df['TOTAL_SALES_QTY'].fillna(0)
        result_df['SALES_PERIOD_DAYS'] = result_df['SALES_PERIOD_DAYS'].fillna(0)
    else:
        # Add empty sales columns if sales data not available
        result_df['MAX_DAILY_SALES'] = 0
        result_df['MIN_DAILY_SALES'] = 0
        result_df['AVG_DAILY_SALES'] = 0
        result_df['SALES_TRANSACTIONS'] = 0
        result_df['TOTAL_SALES_QTY'] = 0
        result_df['SALES_PERIOD_DAYS'] = 0
    
    # Calculate stock autonomy (days of stock remaining at current sales rate)
    result_df['STOCK_AUTONOMY_DAYS'] = result_df.apply(
        lambda row: (row['CURRENT_STOCK'] / row['AVG_DAILY_SALES']) 
        if row['AVG_DAILY_SALES'] > 0 else float('inf'), axis=1
    )
    # Cap infinity values at 9999 for display purposes
    result_df['STOCK_AUTONOMY_DAYS'] = result_df['STOCK_AUTONOMY_DAYS'].replace(float('inf'), 9999)
    
    # Add site names if available
    if sites_df is not None:
        site_names = sites_df[['ID', 'SITE']].drop_duplicates()
        site_names = site_names.rename(columns={'ID': 'SITE', 'SITE': 'SITE_NAME'})
        result_df = result_df.merge(site_names, on='SITE', how='left')
    
    # Add item names if available
    if inventory_items_df is not None:
        # Use DESCR1 as primary item name (item description)
        item_names = inventory_items_df[['ITEM', 'DESCR1']].drop_duplicates()
        item_names['ITEM_NAME'] = item_names['DESCR1'].fillna('').astype(str)
        
        # Merge with result_df
        result_df = result_df.merge(
            item_names[['ITEM', 'ITEM_NAME']], 
            on='ITEM', how='left'
        )
    else:
        # If no item data available, create empty column
        result_df['ITEM_NAME'] = None
    
    # Reorder columns to show: Site, Item, Item Description, Current Stock, Total Sales, Avg Sales/Day, Max/Min Daily, Stock Autonomy, Transactions
    final_cols = ['SITE', 'ITEM', 'ITEM_NAME', 'CURRENT_STOCK', 'TOTAL_SALES_QTY', 'AVG_DAILY_SALES', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'STOCK_AUTONOMY_DAYS', 'SALES_TRANSACTIONS']
    
    # Keep only the columns we want in the final output
    available_cols = [col for col in final_cols if col in result_df.columns]
    result_df = result_df[available_cols]
    
    # Sort by current stock descending
    result_df = result_df.sort_values('CURRENT_STOCK', ascending=False)
    
    return result_df

@app.route('/')
def index():
    """Home page"""
    return render_template('index.html')

@app.route('/autonomy')
def autonomy_report():
    """Autonomy report page"""
    return render_template('autonomy.html')

@app.route('/api/test-connection')
def test_connection():
    """Test database connection"""
    try:
        print("Testing database connection...")
        conn = pyodbc.connect(get_connection_string(), timeout=30)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM ALLSTOCK")
        count = cursor.fetchone()[0]
        conn.close()
        return jsonify({'status': 'success', 'message': f'Connection successful. ALLSTOCK has {count} rows.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/load-dataframes', methods=['POST'])
def load_dataframes():
    """Load/reload all dataframes"""
    try:
        clear_cache()  # Clear existing cache
        dataframes = load_all_tables()
        
        # Count loaded tables
        loaded_count = sum(1 for df in dataframes.values() if df is not None)
        total_count = len(dataframes)
        
        return jsonify({
            'status': 'success',
            'message': f'Loaded {loaded_count}/{total_count} tables successfully',
            'loaded_tables': [name for name, df in dataframes.items() if df is not None],
            'cache_time': cache_timestamp.isoformat() if cache_timestamp else None
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/cache-status')
def cache_status():
    """Get cache status"""
    global cached_dataframes, cache_timestamp
    
    if cached_dataframes and cache_timestamp:
        time_diff = datetime.now() - cache_timestamp
        cached_tables = [name for name, df in cached_dataframes.items() if df is not None]
        return jsonify({
            'cached': True,
            'cache_age_seconds': int(time_diff.total_seconds()),
            'cache_time': cache_timestamp.isoformat(),
            'cached_tables': cached_tables,
            'table_count': len(cached_tables)
        })
    else:
        return jsonify({
            'cached': False,
            'cache_age_seconds': 0,
            'cache_time': None,
            'cached_tables': [],
            'table_count': 0
        })

@app.route('/api/sites')
def get_sites():
    """Get list of all sites"""
    try:
        # Use cached data if available, otherwise load fresh
        dataframes = load_all_tables()
        sites_df = dataframes.get('sites_df')
        
        if sites_df is not None:
            sites = sites_df[['ID', 'SITE']].drop_duplicates().sort_values('ID')
            sites_list = [{'id': row['ID'], 'name': row['SITE']} for _, row in sites.iterrows()]
            return jsonify({'sites': sites_list})
        return jsonify({'error': 'Could not load sites'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/items')
def get_items():
    """Get list of all items"""
    try:
        # Use cached data if available, otherwise load fresh
        dataframes = load_all_tables()
        items_df = dataframes.get('inventory_items_df')
        
        if items_df is not None:
            items = items_df[['ITEM', 'DESCR1']].drop_duplicates().sort_values('ITEM')
            items_list = [{'id': row['ITEM'], 'name': f"{row['ITEM']} - {row['DESCR1']}"} for _, row in items.iterrows()]
            return jsonify({'items': items_list})
        return jsonify({'error': 'Could not load items'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/autonomy-report', methods=['POST'])
def generate_autonomy_report():
    """Generate autonomy report based on filters"""
    try:
        data = request.get_json()
        item_code = data.get('item_code') if data.get('item_code') else None
        site_code = data.get('site_code') if data.get('site_code') else None
        from_date = data.get('from_date') if data.get('from_date') else None
        to_date = data.get('to_date') if data.get('to_date') else None
        
        # Load all tables
        dataframes = load_all_tables()
        
        # Calculate stock and sales
        result_df = calculate_stock_and_sales(
            dataframes, 
            item_code=item_code,
            site_code=site_code, 
            from_date=from_date,
            to_date=to_date
        )
        
        if result_df is None or result_df.empty:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Convert to JSON format
        result_df = result_df.round(2)  # Round to 2 decimal places
        result_data = result_df.to_dict('records')
        
        # Add summary statistics
        summary = {
            'total_items': len(result_df),
            'total_stock': float(result_df['CURRENT_STOCK'].sum()),
            'items_with_stock': int((result_df['CURRENT_STOCK'] > 0).sum()),
            'items_low_autonomy': int((result_df['STOCK_AUTONOMY_DAYS'] < 30).sum()),
            'avg_autonomy': float(result_df[result_df['STOCK_AUTONOMY_DAYS'] < 9999]['STOCK_AUTONOMY_DAYS'].mean()) if len(result_df[result_df['STOCK_AUTONOMY_DAYS'] < 9999]) > 0 else 0
        }
        
        return jsonify({
            'data': result_data,
            'summary': summary,
            'filters': {
                'item_code': item_code,
                'site_code': site_code,
                'from_date': from_date,
                'to_date': to_date
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-autonomy', methods=['POST'])
def export_autonomy_report():
    """Export autonomy report to Excel"""
    try:
        data = request.get_json()
        item_code = data.get('item_code') if data.get('item_code') else None
        site_code = data.get('site_code') if data.get('site_code') else None
        from_date = data.get('from_date') if data.get('from_date') else None
        to_date = data.get('to_date') if data.get('to_date') else None
        
        # Load all tables
        dataframes = load_all_tables()
        
        # Calculate stock and sales
        result_df = calculate_stock_and_sales(
            dataframes, 
            item_code=item_code,
            site_code=site_code, 
            from_date=from_date,
            to_date=to_date
        )
        
        if result_df is None or result_df.empty:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Create Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name='Autonomy Report', index=False)
        
        output.seek(0)
        
        filename = f"autonomy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
