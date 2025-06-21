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

# Suppress warnings to match notebook behavior
warnings.filterwarnings('ignore')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'iba-dust-reports-2025'

# Database connection parameters (matching notebook exactly)
DATABASE_CONFIG = {
    'DATA_SOURCE': "100.200.2.1",
    'DATABASE_PATH': r"D:\dolly2008\fer2015.dol",
    'USERNAME': "ALIOSS",
    'PASSWORD': "$9-j[+Mo$AA833C4FA$",
    'CLIENT_LIBRARY': r"C:\Users\User\Downloads\Compressed\ibclient64-14.1_x86-64\ibclient64-14.1.dll"
}

connection_string = (
    f"DRIVER=Devart ODBC Driver for InterBase;"
    f"Data Source={DATABASE_CONFIG['DATA_SOURCE']};"
    f"Database={DATABASE_CONFIG['DATABASE_PATH']};"
    f"User ID={DATABASE_CONFIG['USERNAME']};"
    f"Password={DATABASE_CONFIG['PASSWORD']};"
    f"Client Library={DATABASE_CONFIG['CLIENT_LIBRARY']};"
)

# Global cache for dataframes (matching notebook structure)
dataframes = {}
cache_lock = threading.Lock()
cache_loading = False

def connect_and_load_table(table_name):
    """Load a table from the database using manual DataFrame creation (matching notebook exactly)"""
    try:
        print(f"🔄 Connecting to database for table {table_name}...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print(f"✅ Connected successfully, loading {table_name}...")
        
        # Execute query and get column names
        cursor.execute(f"SELECT * FROM {table_name}")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to DataFrame manually
        df = pd.DataFrame([list(row) for row in rows], columns=columns)
        
        conn.close()
        print(f"✅ {table_name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"❌ {table_name}: Failed to load - {e}")
        print(f"   Connection string: {connection_string[:50]}...")
        return None

def load_dataframes():
    """Load all tables with descriptive names (matching notebook exactly)"""
    global dataframes, cache_loading
    
    try:
        print("Loading database tables...")
        
        # Test connection first
        try:
            test_conn = pyodbc.connect(connection_string)
            test_conn.close()
            print("✅ Database connection test successful")
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

def calculate_stock_and_sales(item_code=None, site_code=None, from_date=None, to_date=None):
    """
    Calculate current stock and sales (matching notebook exactly)
    """
    if not dataframes:
        return None
        
    # Start with inventory transactions for stock calculation
    df_stock = dataframes['inventory_transactions'].copy()
    
    # Filter by item if specified
    if item_code:
        df_stock = df_stock[df_stock['ITEM'] == item_code]
        if df_stock.empty:
            print(f"❌ No stock transactions found for item: {item_code}")
            return None
    
    # Filter by site if specified  
    if site_code:
        df_stock = df_stock[df_stock['SITE'] == site_code]
        if df_stock.empty:
            print(f"❌ No stock transactions found for site: {site_code}")
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
    if 'sales_details' in dataframes and dataframes['sales_details'] is not None:
        df_sales = dataframes['sales_details'].copy()
        
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
            sales_only = df_sales[df_sales['FTYPE'].isin([1, 2])]
        else:
            sales_only = df_sales
        
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
                'SITE': [site_code],
                'ITEM': [item_code],
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
    def calculate_autonomy(row):
        if row['CURRENT_STOCK'] <= 0:
            return -1  # Use -1 to represent N/A for zero stock
        elif row['AVG_DAILY_SALES'] > 0:
            return row['CURRENT_STOCK'] / row['AVG_DAILY_SALES']
        else:
            return 9999  # Use 9999 for infinite autonomy (stock but no sales)
    
    result_df['STOCK_AUTONOMY_DAYS'] = result_df.apply(calculate_autonomy, axis=1)
    
    # Add site names if available
    if 'sites' in dataframes and dataframes['sites'] is not None:
        site_names = dataframes['sites'][['ID', 'SITE']].drop_duplicates()
        site_names = site_names.rename(columns={'ID': 'SITE', 'SITE': 'SITE_NAME'})
        result_df = result_df.merge(site_names, on='SITE', how='left')
    
    # Add item names if available
    if 'inventory_items' in dataframes and dataframes['inventory_items'] is not None:
        item_names = dataframes['inventory_items'][['ITEM', 'DESCR1']].drop_duplicates()
        item_names['ITEM_NAME'] = item_names['DESCR1'].fillna('').astype(str)
        item_names = item_names[['ITEM', 'ITEM_NAME']]
        result_df = result_df.merge(item_names, on='ITEM', how='left')
        result_df['ITEM_NAME'] = result_df['ITEM_NAME'].fillna('')
    
    return result_df

# Routes
@app.route('/favicon.ico')
def favicon():
    return '', 204  # No content

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/autonomy')
def autonomy():
    return render_template('autonomy.html')

@app.route('/api/load-dataframes', methods=['POST'])
def api_load_dataframes():
    global cache_loading
    try:
        print(f"📡 API load-dataframes called. Current cache_loading: {cache_loading}")
        
        with cache_lock:
            if cache_loading:
                print("⏳ Cache loading already in progress")
                return jsonify({'status': 'loading', 'message': 'Cache loading already in progress'})
            cache_loading = True
            print("🔒 Cache loading lock acquired")
        
        print("🚀 Starting dataframes loading...")
        result = load_dataframes()
        
        if result and len(result) > 0:
            print(f"✅ Successfully loaded {len(result)} dataframes")
            return jsonify({
                'status': 'success', 
                'message': f'Successfully loaded {len(result)} tables',
                'tables': list(result.keys())
            })
        else:
            print("❌ No dataframes were loaded")
            return jsonify({'status': 'error', 'message': 'No tables were loaded successfully'}), 500
            
    except Exception as e:
        cache_loading = False
        error_msg = str(e)
        print(f"❌ Error in api_load_dataframes: {error_msg}")
        return jsonify({'status': 'error', 'message': error_msg}), 500

@app.route('/api/cache-status')
def api_cache_status():
    return jsonify({
        'cached': len(dataframes) > 0,
        'loading': cache_loading,
        'tables': list(dataframes.keys()) if dataframes else []
    })

@app.route('/api/sites')
def api_sites():
    try:
        if 'sites' not in dataframes or dataframes['sites'] is None:
            return jsonify([])
        
        sites = dataframes['sites'][['ID', 'SITE']].drop_duplicates()
        sites_list = [{'id': row['ID'], 'name': row['SITE']} for _, row in sites.iterrows()]
        return jsonify(sites_list)
    except Exception as e:
        print(f"Error in api_sites: {e}")
        return jsonify([])

@app.route('/api/items')
def api_items():
    try:
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify([])
        
        items = dataframes['inventory_items'][['ITEM', 'DESCR1']].drop_duplicates()
        items_list = [{'code': row['ITEM'], 'name': row['DESCR1'] or ''} for _, row in items.iterrows()]
        return jsonify(items_list)
    except Exception as e:
        print(f"Error in api_items: {e}")
        return jsonify([])

@app.route('/api/autonomy-report', methods=['POST'])
def api_autonomy_report():
    try:
        data = request.get_json()
        item_code = data.get('item_code')
        site_code = data.get('site_code') 
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        result_df = calculate_stock_and_sales(item_code, site_code, from_date, to_date)
        
        if result_df is None or result_df.empty:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Convert to JSON
        result_json = result_df.to_dict('records')
        return jsonify(result_json)
        
    except Exception as e:
        print(f"Error in autonomy report: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/export-excel', methods=['POST'])
def api_export_excel():
    try:
        data = request.get_json()
        report_data = data.get('data', [])
        
        if not report_data:
            return jsonify({'error': 'No data to export'}), 400
        
        df = pd.DataFrame(report_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Autonomy Report', index=False)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'autonomy_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 Starting DustReports Flask Application")
    print("📊 Using notebook-style data loading approach")
    app.run(host='0.0.0.0', port=5000, debug=True)
