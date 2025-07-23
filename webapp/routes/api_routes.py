"""API routes for data operations"""

from flask import Blueprint, request, jsonify
import pandas as pd
from services.database_service import (
    load_dataframes, get_dataframes, is_cache_loading, get_cache_lock,
    get_cache_timestamp, get_cache_age_seconds
)
from models.stock_analysis import StockAnalyzer

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/load-dataframes', methods=['POST'])
def api_load_dataframes():
    """Load dataframes from database"""
    try:
        print(f"📡 API load-dataframes called. Current cache_loading: {is_cache_loading()}")
        
        with get_cache_lock():
            if is_cache_loading():
                print("⏳ Cache loading already in progress")
                return jsonify({'status': 'loading', 'message': 'Cache loading already in progress'})
        
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
        error_msg = str(e)
        print(f"❌ Error in api_load_dataframes: {error_msg}")
        return jsonify({'status': 'error', 'message': error_msg}), 500

@api_bp.route('/cache-status')
def api_cache_status():
    """Get cache status"""
    dataframes = get_dataframes()
    cache_age = get_cache_age_seconds()
    cache_timestamp = get_cache_timestamp()
    
    response = {
        'cached': len(dataframes) > 0,
        'loading': is_cache_loading(),
        'tables': list(dataframes.keys()) if dataframes else [],
        'table_count': len(dataframes),
        'cache_age_seconds': cache_age if cache_age is not None else 0,
        'cache_timestamp': cache_timestamp.isoformat() if cache_timestamp else None
    }
    
    return jsonify(response)

@api_bp.route('/auto-refresh-cache', methods=['POST'])
def api_auto_refresh_cache():
    """Auto-refresh cache if older than specified threshold"""
    try:
        data = request.get_json() or {}
        max_age_hours = data.get('max_age_hours', 1)  # Default 1 hour
        
        cache_age = get_cache_age_seconds()
        max_age_seconds = max_age_hours * 3600  # Convert hours to seconds
        
        if cache_age is None or cache_age > max_age_seconds:
            print(f"🔄 Auto-refresh triggered: cache age {cache_age}s > {max_age_seconds}s threshold")
            
            with get_cache_lock():
                if is_cache_loading():
                    return jsonify({'status': 'loading', 'message': 'Cache refresh already in progress'})
            
            result = load_dataframes()
            
            if result and len(result) > 0:
                return jsonify({
                    'status': 'refreshed',
                    'message': f'Cache auto-refreshed successfully',
                    'tables': list(result.keys()),
                    'trigger_reason': f'Cache was {cache_age/3600:.1f} hours old'
                })
            else:
                return jsonify({'status': 'error', 'message': 'Auto-refresh failed'}), 500
        else:
            return jsonify({
                'status': 'not_needed',
                'message': f'Cache is fresh (age: {cache_age/3600:.1f} hours)',
                'cache_age_hours': cache_age/3600
            })
            
    except Exception as e:
        print(f"❌ Error in auto-refresh: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@api_bp.route('/sites')
def api_sites():
    """Get list of sites"""
    try:
        dataframes = get_dataframes()
        if 'sites' not in dataframes or dataframes['sites'] is None:
            return jsonify([])
        
        sites = dataframes['sites'][['ID', 'SITE']].drop_duplicates()
        sites_list = [{'id': row['ID'], 'name': row['SITE']} for _, row in sites.iterrows()]
        return jsonify(sites_list)
    except Exception as e:
        print(f"Error in api_sites: {e}")
        return jsonify([])

@api_bp.route('/categories')
def api_categories():
    """Get list of categories"""
    try:
        dataframes = get_dataframes()
        if 'categories' not in dataframes or dataframes['categories'] is None:
            print("⚠️ Categories dataframe not available")
            return jsonify([])
        
        # Filter out categories with 0 items
        if 'inventory_items' in dataframes and dataframes['inventory_items'] is not None:
            # Convert category IDs to string for consistent comparison
            categories_with_items = dataframes['inventory_items']['CATEGORY'].dropna().astype(str).unique()
            
            categories = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            # Convert category IDs to string for matching
            categories['ID'] = categories['ID'].astype(str)
            categories = categories[categories['ID'].isin(categories_with_items)]
            
            categories_list = [{'id': row['ID'], 'name': row['DESCR'] or ''} for _, row in categories.iterrows()]
            categories_list.sort(key=lambda x: x['name'])
            
            print(f"📋 Returning {len(categories_list)} categories with items")
            return jsonify(categories_list)
        else:
            print("⚠️ Inventory items dataframe not available - returning all categories")
            categories = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            categories_list = [{'id': row['ID'], 'name': row['DESCR'] or ''} for _, row in categories.iterrows()]
            categories_list.sort(key=lambda x: x['name'])
            return jsonify(categories_list)
    except Exception as e:
        print(f"❌ Error in api_categories: {e}")
        import traceback
        traceback.print_exc()
        return jsonify([])

@api_bp.route('/items')
def api_items():
    """Get list of items"""
    try:
        dataframes = get_dataframes()
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify([])
        
        items = dataframes['inventory_items'][['ITEM', 'DESCR1']].drop_duplicates()
        items_list = [{'code': row['ITEM'], 'name': row['DESCR1'] or ''} for _, row in items.iterrows()]
        return jsonify(items_list)
    except Exception as e:
        print(f"Error in api_items: {e}")
        return jsonify([])

@api_bp.route('/autonomy-report', methods=['POST'])
def api_autonomy_report():
    """Generate autonomy report"""
    try:
        data = request.get_json()
        item_code = data.get('item_code')
        site_code = data.get('site_code') 
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        analyzer = StockAnalyzer()
        result_df = analyzer.calculate_stock_and_sales(
            item_code=item_code, 
            site_code=site_code, 
            from_date=from_date, 
            to_date=to_date
        )
        
        if result_df is None or result_df.empty:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Calculate period days
        if from_date and to_date:
            from_dt = pd.to_datetime(from_date)
            to_dt = pd.to_datetime(to_date)
            period_days = (to_dt - from_dt).days + 1
        else:
            period_days = result_df['SALES_PERIOD_DAYS'].iloc[0] if not result_df.empty and 'SALES_PERIOD_DAYS' in result_df.columns else 0
        
        # Convert to JSON
        result_json = result_df.to_dict('records')
        
        return jsonify({
            'data': result_json,
            'metadata': {
                'period_days': int(period_days),
                'from_date': from_date,
                'to_date': to_date,
                'total_rows': len(result_df)
            }
        })
        
    except Exception as e:
        print(f"Error in autonomy report: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/stock-by-site-report', methods=['POST'])
def api_stock_by_site_report():
    """Generate stock by site report"""
    try:
        data = request.get_json()
        item_code = data.get('item_code')
        site_code = data.get('site_code')
        site_codes = data.get('site_codes')
        category_id = data.get('category_id')
        as_of_date = data.get('as_of_date')
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        analyzer = StockAnalyzer()
        result_df = analyzer.calculate_stock_and_sales(
            item_code=item_code, 
            site_code=site_code,
            site_codes=site_codes,
            category_id=category_id,
            as_of_date=as_of_date
        )
        
        if result_df is None or result_df.empty:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Sort results
        if site_codes and isinstance(site_codes, list) and len(site_codes) > 1:
            result_df = result_df.sort_values(['ITEM_NAME', 'CURRENT_STOCK'], ascending=[True, False])
        else:
            result_df = result_df.sort_values(['SITE_NAME', 'CURRENT_STOCK'], ascending=[True, False])
        
        # Convert to JSON
        result_json = result_df.to_dict('records')
        
        return jsonify({
            'data': result_json,
            'metadata': {
                'total_rows': len(result_df),
                'filters': {
                    'site_code': site_code,
                    'site_codes': site_codes,
                    'item_code': item_code,
                    'category_id': category_id,
                    'as_of_date': as_of_date
                }
            }
        })
        
    except Exception as e:
        print(f"Error in stock by site report: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/get-available-columns')
def api_get_available_columns():
    """Get all available columns from all loaded dataframes"""
    try:
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded'}), 400
        
        all_columns = {}
        for table_name, df in dataframes.items():
            if df is not None and not df.empty:
                all_columns[table_name] = {
                    'columns': df.columns.tolist(),
                    'row_count': len(df)
                }
        
        return jsonify(all_columns)
        
    except Exception as e:
        print(f"Error in get_available_columns: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/custom-report', methods=['POST'])
def api_custom_report():
    """Generate custom report based on user parameters"""
    try:
        data = request.get_json()
        table_name = data.get('table_name')
        columns = data.get('columns', [])
        filters = data.get('filters', {})
        limit = data.get('limit', 1000)
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        if table_name not in dataframes:
            return jsonify({'error': f'Table {table_name} not found'}), 404
        
        df = dataframes[table_name].copy()
        
        # Apply filters
        for column, value in filters.items():
            if column in df.columns and value:
                if isinstance(value, str):
                    df = df[df[column].astype(str).str.contains(value, case=False, na=False)]
                else:
                    df = df[df[column] == value]
        
        # Select columns
        if columns:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]
        
        # Apply limit
        if len(df) > limit:
            df = df.head(limit)
        
        # Convert to JSON
        result_json = df.to_dict('records')
        
        return jsonify({
            'data': result_json,
            'metadata': {
                'total_rows': len(df),
                'columns': df.columns.tolist(),
                'table_name': table_name
            }
        })
        
    except Exception as e:
        print(f"Error in custom report: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/ciment-report', methods=['POST'])
def api_ciment_report():
    """
    Generate Ciment Report showing site-wise ciment category sales and stock
    """
    try:
        data = request.get_json()
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get all non-depot sites (exclude depot sites where SIDNO = '3700004')
        if 'sites' not in dataframes or dataframes['sites'] is None:
            return jsonify({'error': 'Sites data not available'}), 400
        
        # Get Kinshasa sites only (SIDNO = '3700002')
        kinshasa_sites = dataframes['sites'][dataframes['sites']['SIDNO'] == '3700002'].copy()
        
        if kinshasa_sites.empty:
            return jsonify({'error': 'No Kinshasa sites found (SIDNO = 3700002)'}), 404
            
        print(f"📍 Found {len(kinshasa_sites)} Kinshasa sites for ciment report")
        
        # Get ciment category items
        if 'categories' not in dataframes or dataframes['categories'] is None:
            return jsonify({'error': 'Categories data not available'}), 400
        
        # Find ciment category (look for category containing "ciment" in name)
        ciment_categories = dataframes['categories'][
            dataframes['categories']['DESCR'].str.contains('ciment', case=False, na=False)
        ]
        
        if ciment_categories.empty:
            # Try alternative names
            ciment_categories = dataframes['categories'][
                dataframes['categories']['DESCR'].str.contains('cement', case=False, na=False)
            ]
        
        if ciment_categories.empty:
            return jsonify({'error': 'Ciment category not found. Please check category names.'}), 404
        
        ciment_category_id = ciment_categories.iloc[0]['ID']
        print(f"📋 Found ciment category: {ciment_categories.iloc[0]['DESCR']} (ID: {ciment_category_id})")
        
        # Get items in ciment category
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify({'error': 'Inventory items data not available'}), 400
        
        # Get all ciment items from the ciment category
        ciment_items = dataframes['inventory_items'][
            dataframes['inventory_items']['CATEGORY'].astype(str) == str(ciment_category_id)
        ]['ITEM'].unique()
        
        if len(ciment_items) == 0:
            return jsonify({'error': 'No items found in ciment category'}), 404
        
        print(f"📦 Found {len(ciment_items)} items in ciment category")
        
        # Pre-filter sales data to find items with actual sales
        if 'sales_details' in dataframes and dataframes['sales_details'] is not None:
            all_sales = dataframes['sales_details'].copy()
            
            # Filter by date range if specified
            if from_date or to_date:
                all_sales['FDATE'] = pd.to_datetime(all_sales['FDATE'], errors='coerce')
                if from_date:
                    from_date_dt = pd.to_datetime(from_date)
                    all_sales = all_sales[all_sales['FDATE'] >= from_date_dt]
                if to_date:
                    to_date_dt = pd.to_datetime(to_date)
                    all_sales = all_sales[all_sales['FDATE'] <= to_date_dt]
            
            # Filter for sales transactions only (FTYPE = 1 for sales)
            if 'FTYPE' in all_sales.columns:
                all_sales = all_sales[all_sales['FTYPE'] == 1]
            
            # Find ciment items that have sales in the period
            ciment_items_with_sales = all_sales[
                all_sales['ITEM'].isin(ciment_items)
            ]['ITEM'].unique()
            
            # Use only ciment items that have sales
            active_ciment_items = ciment_items_with_sales
            
            print(f"📊 Pre-filtered sales data: {len(all_sales)} transactions")
            print(f"📦 Active ciment items (with sales): {len(active_ciment_items)} items shown")
        else:
            all_sales = pd.DataFrame()
            active_ciment_items = ciment_items
            print("⚠️ No sales data available - showing all ciment items")
        
        # Get item prices for amount calculations
        item_prices = {}
        if 'inventory_items' in dataframes and dataframes['inventory_items'] is not None:
            price_data = dataframes['inventory_items'][
                dataframes['inventory_items']['ITEM'].isin(active_ciment_items)
            ][['ITEM', 'POSPRICE1']].drop_duplicates()
            item_prices = dict(zip(price_data['ITEM'], price_data['POSPRICE1'].fillna(0)))
            print(f"📊 Retrieved prices for {len(item_prices)} ciment items")
        
        # Calculate sales for each site (no stock data needed)
        result_data = []
        
        for i, (_, site) in enumerate(kinshasa_sites.iterrows()):
            if i % 10 == 0:  # Progress logging every 10 sites
                print(f"📍 Processing site {i+1}/{len(kinshasa_sites)}: {site['SITE']}")
            
            site_id = site['ID']
            site_name = site['SITE']
            
            # Initialize row data with Python native types (sales only)
            row_data = {
                'SITE_ID': str(site_id),
                'SITE_NAME': str(site_name),
                'TOTAL_AMOUNT': 0.0,  # Total sales amount (qty × price)
                'CIMENT_ARTICLES_WITH_SALES': 0,  # Count of ciment items with sales
                'TOTAL_CIMENT_SALES_QTY': 0.0  # Total ciment sales quantity
            }
            
            # Calculate sales for this site
            if not all_sales.empty:
                site_sales = all_sales[all_sales['SITE'] == site_id]
                
                # Calculate ciment sales by item with amounts
                ciment_sales_by_item = {}
                ciment_amounts_by_item = {}
                total_ciment_sales = 0.0
                total_amount = 0.0
                articles_with_sales = 0
                
                for item_code in active_ciment_items:
                    item_sales = site_sales[site_sales['ITEM'] == item_code]
                    item_sales_qty = float(item_sales['QTY'].fillna(0).sum())
                    
                    # Calculate amount (qty × price)
                    item_price = item_prices.get(item_code, 0)
                    item_amount = item_sales_qty * item_price
                    
                    if item_sales_qty > 0:
                        articles_with_sales += 1
                    
                    ciment_sales_by_item[str(item_code)] = item_sales_qty
                    ciment_amounts_by_item[str(item_code)] = item_amount
                    total_ciment_sales += item_sales_qty
                    total_amount += item_amount
                
                row_data['TOTAL_AMOUNT'] = total_amount
                row_data['CIMENT_ARTICLES_WITH_SALES'] = articles_with_sales
                row_data['TOTAL_CIMENT_SALES_QTY'] = total_ciment_sales
                row_data['CIMENT_SALES_BY_ITEM'] = ciment_sales_by_item
                row_data['CIMENT_AMOUNTS_BY_ITEM'] = ciment_amounts_by_item
            else:
                row_data['CIMENT_SALES_BY_ITEM'] = {str(item): 0.0 for item in active_ciment_items}
                row_data['CIMENT_AMOUNTS_BY_ITEM'] = {str(item): 0.0 for item in active_ciment_items}
            
            result_data.append(row_data)
        
        # Filter out sites with zero total sales amount
        result_data = [site_data for site_data in result_data if site_data['TOTAL_AMOUNT'] > 0]
        print(f"📊 Filtered out sites with zero sales: {len(result_data)} sites remaining")
        
        # Sort by total amount descending (highest sales first)
        result_data.sort(key=lambda x: x['TOTAL_AMOUNT'], reverse=True)
        
        # Filter out ciment items with 0 overall sales across all sites
        items_with_sales = set()
        for site_data in result_data:
            for item_code, qty in site_data.get('CIMENT_SALES_BY_ITEM', {}).items():
                if qty > 0:
                    items_with_sales.add(item_code)
        
        # Update active_ciment_items to only include items with sales
        active_ciment_items = [item for item in active_ciment_items if str(item) in items_with_sales]
        
        # Remove filtered items from each site's data
        for site_data in result_data:
            if 'CIMENT_SALES_BY_ITEM' in site_data:
                site_data['CIMENT_SALES_BY_ITEM'] = {
                    item_code: qty for item_code, qty in site_data['CIMENT_SALES_BY_ITEM'].items()
                    if item_code in items_with_sales
                }
            if 'CIMENT_AMOUNTS_BY_ITEM' in site_data:
                site_data['CIMENT_AMOUNTS_BY_ITEM'] = {
                    item_code: amount for item_code, amount in site_data['CIMENT_AMOUNTS_BY_ITEM'].items()
                    if item_code in items_with_sales
                }
        
        print(f"📊 Filtered to {len(active_ciment_items)} ciment items with actual sales")
        
        # Get ciment item details for column headers (only items with sales)
        ciment_item_details = dataframes['inventory_items'][
            dataframes['inventory_items']['ITEM'].isin(active_ciment_items)
        ][['ITEM', 'DESCR1']].drop_duplicates()
        
        # Convert to JSON-serializable format
        ciment_item_details = [
            {
                'ITEM': str(row['ITEM']), 
                'DESCR1': str(row['DESCR1']) if pd.notna(row['DESCR1']) else ''
            } 
            for _, row in ciment_item_details.iterrows()
        ]
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'total_sites': len(result_data),
                'ciment_category_id': int(ciment_category_id),  # Convert to Python int
                'ciment_category_name': str(ciment_categories.iloc[0]['DESCR']),  # Convert to Python string
                'ciment_items': ciment_item_details,
                'total_ciment_items_in_category': len(ciment_items),
                'active_ciment_items_shown': len(active_ciment_items),
                'from_date': from_date,
                'to_date': to_date,
                'sales_only_mode': True,  # Indicate this is sales-only report
                'columns_info': {
                    'site': 'Site name',
                    'total_amount': 'Total amount (sales × price)',
                    'ciment_articles_with_sales': 'Number of ciment articles with sales',
                    'total_ciment_sales_qty': 'Total ciment sales quantity'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in ciment report: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/sales-report', methods=['POST'])
def api_sales_report():
    """
    Generate Sales Report using INVOICE table with NET, DISCOUNT, and cumulative calculations
    Filter for SID starting with "530" and FTYPE = 1 (valid sales transactions)
    """
    try:
        data = request.get_json()
        site_type = data.get('site_type')  # 'int' or 'kinshasa'
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        report_date = data.get('report_date')  # New parameter for single date
        
        # If report_date is provided, use it; otherwise, use from_date or to_date
        if report_date:
            selected_date = report_date
        elif from_date and to_date:
            # If both dates are provided and they're the same, use that date
            if from_date == to_date:
                selected_date = from_date
            else:
                # If different dates, default to from_date for backward compatibility
                selected_date = from_date
        elif from_date:
            selected_date = from_date
        elif to_date:
            selected_date = to_date
        else:
            from datetime import datetime
            today = datetime.now()
            selected_date = today.strftime('%Y-%m-%d')
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get invoice data (original table name: INVOICE)
        if 'invoice_headers' not in dataframes or dataframes['invoice_headers'] is None:
            return jsonify({'error': 'Invoice data not available'}), 400
        
        invoice_df = dataframes['invoice_headers'].copy()
        print(f"📊 Working with {len(invoice_df)} invoice records")
        
        # Filter for valid sales transactions (FTYPE = 1)
        if 'FTYPE' in invoice_df.columns:
            invoice_df = invoice_df[invoice_df['FTYPE'] == 1]
            print(f"📊 After FTYPE=1 filter: {len(invoice_df)} records")
        
        # Filter for SID starting with "530"
        if 'SID' in invoice_df.columns:
            invoice_df = invoice_df[invoice_df['SID'].astype(str).str.startswith('530')]
            print(f"📊 After SID starts with '530' filter: {len(invoice_df)} records")
        else:
            return jsonify({'error': 'SID column not found in invoice data'}), 400
        
        # Ensure required columns exist
        required_cols = ['SID', 'NET', 'SUBTOTAL', 'VAT', 'OTHER', 'FDATE']
        missing_cols = [col for col in required_cols if col not in invoice_df.columns]
        if missing_cols:
            return jsonify({'error': f'Missing required columns: {missing_cols}'}), 400
        
        # Convert date column and filter by selected date
        invoice_df['FDATE'] = pd.to_datetime(invoice_df['FDATE'], errors='coerce')
        selected_date_dt = pd.to_datetime(selected_date)
        
        # Filter for the specific selected date
        invoice_df = invoice_df[invoice_df['FDATE'].dt.date == selected_date_dt.date()]
        
        print(f"📊 After date filter (selected date: {selected_date}): {len(invoice_df)} records")
        
        # Fill NaN values for calculations
        invoice_df['NET'] = invoice_df['NET'].fillna(0)
        invoice_df['SUBTOTAL'] = invoice_df['SUBTOTAL'].fillna(0)
        invoice_df['VAT'] = invoice_df['VAT'].fillna(0)
        invoice_df['OTHER'] = invoice_df['OTHER'].fillna(0)
        
        # Calculate discount using the OTHER field from INVOICE table
        invoice_df['DISCOUNT_CALC'] = invoice_df['OTHER']
        
        # Filter SIDs based on site type (ID pattern based)
        if site_type == 'kinshasa':
            # Filter for SIDs starting with "5301" (Kinshasa sites)
            invoice_df = invoice_df[invoice_df['SID'].astype(str).str.startswith('5301')]
            site_type_name = 'Kinshasa'
        elif site_type == 'int':
            # Filter for SIDs starting with "5302" (INT sites)  
            invoice_df = invoice_df[invoice_df['SID'].astype(str).str.startswith('5302')]
            site_type_name = 'INT'
        else:
            return jsonify({'error': 'Invalid site_type. Must be "kinshasa" or "int"'}), 400
        
        if invoice_df.empty:
            return jsonify({'error': f'No {site_type_name} sales found (SID starting with 530{"1" if site_type == "kinshasa" else "2"})'}), 404
        
        print(f"📍 Found {len(invoice_df)} {site_type_name} sales records")
        
        # Calculate sales for the selected date (sum NET by SID)
        period_sales = invoice_df.groupby('SID').agg({
            'NET': 'sum',
            'DISCOUNT_CALC': 'sum'
        }).reset_index()
        period_sales.columns = ['SID', 'SALES', 'DISCOUNT']
        
        # Calculate cumulative sales from INVOICE table (from 1st of month until selected_date)
        # Calculate from 1st of the month until selected_date
        first_of_month = selected_date_dt.replace(day=1)
        
        print(f"📊 Calculating cumulative sales from {first_of_month.strftime('%Y-%m-%d')} to {selected_date}")
        
        # Get fresh copy of invoice data for cumulative calculation
        cumulative_invoice_df = dataframes['invoice_headers'].copy()
        
        # Apply same base filters
        if 'FTYPE' in cumulative_invoice_df.columns:
            cumulative_invoice_df = cumulative_invoice_df[cumulative_invoice_df['FTYPE'] == 1]
        
        if 'SID' in cumulative_invoice_df.columns:
            cumulative_invoice_df = cumulative_invoice_df[cumulative_invoice_df['SID'].astype(str).str.startswith('530')]
            
            # Apply site type filter
            if site_type == 'kinshasa':
                cumulative_invoice_df = cumulative_invoice_df[cumulative_invoice_df['SID'].astype(str).str.startswith('5301')]
            elif site_type == 'int':
                cumulative_invoice_df = cumulative_invoice_df[cumulative_invoice_df['SID'].astype(str).str.startswith('5302')]
        
        # Filter by cumulative date range (1st of month to selected_date)
        cumulative_invoice_df['FDATE'] = pd.to_datetime(cumulative_invoice_df['FDATE'], errors='coerce')
        cumulative_invoice_df = cumulative_invoice_df[
            (cumulative_invoice_df['FDATE'] >= first_of_month) & 
            (cumulative_invoice_df['FDATE'] <= selected_date_dt)
        ]
        
        # Fill NaN values and calculate cumulative sales (sum NET by SID)
        cumulative_invoice_df['NET'] = cumulative_invoice_df['NET'].fillna(0)
        
        # Calculate cumulative sales by SID
        cumulative_sales = cumulative_invoice_df.groupby('SID')['NET'].sum().reset_index()
        cumulative_sales.columns = ['SID', 'CUMULATIVE_SALES']
        
        print(f"📊 Calculated cumulative sales for {len(cumulative_sales)} SIDs from INVOICE table (1st of month to selected date)")
        
        # Merge period and cumulative sales
        result_df = period_sales.merge(cumulative_sales, on='SID', how='left')
        result_df['CUMULATIVE_SALES'] = result_df['CUMULATIVE_SALES'].fillna(result_df['SALES'])
        
        # Add site information using SUB table for site names
        if 'accounts' in dataframes and dataframes['accounts'] is not None:
            # Get site names from SUB table (SQL: SELECT s.sname FROM SUB s WHERE s.sid = :site_id)
            sub_df = dataframes['accounts'].copy()
            
            # Ensure SID columns are strings for proper matching
            sub_df['SID'] = sub_df['SID'].astype(str) 
            result_df['SID'] = result_df['SID'].astype(str)
            
            # Merge to get site names (SNAME from SUB table)
            if 'SNAME' in sub_df.columns:
                site_names = sub_df[['SID', 'SNAME']].drop_duplicates()
                result_df = result_df.merge(site_names, on='SID', how='left')
                
                # Fill missing site names with SID as fallback
                result_df['SNAME'] = result_df['SNAME'].fillna(result_df['SID'])
                print(f"📍 Retrieved site names for {len(site_names)} sites from SUB table")
            else:
                print("⚠️ SNAME column not found in SUB table, using SID as site name")
                result_df['SNAME'] = result_df['SID']
        else:
            print("⚠️ SUB table (accounts) not available, using SID as site name")
            result_df['SNAME'] = result_df['SID']
        
        # Sort by sales amount descending
        result_df = result_df.sort_values('SALES', ascending=False)
        
        # Convert to list of dictionaries for JSON response
        result_data = []
        for _, row in result_df.iterrows():
            # Use site name from SUB table if available, otherwise use SID
            site_name = str(row['SNAME']) if pd.notna(row['SNAME']) and str(row['SNAME']).strip() else f"Site {row['SID']}"
            
            row_data = {
                'SITE_ID': str(row['SID']),
                'SITE_NAME': site_name,
                'SALES_AMOUNT': float(row['SALES']),
                'DISCOUNT_AMOUNT': float(row['DISCOUNT']),
                'CUMULATIVE_SALES': float(row['CUMULATIVE_SALES'])
            }
            result_data.append(row_data)
        
        total_sales = result_df['SALES'].sum()
        total_discount = result_df['DISCOUNT'].sum()
        total_cumulative = result_df['CUMULATIVE_SALES'].sum()
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'total_sites': len(result_data),
                'site_type': site_type,
                'site_type_name': site_type_name,
                'total_sales_amount': float(total_sales),
                'total_discount_amount': float(total_discount),
                'total_cumulative_sales': float(total_cumulative),
                'selected_date': selected_date,
                'data_source': 'INVOICE table (NET for both sales and cumulative)',
                'calculation_method': {
                    'sales': 'NET from INVOICE table for specific selected date',
                    'discount': 'OTHER field from INVOICE table',
                    'cumulative': 'NET from INVOICE table from 1st of month until selected date'
                },
                'columns_info': {
                    'site_name': 'Site identifier (SID)',
                    'sales_amount': 'Net sales amount for selected date (INVOICE.NET)',
                    'discount_amount': 'Discount amount (INVOICE.OTHER)',
                    'cumulative_sales': 'Cumulative sales from 1st of month to selected date (INVOICE.NET)'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in sales report: {e}")
        return jsonify({'error': str(e)}), 500

@api_bp.route('/sales-by-item-report', methods=['POST'])
def api_sales_by_item_report():
    """
    Generate Sales by Item Report using original SQL query logic
    Replicates: SUM(CREDITUS-DEBITUS) + SUM(creditvatamount-debitvatamount) as total sales
    Joins ITEMS with INVOICE on MID=ID for discount calculation
    """
    try:
        data = request.get_json()
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        site_type = data.get('site_type')  # Optional: 'int', 'kinshasa', or null for all
        
        # Default to today if no dates provided
        if not from_date and not to_date:
            from datetime import datetime
            today = datetime.now()
            from_date = to_date = today.strftime('%Y-%m-%d')
        elif not to_date:
            to_date = from_date
        elif not from_date:
            from_date = to_date
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get sales details data for item-level transactions
        if 'sales_details' not in dataframes or dataframes['sales_details'] is None:
            return jsonify({'error': 'Sales details data not available'}), 400
        
        # Get invoice headers for joining and discount calculation
        if 'invoice_headers' not in dataframes or dataframes['invoice_headers'] is None:
            return jsonify({'error': 'Invoice headers data not available'}), 400
        
        sales_df = dataframes['sales_details'].copy()
        invoice_df = dataframes['invoice_headers'].copy()
        
        print(f"📊 Working with {len(sales_df)} sales detail records and {len(invoice_df)} invoice records")
        
        # Check for required fields from original query
        required_sales_fields = ['ITEM', 'FDATE', 'FTYPE', 'SID', 'MID']
        missing_sales_fields = [field for field in required_sales_fields if field not in sales_df.columns]
        
        required_invoice_fields = ['ID', 'OTHER', 'SUBTOTAL']
        missing_invoice_fields = [field for field in required_invoice_fields if field not in invoice_df.columns]
        
        if missing_sales_fields:
            print(f"⚠️ Missing sales fields: {missing_sales_fields}")
        if missing_invoice_fields:
            print(f"⚠️ Missing invoice fields: {missing_invoice_fields}")
        
        # Check for original query calculation fields
        original_calc_fields = ['CREDITQTY', 'DEBITQTY', 'CREDITUS', 'DEBITUS', 'creditvatamount', 'debitvatamount', 'discount']
        available_calc_fields = [field for field in original_calc_fields if field in sales_df.columns]
        missing_calc_fields = [field for field in original_calc_fields if field not in sales_df.columns]
        
        print(f"📊 Available calculation fields: {available_calc_fields}")
        if missing_calc_fields:
            print(f"⚠️ Missing calculation fields: {missing_calc_fields}")
        
        # Filter for sales transactions (FTYPE = 1 or 2) - from original query
        if 'FTYPE' in sales_df.columns:
            sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]
            print(f"📊 After FTYPE=1,2 filter: {len(sales_df)} records")
        
        # Filter for site sales only (SID starting with "530")
        if 'SID' in sales_df.columns:
            sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('530')]
            print(f"📊 After SID filter: {len(sales_df)} records")
        
        # Convert date column and filter by date range (FDATE BETWEEN from_date AND to_date)
        if 'FDATE' in sales_df.columns:
            sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
            
            if from_date:
                from_date_dt = pd.to_datetime(from_date)
                sales_df = sales_df[sales_df['FDATE'] >= from_date_dt]
            
            if to_date:
                to_date_dt = pd.to_datetime(to_date)
                sales_df = sales_df[sales_df['FDATE'] <= to_date_dt]
                
            print(f"📊 After date filter ({from_date} to {to_date}): {len(sales_df)} records")
        
        # Filter by site type if specified
        if site_type:
            if site_type == 'kinshasa':
                sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('5301')]
                site_type_name = 'Kinshasa'
            elif site_type == 'int':
                sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('5302')]
                site_type_name = 'INT'
            else:
                return jsonify({'error': 'Invalid site_type. Must be "kinshasa" or "int"'}), 400
            print(f"📍 After site type filter ({site_type_name}): {len(sales_df)} records")
        else:
            site_type_name = 'All Sites'
        
        if sales_df.empty:
            return jsonify({'error': 'No sales found for the specified criteria'}), 404
        
        # Prepare invoice data for joining (ITEMS.MID = INVOICE.ID)
        invoice_df = invoice_df[invoice_df['SUBTOTAL'] != 0]  # invoice.subtotal<>0 from original query
        
        # Join sales with invoice data if MID and ID fields are available
        if 'MID' in sales_df.columns and 'ID' in invoice_df.columns:
            print("📊 Joining ITEMS with INVOICE on MID=ID")
            # Perform the join (ITEMS.MID = INVOICE.ID)
            sales_with_invoice = sales_df.merge(
                invoice_df[['ID', 'OTHER', 'SUBTOTAL']], 
                left_on='MID', 
                right_on='ID', 
                how='inner'
            )
            print(f"📊 After join: {len(sales_with_invoice)} records")
        else:
            print("⚠️ Cannot join ITEMS with INVOICE - using sales data only")
            sales_with_invoice = sales_df.copy()
            # Add placeholder columns for calculations
            sales_with_invoice['OTHER'] = 0
            sales_with_invoice['SUBTOTAL'] = 1  # Avoid division by zero
        
        # Fill NaN values for calculations
        numeric_fields = ['CREDITQTY', 'DEBITQTY', 'CREDITUS', 'DEBITUS', 'creditvatamount', 'debitvatamount', 'QTY']
        for field in numeric_fields:
            if field in sales_with_invoice.columns:
                sales_with_invoice[field] = sales_with_invoice[field].fillna(0)
        
        # Calculate using original query logic
        calculation_results = []
        
        # Group by ITEM and discount (from original query: GROUP BY ITEMS.ITEM,ITEMS.discount)
        if 'discount' in sales_with_invoice.columns:
            groupby_cols = ['ITEM', 'discount']
        else:
            groupby_cols = ['ITEM']
            sales_with_invoice['discount'] = 0  # Default if field doesn't exist
        
        for group_name, group_data in sales_with_invoice.groupby(groupby_cols):
            if isinstance(group_name, tuple):
                if len(group_name) == 2:
                    item_code, item_discount = group_name
                else:
                    # Single element tuple case
                    item_code = group_name[0]
                    item_discount = 0
            else:
                item_code = group_name
                item_discount = 0
            
            # Calculate using original query formulas
            # SUM(ITEMS.CREDITQTY-ITEMS.DEBITQTY) AS SALES
            if 'CREDITQTY' in group_data.columns and 'DEBITQTY' in group_data.columns:
                sales_qty = (group_data['CREDITQTY'] - group_data['DEBITQTY']).sum()
            else:
                # Fallback to QTY if original fields not available
                sales_qty = group_data['QTY'].sum() if 'QTY' in group_data.columns else 0
            
            # SUM(ITEMS.CREDITUS-ITEMS.DEBITUS) AS TOTAL
            if 'CREDITUS' in group_data.columns and 'DEBITUS' in group_data.columns:
                total_amount = (group_data['CREDITUS'] - group_data['DEBITUS']).sum()
            else:
                # Fallback calculation if original fields not available
                total_amount = 0
                print(f"⚠️ CREDITUS/DEBITUS fields not available for item {item_code}")
            
            # Calculate VAT amount from inventory items VAT rate 
            # We'll calculate this after merging with inventory items data
            # For now, set to 0 and calculate later based on VAT rate
            vat_amount = 0
            
            # Final total sales = TOTAL + VAT AMOUNT (as requested)
            final_total_sales = total_amount + vat_amount
            
            # MAX(INVOICE.OTHER*100/INVOICE.SUBTOTAL) AS DISCOUNT
            if 'OTHER' in group_data.columns and 'SUBTOTAL' in group_data.columns:
                # Calculate discount percentage for each record and take max
                discount_percentages = (group_data['OTHER'] * 100 / group_data['SUBTOTAL']).replace([float('inf'), -float('inf')], 0)
                max_discount_pct = discount_percentages.max()
                discount_amount = (final_total_sales * max_discount_pct / 100) if max_discount_pct > 0 else 0
            else:
                max_discount_pct = 0
                discount_amount = 0
            
            calculation_results.append({
                'ITEM_CODE': str(item_code),
                'QTY_SOLD': float(sales_qty),
                'TOTAL_AMOUNT': float(total_amount),
                'VAT_AMOUNT': float(vat_amount),
                'FINAL_SALES_AMOUNT': float(final_total_sales),
                'ITEM_DISCOUNT': float(item_discount),
                'DISCOUNT_PERCENTAGE': float(max_discount_pct),
                'DISCOUNT_AMOUNT': float(discount_amount)
            })
        
        # Convert to DataFrame for easier processing
        result_df = pd.DataFrame(calculation_results)
        
        if result_df.empty:
            return jsonify({'error': 'No sales calculated for the specified criteria'}), 404
        
        print(f"📦 Calculated sales for {len(result_df)} unique items using original query logic")
        
        # Get item information (names, categories, prices, VAT rates)
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify({'error': 'Inventory items data not available'}), 400
        
        # Include VAT column for proper VAT calculation
        vat_cols = ['ITEM', 'DESCR1', 'CATEGORY', 'POSPRICE1']
        if 'VAT' in dataframes['inventory_items'].columns:
            vat_cols.append('VAT')
        
        items_df = dataframes['inventory_items'][vat_cols].drop_duplicates()
        
        # Rename columns for consistency
        new_cols = ['ITEM_CODE', 'ITEM_NAME', 'CATEGORY_ID', 'PRIX']
        if 'VAT' in dataframes['inventory_items'].columns:
            new_cols.append('VAT_RATE')
        
        items_df.columns = new_cols
        
        # Merge with item information
        result_df = result_df.merge(items_df, on='ITEM_CODE', how='left')
        
        # Fill missing item information
        result_df['ITEM_NAME'] = result_df['ITEM_NAME'].fillna('Unknown Item')
        result_df['CATEGORY_ID'] = result_df['CATEGORY_ID'].fillna('')
        result_df['PRIX'] = result_df['PRIX'].fillna(0)
        
        # Calculate proper VAT amount using VAT rate from inventory items
        if 'VAT_RATE' in result_df.columns:
            result_df['VAT_RATE'] = result_df['VAT_RATE'].fillna(0)
            # VAT amount = total_amount * (VAT rate / 100)
            result_df['VAT_AMOUNT'] = result_df['TOTAL_AMOUNT'] * (result_df['VAT_RATE'] / 100)
            # Recalculate final sales amount with proper VAT
            result_df['FINAL_SALES_AMOUNT'] = result_df['TOTAL_AMOUNT'] + result_df['VAT_AMOUNT']
            print(f"📊 Calculated VAT amounts using VAT rates from inventory items")
        else:
            print(f"⚠️ VAT_RATE column not found - VAT amounts will remain 0")
        
        # Get category names
        if 'categories' in dataframes and dataframes['categories'] is not None:
            categories_df = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            categories_df['ID'] = categories_df['ID'].astype(str)
            categories_df.columns = ['CATEGORY_ID', 'CATEGORY_NAME']
            
            result_df['CATEGORY_ID'] = result_df['CATEGORY_ID'].astype(str)
            result_df = result_df.merge(categories_df, on='CATEGORY_ID', how='left')
            result_df['CATEGORY_NAME'] = result_df['CATEGORY_NAME'].fillna('Unknown Category')
        else:
            result_df['CATEGORY_NAME'] = 'Unknown Category'
        
        # Filter out items with no sales
        result_df = result_df[result_df['QTY_SOLD'] != 0]
        
        # Sort by final sales amount descending
        result_df = result_df.sort_values('FINAL_SALES_AMOUNT', ascending=False)
        
        # Convert to list of dictionaries for JSON response
        result_data = []
        for _, row in result_df.iterrows():
            row_data = {
                'ITEM_CODE': str(row['ITEM_CODE']),
                'ITEM_NAME': str(row['ITEM_NAME']),
                'CATEGORY': str(row['CATEGORY_NAME']),
                'PRIX': float(row['PRIX']),
                'QTY_SOLD': float(row['QTY_SOLD']),
                'TOTAL_AMOUNT': float(row['TOTAL_AMOUNT']),
                'VAT_AMOUNT': float(row['VAT_AMOUNT']),
                'DISCOUNT': float(row['DISCOUNT_AMOUNT']),
                'TOTAL_SALES': float(row['FINAL_SALES_AMOUNT'])
            }
            result_data.append(row_data)
        
        # Calculate totals
        total_qty_sold = result_df['QTY_SOLD'].sum()
        total_amount_sum = result_df['TOTAL_AMOUNT'].sum()
        total_vat_sum = result_df['VAT_AMOUNT'].sum()
        total_discount = result_df['DISCOUNT_AMOUNT'].sum()
        total_sales_amount = result_df['FINAL_SALES_AMOUNT'].sum()
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'total_items': len(result_data),
                'from_date': from_date,
                'to_date': to_date,
                'site_type': site_type,
                'site_type_name': site_type_name,
                'total_qty_sold': float(total_qty_sold),
                'total_amount': float(total_amount_sum),
                'total_vat_amount': float(total_vat_sum),
                'total_discount': float(total_discount),
                'total_sales_amount': float(total_sales_amount),
                'data_source': 'Original SQL query logic: ITEMS joined with INVOICE',
                'calculation_method': {
                    'filtering': 'FTYPE IN (1,2), FDATE BETWEEN dates, SID like 530%, SUBTOTAL<>0',
                    'join': 'ITEMS.MID = INVOICE.ID',
                    'sales_qty': 'SUM(CREDITQTY-DEBITQTY) OR fallback to SUM(QTY)',
                    'total_amount': 'SUM(CREDITUS-DEBITUS)',
                    'vat_amount': 'TOTAL_AMOUNT * (VAT_RATE/100) from inventory_items.VAT',
                    'final_sales': 'TOTAL_AMOUNT + VAT_AMOUNT',
                    'discount': 'MAX(INVOICE.OTHER*100/INVOICE.SUBTOTAL) applied to final_sales'
                },
                'columns_info': {
                    'item_code': 'Item identifier',
                    'item_name': 'Item description',
                    'category': 'Item category name',
                    'prix': 'Unit price (POSPRICE1 reference)',
                    'qty_sold': 'Sales quantity (CREDITQTY-DEBITQTY)',
                    'total_amount': 'Base amount (CREDITUS-DEBITUS)',
                    'vat_amount': 'VAT amount (calculated from inventory items VAT rate)', 
                    'discount': 'Calculated discount amount',
                    'total_sales': 'Final sales (TOTAL + VAT)'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in sales by item report: {e}")
        return jsonify({'error': str(e)}), 500
