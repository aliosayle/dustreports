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
            if 'ITEM_NAME' in result_df.columns and 'CURRENT_STOCK' in result_df.columns:
                result_df = result_df.sort_values(['ITEM_NAME', 'CURRENT_STOCK'], ascending=[True, False])
        else:
            if 'SITE_NAME' in result_df.columns and 'CURRENT_STOCK' in result_df.columns:
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
        }), 200

    except Exception as e:
        print(f"Error in stock by site report: {e}")
        import traceback
        traceback.print_exc()
        # Return a proper error code and message
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
        
        # NOTE: Removed ALLSTOCK-based site filtering to match sales report exactly
        # We'll filter by SID patterns in the INVOICE table directly (same as sales report)
        
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
        
        # Get invoice data for total sales calculation (same logic as sales report)
        if 'invoice_headers' not in dataframes or dataframes['invoice_headers'] is None:
            return jsonify({'error': 'Invoice data not available'}), 400
        
        invoice_df = dataframes['invoice_headers'].copy()
        print(f"📊 Working with {len(invoice_df)} invoice records for total sales")
        
        # Filter for valid sales transactions (FTYPE = 1) - same as sales report
        if 'FTYPE' in invoice_df.columns:
            invoice_df = invoice_df[invoice_df['FTYPE'] == 1]
            print(f"📊 After FTYPE=1 filter: {len(invoice_df)} invoice records")
        
        # Filter for SID starting with "530" - EXACT same logic as sales report
        if 'SID' in invoice_df.columns:
            invoice_df = invoice_df[invoice_df['SID'].astype(str).str.startswith('530')]
            print(f"📊 After SID starts with '530' filter: {len(invoice_df)} invoice records")
        else:
            return jsonify({'error': 'SID column not found in invoice data'}), 400
        
        # Filter for Kinshasa sites (SID starting with "5301") - EXACT same logic as sales report
        invoice_df = invoice_df[invoice_df['SID'].astype(str).str.startswith('5301')]
        print(f"📊 After Kinshasa SID filter (5301): {len(invoice_df)} invoice records")
        
        if invoice_df.empty:
            return jsonify({'error': 'No Kinshasa sales found (SID starting with 5301)'}), 404
        
        # Filter by date range if specified
        if from_date or to_date:
            invoice_df['FDATE'] = pd.to_datetime(invoice_df['FDATE'], errors='coerce')
            if from_date:
                from_date_dt = pd.to_datetime(from_date)
                invoice_df = invoice_df[invoice_df['FDATE'] >= from_date_dt]
            if to_date:
                to_date_dt = pd.to_datetime(to_date)
                invoice_df = invoice_df[invoice_df['FDATE'] <= to_date_dt]
            print(f"📊 After date filter: {len(invoice_df)} invoice records")
        
        # Fill NaN values for calculations
        invoice_df['NET'] = invoice_df['NET'].fillna(0)
        
        # Get unique SITE values from filtered invoice data (SITE is the actual site identifier)
        kinshasa_sites_from_invoices = invoice_df['SITE'].unique()
        print(f"📍 Found {len(kinshasa_sites_from_invoices)} unique SITE values from Kinshasa invoices: {list(kinshasa_sites_from_invoices)[:10]}...")
        
        # Also show SID to SITE mapping for debugging
        sid_site_mapping = invoice_df[['SID', 'SITE']].drop_duplicates().head(10)
        print(f"📍 SID to SITE mapping (sample):")
        for _, row in sid_site_mapping.iterrows():
            print(f"   SID: {row['SID']} -> SITE: {row['SITE']}")
        
        # Pre-filter sales data from ITEMS table to find ciment items with actual sales
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
        
        # Calculate stock for all ciment items at each site (same approach as autonomy stock)
        print(f"📦 Calculating stock for ciment items at each site...")
        inventory_df = dataframes.get('inventory_transactions')
        if inventory_df is not None:
            # Filter inventory transactions for ciment items only
            ciment_inventory = inventory_df[inventory_df['ITEM'].isin(ciment_items)].copy()
            
            # Fill NaN values with 0 for calculations
            ciment_inventory['DEBITQTY'] = ciment_inventory['DEBITQTY'].fillna(0)
            ciment_inventory['CREDITQTY'] = ciment_inventory['CREDITQTY'].fillna(0)
            
            # Calculate stock by SITE and ITEM: DEBITQTY - CREDITQTY (same as autonomy stock)
            stock_summary = ciment_inventory.groupby(['SITE', 'ITEM']).agg({
                'DEBITQTY': 'sum',
                'CREDITQTY': 'sum'
            }).reset_index()
            
            # Calculate current stock: DEBITQTY (incoming) - CREDITQTY (outgoing)
            stock_summary['CURRENT_STOCK'] = stock_summary['DEBITQTY'] - stock_summary['CREDITQTY']
            
            print(f"📊 Stock calculation: {len(stock_summary)} unique item/site combinations for ciment items")
        else:
            stock_summary = pd.DataFrame()
            print("⚠️ No inventory transactions available for stock calculation")
        
        # Calculate sales for each site (no stock data needed)
        result_data = []
        
        # Get site names from sites dataframe using SITE field
        site_names_mapping = {}
        if 'sites' in dataframes and dataframes['sites'] is not None:
            sites_df = dataframes['sites'].copy()
            if 'SITE' in sites_df.columns:
                # Convert both to string for proper matching
                sites_df['ID'] = sites_df['ID'].astype(str)
                site_names_dict = dict(zip(sites_df['ID'], sites_df['SITE']))
                site_names_mapping = site_names_dict
                print(f"📍 Retrieved site names for {len(site_names_dict)} sites from sites dataframe (SITE field)")
                
                # Debug: Show some sample mappings
                sample_sites = list(site_names_dict.items())[:5]
                print(f"📍 Sample site mappings:")
                for site_id, site_name in sample_sites:
                    print(f"   {site_id} -> {site_name}")
        
        for i, site_id in enumerate(kinshasa_sites_from_invoices):
            if i % 10 == 0:  # Progress logging every 10 sites
                print(f"📍 Processing site {i+1}/{len(kinshasa_sites_from_invoices)}: {site_id}")
            
            # Convert site_id to string for proper matching
            site_id_str = str(site_id)
            
            # Use site name from sites dataframe DESCR1 field, otherwise use SITE value
            # NOTE: site_name is for DISPLAY ONLY - all filtering uses actual SITE field
            site_name = site_names_mapping.get(site_id_str, f"Site {site_id}")
            print(f"  🏷️  SITE {site_id} -> Display name: '{site_name}'")
            
            # Initialize row data with Python native types (sales only)
            row_data = {
                'SITE_ID': site_id_str,         # Actual SITE field value (as string)
                'SITE_NAME': str(site_name),    # Display name from sites.DESCR1
                'TOTAL_SALES': 0.0,             # Total sales for site (all items)
                'TOTAL_CIMENT_STOCK': 0.0,      # Total ciment stock at this site
                'CIMENT_ARTICLES_WITH_SALES': 0,  # Count of ciment items with sales
                'TOTAL_CIMENT_SALES_QTY': 0.0  # Total ciment sales quantity
            }
            
            # Calculate total sales for this site using INVOICE.NET
            if not invoice_df.empty:
                # Use SITE column from INVOICE table (site_id is the SITE field value)
                site_invoices = invoice_df[invoice_df['SITE'] == site_id]
                
                # Calculate total sales using NET amounts (actual invoiced revenue)
                total_site_sales = float(site_invoices['NET'].fillna(0).sum())
                row_data['TOTAL_SALES'] = total_site_sales
                
                print(f"📍 SITE {site_id}: {len(site_invoices)} invoices, total sales: ${total_site_sales:,.2f}")
            else:
                row_data['TOTAL_SALES'] = 0.0
                print(f"📍 SITE {site_id}: No invoice data available")
            
            # Calculate ciment stock for this site
            if not stock_summary.empty:
                site_stock = stock_summary[stock_summary['SITE'] == site_id]
                total_ciment_stock = float(site_stock['CURRENT_STOCK'].fillna(0).sum())
                row_data['TOTAL_CIMENT_STOCK'] = total_ciment_stock
                print(f"  📦 SITE {site_id}: {len(site_stock)} ciment items, total stock: {total_ciment_stock:,.0f}")
            else:
                row_data['TOTAL_CIMENT_STOCK'] = 0.0
                print(f"  📦 SITE {site_id}: No stock data available")
            
            # Calculate ciment-specific sales from ITEMS table (for individual item breakdown)
            if not all_sales.empty:
                # Use site_id (SITE field) for filtering ITEMS table, not site_name
                site_sales = all_sales[all_sales['SITE'] == site_id]
                print(f"  📦 ITEMS table filtering: Found {len(site_sales)} records for SITE {site_id}")
                
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
                
                row_data['CIMENT_ARTICLES_WITH_SALES'] = articles_with_sales
                row_data['TOTAL_CIMENT_SALES_QTY'] = total_ciment_sales
                row_data['CIMENT_SALES_BY_ITEM'] = ciment_sales_by_item
                row_data['CIMENT_AMOUNTS_BY_ITEM'] = ciment_amounts_by_item
                
                # Debug comparison: Invoice sales vs Ciment sales (all using SITE field for filtering)
                total_sales = row_data.get('TOTAL_SALES', 0)
                total_stock = row_data.get('TOTAL_CIMENT_STOCK', 0)
                if total_sales > 0 or total_amount > 0:
                    print(f"  💰 SITE {site_id} ('{site_name}'): Invoice Sales: ${total_sales:,.2f}, Ciment Sales: ${total_amount:,.2f}, Ciment Stock: {total_stock:,.0f}")
            else:
                # No ITEMS data available for ciment calculations
                row_data['CIMENT_SALES_BY_ITEM'] = {str(item): 0.0 for item in active_ciment_items}
                row_data['CIMENT_AMOUNTS_BY_ITEM'] = {str(item): 0.0 for item in active_ciment_items}
            
            result_data.append(row_data)
        
        # Filter out sites with zero total sales (using INVOICE.NET amounts)
        result_data = [site_data for site_data in result_data if site_data['TOTAL_SALES'] > 0]
        print(f"📊 Filtered out sites with zero invoice sales: {len(result_data)} sites remaining")
        
        # Sort by total sales descending (highest invoice sales first)
        result_data.sort(key=lambda x: x['TOTAL_SALES'], reverse=True)
        
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
        
        # Summary comparison of invoice vs ciment sales
        total_invoice_sales = sum(site['TOTAL_SALES'] for site in result_data)
        total_ciment_stock = sum(site['TOTAL_CIMENT_STOCK'] for site in result_data)
        print(f"💰 SALES SUMMARY:")
        print(f"   📊 Total Invoice Sales (INVOICE.NET): ${total_invoice_sales:,.2f}")
        print(f"   📦 Total Ciment Stock (ALLITEM): {total_ciment_stock:,.0f} units")
        print(f"   📈 Stock Distribution: {len([s for s in result_data if s['TOTAL_CIMENT_STOCK'] > 0])} sites with ciment stock")
        
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
                'sales_only_mode': False,  # Now includes stock data
                'data_source': 'INVOICE table (NET) for total sales + ITEMS table for ciment details + ALLITEM for stock',
                'calculation_method': {
                    'total_sales': 'SUM(INVOICE.NET) grouped by SITE (filtered by SID patterns for Kinshasa)',
                    'ciment_sales': 'SUM(ITEMS.QTY) filtered by ciment category × STOCK.POSPRICE1',
                    'ciment_stock': 'SUM(ALLITEM.DEBITQTY - ALLITEM.CREDITQTY) filtered by ciment category and SITE',
                    'filtering': 'FTYPE = 1, SID starts with "530" then "5301" (Kinshasa), then group by SITE, date range filtering'
                },
                'columns_info': {
                    'site': 'Site name from sites.SITE field',
                    'total_sales': 'Total site sales revenue (INVOICE.NET filtered by SITE)',
                    'total_ciment_stock': 'Total ciment stock quantity (ALLITEM filtered by ciment category and SITE)',
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
    Replicates: SUM(CREDITUS-DEBITUS) + SUM(CREDITVATAMOUNT-DEBITVATAMOUNT) as total sales
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
        original_calc_fields = ['CREDITQTY', 'DEBITQTY', 'CREDITUS', 'DEBITUS', 'CREDITVATAMOUNT', 'DEBITVATAMOUNT', 'discount']
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
        numeric_fields = ['CREDITQTY', 'DEBITQTY', 'CREDITUS', 'DEBITUS', 'CREDITVATAMOUNT', 'DEBITVATAMOUNT', 'QTY']
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

@api_bp.route('/kinshasa-bureau-client-report', methods=['POST'])
def api_kinshasa_bureau_client_report():
    """
    Generate Kinshasa Sales Bureau Client Report
    Shows sales and returns for office clients (SID starting with 411)
    """
    try:
        data = request.get_json()
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        site_sidno = data.get('site_sidno')  # Optional: filter by SIDNO from ALLSTOCK table (can be array or single value)
        contact = data.get('contact')  # Optional: filter by CONTACT (can be array or single value)
        
        print(f"🔍 Report 7 - Received contact filter: {contact} (type: {type(contact)})")
        
        # Validate dates
        if not from_date or not to_date:
            return jsonify({'error': 'Both from_date and to_date are required'}), 400
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get sales details data (ITEMS table)
        if 'sales_details' not in dataframes or dataframes['sales_details'] is None:
            return jsonify({'error': 'Sales details data not available'}), 400
        
        sales_df = dataframes['sales_details'].copy()
        print(f"📊 Working with {len(sales_df)} sales detail records")
        
        # Filter for SID starting with "411" (office clients)
        if 'SID' in sales_df.columns:
            sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 After SID starts with '411' filter: {len(sales_df)} records")
        else:
            return jsonify({'error': 'SID column not found in sales details'}), 400
        
        # Filter by SIDNO from ALLSTOCK table if site_sidno is provided
        # Join: ITEMS.SITE = ALLSTOCK.ID, then filter by ALLSTOCK.SIDNO
        # site_sidno can be a single value or an array
        if site_sidno:
            if 'sites' not in dataframes or dataframes['sites'] is None:
                return jsonify({'error': 'Sites data (ALLSTOCK) not available'}), 400
            
            sites_df = dataframes['sites'].copy()
            
            # Check if SIDNO column exists
            if 'SIDNO' not in sites_df.columns:
                return jsonify({'error': 'SIDNO column not found in sites (ALLSTOCK) table'}), 400
            
            # Convert site_sidno to list if it's a single value
            if not isinstance(site_sidno, list):
                site_sidno = [site_sidno]
            
            # Convert all to strings for comparison
            site_sidno_str = [str(s) for s in site_sidno]
            
            # Filter sites by SIDNO (multiple values)
            filtered_sites = sites_df[sites_df['SIDNO'].astype(str).isin(site_sidno_str)]
            
            if filtered_sites.empty:
                return jsonify({'error': f'No sites found with SIDNO in {site_sidno_str}'}), 404
            
            # Get site IDs (ALLSTOCK.ID) that match any of the SIDNO values
            site_ids = filtered_sites['ID'].unique().tolist()
            print(f"📍 Found {len(site_ids)} sites with SIDNO in {site_sidno_str}")
            
            # Filter sales_df by SITE (ITEMS.SITE = ALLSTOCK.ID)
            if 'SITE' in sales_df.columns:
                # Convert both to string for proper matching
                sales_df['SITE'] = sales_df['SITE'].astype(str)
                site_ids_str = [str(sid) for sid in site_ids]
                sales_df = sales_df[sales_df['SITE'].isin(site_ids_str)]
                print(f"📊 After SIDNO filter ({site_sidno_str}): {len(sales_df)} records")
            else:
                return jsonify({'error': 'SITE column not found in sales details'}), 400
        
        # Filter by date range
        if 'FDATE' in sales_df.columns:
            sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
            from_date_dt = pd.to_datetime(from_date)
            to_date_dt = pd.to_datetime(to_date)
            
            sales_df = sales_df[
                (sales_df['FDATE'] >= from_date_dt) & 
                (sales_df['FDATE'] <= to_date_dt)
            ]
            print(f"📊 After date filter ({from_date} to {to_date}): {len(sales_df)} records")
        else:
            return jsonify({'error': 'FDATE column not found in sales details'}), 400
        
        # Filter by CONTACT if provided (via SUB table)
        if contact:
            if 'accounts' not in dataframes or dataframes['accounts'] is None:
                return jsonify({'error': 'Accounts data (SUB table) not available for CONTACT filtering'}), 400
            
            accounts_df = dataframes['accounts'].copy()
            
            # Check if CONTACT column exists in SUB table (case-insensitive)
            contact_col = None
            for col in accounts_df.columns:
                if col.upper() == 'CONTACT':
                    contact_col = col
                    break
            
            if not contact_col:
                return jsonify({'error': 'CONTACT column not found in SUB table (accounts)'}), 400
            
            # Convert contact to list if it's a single value
            if not isinstance(contact, list):
                contact = [contact]
            
            # Filter SUB table by SID starting with '411' first
            accounts_df = accounts_df[accounts_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 Accounts with SID starting with 411: {len(accounts_df)}")
            
            # Try multiple matching strategies
            filtered_accounts = None
            
            # Strategy 1: Try numeric matching (CONTACT might be float/int)
            try:
                contact_values = [float(c) for c in contact]
                # Handle NaN values in CONTACT column
                accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_values)]
                if not filtered_accounts.empty:
                    print(f"📊 Matched {len(filtered_accounts)} accounts using numeric matching")
            except (ValueError, TypeError) as e:
                print(f"⚠️ Numeric matching failed: {e}")
            
            # Strategy 2: If numeric failed or returned empty, try string matching
            if filtered_accounts is None or filtered_accounts.empty:
                try:
                    contact_str = [str(c).strip() for c in contact]
                    accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                    accounts_df_clean[contact_col] = accounts_df_clean[contact_col].astype(str).str.strip()
                    filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_str)]
                    if not filtered_accounts.empty:
                        print(f"📊 Matched {len(filtered_accounts)} accounts using string matching")
                except Exception as e:
                    print(f"⚠️ String matching failed: {e}")
            
            if filtered_accounts is None or filtered_accounts.empty:
                # Debug: show some sample CONTACT values
                sample_contacts = accounts_df[contact_col].dropna().unique()[:10]
                print(f"⚠️ No matches found. Sample CONTACT values in SUB table: {sample_contacts}")
                print(f"⚠️ Looking for CONTACT values: {contact}")
                return jsonify({'error': f'No clients found with CONTACT in {contact}. Please check the contact values.'}), 404
            
            # Get SIDs from filtered accounts
            filtered_sids = filtered_accounts['SID'].astype(str).unique().tolist()
            print(f"📊 Found {len(filtered_sids)} unique clients with CONTACT in {contact}")
            print(f"📊 Sample SIDs: {filtered_sids[:5] if len(filtered_sids) > 5 else filtered_sids}")
            
            # Filter sales_df by these SIDs
            sales_df_before = len(sales_df)
            sales_df = sales_df[sales_df['SID'].astype(str).isin(filtered_sids)]
            print(f"📊 After CONTACT filter ({contact}): {len(sales_df)} records (was {sales_df_before})")
        
        if sales_df.empty:
            return jsonify({'error': 'No sales found for the specified criteria'}), 404
        
        # Ensure FTYPE and QTY columns exist
        if 'FTYPE' not in sales_df.columns:
            return jsonify({'error': 'FTYPE column not found in sales details'}), 400
        
        # Get quantity column
        qty_col = 'QTY' if 'QTY' in sales_df.columns else ('QTY1' if 'QTY1' in sales_df.columns else None)
        if qty_col is None:
            return jsonify({'error': 'No quantity column (QTY/QTY1) found in sales details'}), 400
        
        # Fill NaN values
        sales_df[qty_col] = sales_df[qty_col].fillna(0)
        
        # Fill NaN values for USD calculation fields
        if 'CREDITUS' in sales_df.columns:
            sales_df['CREDITUS'] = sales_df['CREDITUS'].fillna(0)
        if 'DEBITUS' in sales_df.columns:
            sales_df['DEBITUS'] = sales_df['DEBITUS'].fillna(0)
        if 'CREDITVATAMOUNT' in sales_df.columns:
            sales_df['CREDITVATAMOUNT'] = sales_df['CREDITVATAMOUNT'].fillna(0)
        if 'DEBITVATAMOUNT' in sales_df.columns:
            sales_df['DEBITVATAMOUNT'] = sales_df['DEBITVATAMOUNT'].fillna(0)
        
        # Filter for FTYPE 1 (sales) and FTYPE 2 (returns)
        sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]
        
        # Calculate sales (FTYPE = 1) and returns (FTYPE = 2) by SID (client)
        sales_by_client = sales_df.groupby(['SID', 'FTYPE'])[qty_col].sum().reset_index()
        
        # Separate sales and returns
        sales_only = sales_by_client[sales_by_client['FTYPE'] == 1].copy()
        returns_only = sales_by_client[sales_by_client['FTYPE'] == 2].copy()
        
        # Calculate number of invoices (distinct MID) per client
        invoice_counts = {}
        if 'MID' in sales_df.columns:
            # Count distinct MID per SID
            invoice_counts_df = sales_df.groupby('SID')['MID'].nunique().reset_index()
            invoice_counts_df.columns = ['SID', 'INVOICE_COUNT']
            invoice_counts = dict(zip(invoice_counts_df['SID'].astype(str), invoice_counts_df['INVOICE_COUNT']))
            print(f"📊 Calculated invoice counts for {len(invoice_counts)} clients")
        else:
            print("⚠️ MID column not found - invoice count will be 0")
        
        # Calculate USD amounts per client using CREDITUS-DEBITUS formula (same as sales by item report)
        usd_amounts = {}
        if 'CREDITUS' in sales_df.columns and 'DEBITUS' in sales_df.columns:
            # Calculate base amount: SUM(CREDITUS - DEBITUS) by SID
            sales_df['BASE_AMOUNT'] = sales_df['CREDITUS'] - sales_df['DEBITUS']
            base_amounts = sales_df.groupby('SID')['BASE_AMOUNT'].sum().reset_index()
            base_amounts.columns = ['SID', 'BASE_AMOUNT']
            
            # Calculate VAT amount if available: SUM(CREDITVATAMOUNT - DEBITVATAMOUNT) by SID
            vat_amounts = pd.Series(0, index=sales_df['SID'].unique())
            if 'CREDITVATAMOUNT' in sales_df.columns and 'DEBITVATAMOUNT' in sales_df.columns:
                sales_df['VAT_AMOUNT'] = sales_df['CREDITVATAMOUNT'] - sales_df['DEBITVATAMOUNT']
                vat_amounts_df = sales_df.groupby('SID')['VAT_AMOUNT'].sum().reset_index()
                vat_amounts_df.columns = ['SID', 'VAT_AMOUNT']
                vat_amounts = dict(zip(vat_amounts_df['SID'].astype(str), vat_amounts_df['VAT_AMOUNT']))
            
            # Merge base amounts with VAT amounts
            for _, row in base_amounts.iterrows():
                sid_str = str(row['SID'])
                base_amount = float(row['BASE_AMOUNT'])
                vat_amount = float(vat_amounts.get(sid_str, 0))
                # Final USD amount = BASE_AMOUNT + VAT_AMOUNT (same as sales by item report)
                usd_amounts[sid_str] = base_amount + vat_amount
            
            print(f"📊 Calculated USD amounts for {len(usd_amounts)} clients using CREDITUS-DEBITUS formula")
        else:
            print("⚠️ CREDITUS/DEBITUS columns not found - USD amounts will be 0")
        
        # Create result dataframe
        result_data = []
        all_sids = sales_df['SID'].unique()
        
        for sid in all_sids:
            sid_str = str(sid)
            
            # Get sales quantity (FTYPE = 1)
            sales_qty = sales_only[sales_only['SID'] == sid][qty_col].sum() if not sales_only[sales_only['SID'] == sid].empty else 0
            
            # Get returns quantity (FTYPE = 2)
            returns_qty = returns_only[returns_only['SID'] == sid][qty_col].sum() if not returns_only[returns_only['SID'] == sid].empty else 0
            
            # Calculate total (sales - returns)
            total_qty = sales_qty - returns_qty
            
            # Get number of invoices
            num_invoices = invoice_counts.get(sid_str, 0)
            
            # Get USD amount
            usd_amount = usd_amounts.get(sid_str, 0.0)
            
            result_data.append({
                'SID': sid_str,
                'SALES_QTY': float(sales_qty),
                'RETURNS_QTY': float(returns_qty),
                'TOTAL_QTY': float(total_qty),
                'NUM_INVOICES': int(num_invoices),
                'QUANTITY_USD': float(usd_amount)
            })
        
        # Get client names and CONTACT from SUB table (accounts dataframe)
        # Get data from FULL accounts_df to ensure we get correct CONTACT values
        result_sids = [row['SID'] for row in result_data]
        client_names = {}
        client_contacts = {}
        
        if 'accounts' in dataframes and dataframes['accounts'] is not None:
            # Use FULL accounts_df to get correct CONTACT values for each SID
            accounts_df = dataframes['accounts'].copy()
            accounts_df['SID'] = accounts_df['SID'].astype(str)
            
            # Filter accounts_df to only include SIDs in the result (for efficiency)
            accounts_df_filtered = accounts_df[accounts_df['SID'].isin(result_sids)]
            
            if 'SNAME' in accounts_df_filtered.columns:
                # Get unique SID-SNAME mappings (take first if duplicates)
                sid_name_map = accounts_df_filtered[['SID', 'SNAME']].drop_duplicates(subset=['SID'], keep='first')
                client_names = dict(zip(sid_name_map['SID'], sid_name_map['SNAME']))
                print(f"📍 Retrieved client names for {len(client_names)} clients from SUB table")
            
            # Get CONTACT values from SUB table - use FULL accounts_df to ensure correct values
            contact_col = None
            for col in accounts_df.columns:
                if col.upper() == 'CONTACT':
                    contact_col = col
                    break
            
            if contact_col:
                # Get CONTACT values from FULL accounts_df, but only for SIDs in result
                # This ensures we get the correct CONTACT value for each SID
                accounts_df_for_contact = accounts_df[accounts_df['SID'].isin(result_sids)]
                
                # Get unique SID-CONTACT mappings (take first if duplicates exist)
                sid_contact_map = accounts_df_for_contact[['SID', contact_col]].drop_duplicates(subset=['SID'], keep='first')
                
                # Don't filter out NaN - we want to see if there are missing values
                # But convert to dict, handling NaN properly
                client_contacts = {}
                for _, row in sid_contact_map.iterrows():
                    sid = str(row['SID'])
                    contact_val = row[contact_col]
                    if pd.notna(contact_val):
                        client_contacts[sid] = contact_val
                
                print(f"📍 Retrieved CONTACT values for {len(client_contacts)} clients from SUB table")
                print(f"📍 Sample CONTACT values: {list(client_contacts.items())[:5]}")
                print(f"📍 Result SIDs count: {len(result_sids)}, CONTACT values count: {len(client_contacts)}")
        
        # Add client names and CONTACT to result data
        for row in result_data:
            sid = row['SID']
            row['CLIENT_NAME'] = client_names.get(sid, f"Client {sid}")
            # Convert CONTACT to string or None for JSON serialization
            contact_val = client_contacts.get(sid, None)
            if contact_val is not None and pd.notna(contact_val):
                # Convert to string, handling both numeric and string types
                if isinstance(contact_val, (int, float)):
                    # If it's a whole number, convert to int then string to avoid .0
                    if isinstance(contact_val, float) and contact_val == int(contact_val):
                        row['CONTACT'] = str(int(contact_val))
                    else:
                        row['CONTACT'] = str(contact_val)
                else:
                    row['CONTACT'] = str(contact_val)
            else:
                row['CONTACT'] = 'N/A'  # Changed from None to 'N/A' for visibility
            # Debug: log first few rows
            if len([r for r in result_data if 'CONTACT' in r]) <= 5:
                print(f"📊 Row SID: {sid}, CONTACT: {row.get('CONTACT', 'N/A')}")
        
        # Sort by total quantity descending
        result_data.sort(key=lambda x: x['TOTAL_QTY'], reverse=True)
        
        # Calculate totals
        total_sales = sum(row['SALES_QTY'] for row in result_data)
        total_returns = sum(row['RETURNS_QTY'] for row in result_data)
        total_net = sum(row['TOTAL_QTY'] for row in result_data)
        total_invoices = sum(row['NUM_INVOICES'] for row in result_data)
        total_usd = sum(row['QUANTITY_USD'] for row in result_data)
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'total_clients': len(result_data),
                'from_date': from_date,
                'to_date': to_date,
                'total_sales_qty': float(total_sales),
                'total_returns_qty': float(total_returns),
                'total_net_qty': float(total_net),
                'total_invoices': int(total_invoices),
                'total_quantity_usd': float(total_usd),
                'site_sidno': site_sidno if site_sidno else None,
                'site_sidno_names': [
                    {
                        '3700002': 'Kinshasa',
                        '3700004': 'Depot Kinshasa',
                        '3700003': 'Interieur'
                    }.get(str(s), f'SIDNO {s}') for s in (site_sidno if isinstance(site_sidno, list) else [site_sidno])
                ] if site_sidno else None,
                'contact': contact if contact else None,
                'filter': f'SID starting with 411 (Office Clients)' + (f', SIDNO in {site_sidno if isinstance(site_sidno, list) else [site_sidno]}' if site_sidno else '') + (f', CONTACT in {contact if isinstance(contact, list) else [contact]}' if contact else ''),
                'data_source': 'ITEMS table (sales_details) joined with ALLSTOCK table (sites) for SIDNO filtering',
                'calculation_method': {
                    'sales': 'SUM(QTY) where FTYPE = 1',
                    'returns': 'SUM(QTY) where FTYPE = 2',
                    'total': 'SALES_QTY - RETURNS_QTY',
                    'num_invoices': 'COUNT(DISTINCT MID) per SID',
                    'quantity_usd': 'SUM(CREDITUS - DEBITUS) + SUM(CREDITVATAMOUNT - DEBITVATAMOUNT) per SID (same as sales by item report)'
                },
                'columns_info': {
                    'client_name': 'Client name from SUB.SNAME',
                    'sales_qty': 'Total sales quantity (FTYPE = 1)',
                    'returns_qty': 'Total returns quantity (FTYPE = 2)',
                    'total_qty': 'Net quantity (Sales - Returns)',
                    'num_invoices': 'Number of distinct invoices (MID) for this client',
                    'quantity_usd': 'Total amount in USD: SUM(CREDITUS-DEBITUS) + SUM(CREDITVATAMOUNT-DEBITVATAMOUNT)'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in Kinshasa Bureau Client report: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/unique-contacts', methods=['GET'])
def api_unique_contacts():
    """
    Get unique CONTACT values from SUB table (accounts dataframe)
    Used for populating CONTACT filter dropdown
    Filters for clients with SID starting with 411 (office clients)
    """
    try:
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get accounts data (SUB table)
        if 'accounts' not in dataframes or dataframes['accounts'] is None:
            return jsonify({'error': 'Accounts data (SUB table) not available'}), 400
        
        accounts_df = dataframes['accounts'].copy()
        
        # Check if CONTACT column exists (case-insensitive)
        contact_col = None
        for col in accounts_df.columns:
            if col.upper() == 'CONTACT':
                contact_col = col
                break
        
        if not contact_col:
            return jsonify({'error': 'CONTACT column not found in SUB table (accounts)'}), 400
        
        # Filter for SID starting with "411" (office clients) to match report scope
        # SUB table uses SID as the client identifier
        if 'SID' not in accounts_df.columns:
            return jsonify({'error': 'SID column not found in SUB table (accounts)'}), 400
        
        accounts_df = accounts_df[accounts_df['SID'].astype(str).str.startswith('411')]
        
        # Get unique non-null CONTACT values
        unique_contacts = accounts_df[contact_col].dropna().unique()
        
        # Convert to list and sort (handle both numeric and string types)
        try:
            # Try to convert to numeric and sort
            unique_contacts = sorted([float(x) for x in unique_contacts if pd.notna(x)])
            # Convert back to string for JSON serialization
            unique_contacts = [str(int(x)) if x == int(x) else str(x) for x in unique_contacts]
        except (ValueError, TypeError):
            # If not numeric, sort as strings
            unique_contacts = sorted([str(x) for x in unique_contacts if pd.notna(x)])
        
        return jsonify({
            'contacts': unique_contacts,
            'count': len(unique_contacts)
        })
        
    except Exception as e:
        print(f"Error in unique contacts: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/kinshasa-bureau-client-items', methods=['POST'])
def api_kinshasa_bureau_client_items():
    """
    Get all items purchased by a specific client (SID) for Kinshasa Bureau
    Shows items with sales and returns quantities
    """
    try:
        data = request.get_json()
        client_sid = data.get('client_sid')
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        site_sidno = data.get('site_sidno')  # Optional: filter by SIDNO from ALLSTOCK table (can be array or single value)
        
        # Validate inputs
        if not client_sid:
            return jsonify({'error': 'Client SID is required'}), 400
        if not from_date or not to_date:
            return jsonify({'error': 'Both from_date and to_date are required'}), 400
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get sales details data (ITEMS table)
        if 'sales_details' not in dataframes or dataframes['sales_details'] is None:
            return jsonify({'error': 'Sales details data not available'}), 400
        
        sales_df = dataframes['sales_details'].copy()
        
        # Filter for specific client SID
        if 'SID' in sales_df.columns:
            sales_df = sales_df[sales_df['SID'].astype(str) == str(client_sid)]
        else:
            return jsonify({'error': 'SID column not found in sales details'}), 400
        
        # Filter by SIDNO from ALLSTOCK table if site_sidno is provided
        # Join: ITEMS.SITE = ALLSTOCK.ID, then filter by ALLSTOCK.SIDNO
        # site_sidno can be a single value or an array
        if site_sidno:
            if 'sites' not in dataframes or dataframes['sites'] is None:
                return jsonify({'error': 'Sites data (ALLSTOCK) not available'}), 400
            
            sites_df = dataframes['sites'].copy()
            
            # Check if SIDNO column exists
            if 'SIDNO' not in sites_df.columns:
                return jsonify({'error': 'SIDNO column not found in sites (ALLSTOCK) table'}), 400
            
            # Convert site_sidno to list if it's a single value
            if not isinstance(site_sidno, list):
                site_sidno = [site_sidno]
            
            # Convert all to strings for comparison
            site_sidno_str = [str(s) for s in site_sidno]
            
            # Filter sites by SIDNO (multiple values)
            filtered_sites = sites_df[sites_df['SIDNO'].astype(str).isin(site_sidno_str)]
            
            if filtered_sites.empty:
                return jsonify({'error': f'No sites found with SIDNO in {site_sidno_str}'}), 404
            
            # Get site IDs (ALLSTOCK.ID) that match any of the SIDNO values
            site_ids = filtered_sites['ID'].unique().tolist()
            print(f"📍 Found {len(site_ids)} sites with SIDNO in {site_sidno_str}")
            
            # Filter sales_df by SITE (ITEMS.SITE = ALLSTOCK.ID)
            if 'SITE' in sales_df.columns:
                # Convert both to string for proper matching
                sales_df['SITE'] = sales_df['SITE'].astype(str)
                site_ids_str = [str(sid) for sid in site_ids]
                sales_df = sales_df[sales_df['SITE'].isin(site_ids_str)]
                print(f"📊 After SIDNO filter ({site_sidno_str}): {len(sales_df)} records")
            else:
                return jsonify({'error': 'SITE column not found in sales details'}), 400
        
        # Filter by date range
        if 'FDATE' in sales_df.columns:
            sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
            from_date_dt = pd.to_datetime(from_date)
            to_date_dt = pd.to_datetime(to_date)
            
            sales_df = sales_df[
                (sales_df['FDATE'] >= from_date_dt) & 
                (sales_df['FDATE'] <= to_date_dt)
            ]
        else:
            return jsonify({'error': 'FDATE column not found in sales details'}), 400
        
        if sales_df.empty:
            return jsonify({'error': 'No sales found for this client in the specified period'}), 404
        
        # Get quantity column
        qty_col = 'QTY' if 'QTY' in sales_df.columns else ('QTY1' if 'QTY1' in sales_df.columns else None)
        if qty_col is None:
            return jsonify({'error': 'No quantity column (QTY/QTY1) found in sales details'}), 400
        
        # Fill NaN values
        sales_df[qty_col] = sales_df[qty_col].fillna(0)
        
        # Filter for FTYPE 1 (sales) and FTYPE 2 (returns)
        sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]
        
        # Calculate sales (FTYPE = 1) and returns (FTYPE = 2) by ITEM
        sales_by_item = sales_df.groupby(['ITEM', 'FTYPE'])[qty_col].sum().reset_index()
        
        # Separate sales and returns
        sales_only = sales_by_item[sales_by_item['FTYPE'] == 1].copy()
        returns_only = sales_by_item[sales_by_item['FTYPE'] == 2].copy()
        
        # Get all items for this client
        all_items = sales_df['ITEM'].unique()
        result_data = []
        
        for item_code in all_items:
            # Get sales quantity (FTYPE = 1)
            sales_qty = sales_only[sales_only['ITEM'] == item_code][qty_col].sum() if not sales_only[sales_only['ITEM'] == item_code].empty else 0
            
            # Get returns quantity (FTYPE = 2)
            returns_qty = returns_only[returns_only['ITEM'] == item_code][qty_col].sum() if not returns_only[returns_only['ITEM'] == item_code].empty else 0
            
            # Calculate total (sales - returns)
            total_qty = sales_qty - returns_qty
            
            result_data.append({
                'ITEM_CODE': str(item_code),
                'SALES_QTY': float(sales_qty),
                'RETURNS_QTY': float(returns_qty),
                'TOTAL_QTY': float(total_qty)
            })
        
        # Convert to DataFrame for easier processing
        result_df = pd.DataFrame(result_data)
        
        # Get item information (names, categories, weight) from inventory_items
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify({'error': 'Inventory items data not available'}), 400
        
        # Include NWEIGHT column for weight calculation
        items_df = dataframes['inventory_items'][['ITEM', 'DESCR1', 'CATEGORY', 'NWEIGHT']].drop_duplicates()
        items_df.columns = ['ITEM_CODE', 'ITEM_NAME', 'CATEGORY_ID', 'NWEIGHT']
        items_df['ITEM_CODE'] = items_df['ITEM_CODE'].astype(str)
        result_df['ITEM_CODE'] = result_df['ITEM_CODE'].astype(str)
        
        # Merge with item information
        result_df = result_df.merge(items_df, on='ITEM_CODE', how='left')
        result_df['ITEM_NAME'] = result_df['ITEM_NAME'].fillna('Unknown Item')
        result_df['CATEGORY_ID'] = result_df['CATEGORY_ID'].fillna('')
        
        # Fill NaN values for NWEIGHT and calculate weight (NWEIGHT * TOTAL_QTY), then convert to tons (divide by 1000)
        result_df['NWEIGHT'] = result_df['NWEIGHT'].fillna(0)
        result_df['WEIGHT'] = (result_df['NWEIGHT'] * result_df['TOTAL_QTY']) / 1000  # Convert to tons
        
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
        result_df = result_df[result_df['TOTAL_QTY'] != 0]
        
        # Sort by total quantity descending
        result_df = result_df.sort_values('TOTAL_QTY', ascending=False)
        
        # Get client name
        client_name = f"Client {client_sid}"
        if 'accounts' in dataframes and dataframes['accounts'] is not None:
            accounts_df = dataframes['accounts'].copy()
            if 'SID' in accounts_df.columns and 'SNAME' in accounts_df.columns:
                accounts_df['SID'] = accounts_df['SID'].astype(str)
                client_info = accounts_df[accounts_df['SID'] == str(client_sid)]
                if not client_info.empty:
                    client_name = str(client_info.iloc[0]['SNAME']) if pd.notna(client_info.iloc[0]['SNAME']) else f"Client {client_sid}"
        
        # Convert to list of dictionaries for JSON response
        result_data = []
        for _, row in result_df.iterrows():
            row_data = {
                'ITEM_CODE': str(row['ITEM_CODE']),
                'ITEM_NAME': str(row['ITEM_NAME']),
                'CATEGORY': str(row['CATEGORY_NAME']),
                'SALES_QTY': float(row['SALES_QTY']),
                'RETURNS_QTY': float(row['RETURNS_QTY']),
                'TOTAL_QTY': float(row['TOTAL_QTY']),
                'WEIGHT': float(row['WEIGHT'])
            }
            result_data.append(row_data)
        
        # Calculate totals
        total_sales = result_df['SALES_QTY'].sum()
        total_returns = result_df['RETURNS_QTY'].sum()
        total_net = result_df['TOTAL_QTY'].sum()
        total_weight = result_df['WEIGHT'].sum()
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'client_sid': str(client_sid),
                'client_name': client_name,
                'total_items': len(result_data),
                'from_date': from_date,
                'to_date': to_date,
                'total_sales_qty': float(total_sales),
                'total_returns_qty': float(total_returns),
                'total_net_qty': float(total_net),
                'total_weight': float(total_weight),
                'filter': f'SID = {client_sid} (Office Client)',
                'data_source': 'ITEMS table (sales_details)'
            }
        })
        
    except Exception as e:
        print(f"Error in Kinshasa Bureau Client Items: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/kinshasa-bureau-item-clients', methods=['POST'])
def api_kinshasa_bureau_item_clients():
    """
    Get all clients who purchased a specific item for Kinshasa Bureau
    Shows clients with sales and returns quantities, number of invoices, and USD amounts
    Similar to Report 7 but filtered by item code
    """
    try:
        data = request.get_json()
        item_code = data.get('item_code')
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        site_sidno = data.get('site_sidno')  # Optional: filter by SIDNO from ALLSTOCK table (can be array or single value)
        contact = data.get('contact')  # Optional: filter by CONTACT (can be array or single value)
        
        print(f"🔍 Item Clients Report - Received contact filter: {contact} (type: {type(contact)})")
        
        # Validate inputs
        if not item_code:
            return jsonify({'error': 'Item code is required'}), 400
        if not from_date or not to_date:
            return jsonify({'error': 'Both from_date and to_date are required'}), 400
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get sales details data (ITEMS table)
        if 'sales_details' not in dataframes or dataframes['sales_details'] is None:
            return jsonify({'error': 'Sales details data not available'}), 400
        
        sales_df = dataframes['sales_details'].copy()
        print(f"📊 Working with {len(sales_df)} sales detail records")
        
        # Filter for SID starting with "411" (office clients)
        if 'SID' in sales_df.columns:
            sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 After SID starts with '411' filter: {len(sales_df)} records")
        else:
            return jsonify({'error': 'SID column not found in sales details'}), 400
        
        # Filter for specific item code
        if 'ITEM' in sales_df.columns:
            sales_df = sales_df[sales_df['ITEM'].astype(str) == str(item_code)]
            print(f"📊 After item filter ({item_code}): {len(sales_df)} records")
        else:
            return jsonify({'error': 'ITEM column not found in sales details'}), 400
        
        # Filter by date range
        if 'FDATE' in sales_df.columns:
            sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
            from_date_dt = pd.to_datetime(from_date)
            to_date_dt = pd.to_datetime(to_date)
            
            sales_df = sales_df[
                (sales_df['FDATE'] >= from_date_dt) & 
                (sales_df['FDATE'] <= to_date_dt)
            ]
            print(f"📊 After date filter ({from_date} to {to_date}): {len(sales_df)} records")
        else:
            return jsonify({'error': 'FDATE column not found in sales details'}), 400
        
        # Filter by SIDNO from ALLSTOCK table if site_sidno is provided
        if site_sidno:
            if 'sites' not in dataframes or dataframes['sites'] is None:
                return jsonify({'error': 'Sites data (ALLSTOCK) not available'}), 400
            
            sites_df = dataframes['sites'].copy()
            
            # Check if SIDNO column exists
            if 'SIDNO' not in sites_df.columns:
                return jsonify({'error': 'SIDNO column not found in sites (ALLSTOCK) table'}), 400
            
            # Convert site_sidno to list if it's a single value
            if not isinstance(site_sidno, list):
                site_sidno = [site_sidno]
            
            # Convert all to strings for comparison
            site_sidno_str = [str(s) for s in site_sidno]
            
            # Filter sites by SIDNO (multiple values)
            filtered_sites = sites_df[sites_df['SIDNO'].astype(str).isin(site_sidno_str)]
            
            if filtered_sites.empty:
                return jsonify({'error': f'No sites found with SIDNO in {site_sidno_str}'}), 404
            
            # Get site IDs (ALLSTOCK.ID) that match any of the SIDNO values
            site_ids = filtered_sites['ID'].unique().tolist()
            print(f"📍 Found {len(site_ids)} sites with SIDNO in {site_sidno_str}")
            
            # Filter sales_df by SITE (ITEMS.SITE = ALLSTOCK.ID)
            if 'SITE' in sales_df.columns:
                # Convert both to string for proper matching
                sales_df['SITE'] = sales_df['SITE'].astype(str)
                site_ids_str = [str(sid) for sid in site_ids]
                sales_df = sales_df[sales_df['SITE'].isin(site_ids_str)]
                print(f"📊 After SIDNO filter ({site_sidno_str}): {len(sales_df)} records")
            else:
                return jsonify({'error': 'SITE column not found in sales details'}), 400
        
        # Filter by CONTACT if provided (via SUB table)
        if contact:
            if 'accounts' not in dataframes or dataframes['accounts'] is None:
                return jsonify({'error': 'Accounts data (SUB table) not available for CONTACT filtering'}), 400
            
            accounts_df = dataframes['accounts'].copy()
            
            # Check if CONTACT column exists in SUB table (case-insensitive)
            contact_col = None
            for col in accounts_df.columns:
                if col.upper() == 'CONTACT':
                    contact_col = col
                    break
            
            if not contact_col:
                return jsonify({'error': 'CONTACT column not found in SUB table (accounts)'}), 400
            
            # Convert contact to list if it's a single value
            if not isinstance(contact, list):
                contact = [contact]
            
            # Filter SUB table by SID starting with '411' first
            accounts_df = accounts_df[accounts_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 Accounts with SID starting with 411: {len(accounts_df)}")
            
            # Try multiple matching strategies
            filtered_accounts = None
            
            # Strategy 1: Try numeric matching (CONTACT might be float/int)
            try:
                contact_values = [float(c) for c in contact]
                # Handle NaN values in CONTACT column
                accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_values)]
                if not filtered_accounts.empty:
                    print(f"📊 Matched {len(filtered_accounts)} accounts using numeric matching")
            except (ValueError, TypeError) as e:
                print(f"⚠️ Numeric matching failed: {e}")
            
            # Strategy 2: If numeric failed or returned empty, try string matching
            if filtered_accounts is None or filtered_accounts.empty:
                try:
                    contact_str = [str(c).strip() for c in contact]
                    accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                    accounts_df_clean[contact_col] = accounts_df_clean[contact_col].astype(str).str.strip()
                    filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_str)]
                    if not filtered_accounts.empty:
                        print(f"📊 Matched {len(filtered_accounts)} accounts using string matching")
                except Exception as e:
                    print(f"⚠️ String matching failed: {e}")
            
            if filtered_accounts is None or filtered_accounts.empty:
                # Debug: show some sample CONTACT values
                sample_contacts = accounts_df[contact_col].dropna().unique()[:10]
                print(f"⚠️ No matches found. Sample CONTACT values in SUB table: {sample_contacts}")
                print(f"⚠️ Looking for CONTACT values: {contact}")
                return jsonify({'error': f'No clients found with CONTACT in {contact}. Please check the contact values.'}), 404
            
            # Get SIDs from filtered accounts
            filtered_sids = filtered_accounts['SID'].astype(str).unique().tolist()
            print(f"📊 Found {len(filtered_sids)} unique clients with CONTACT in {contact}")
            print(f"📊 Sample SIDs: {filtered_sids[:5] if len(filtered_sids) > 5 else filtered_sids}")
            
            # Filter sales_df by these SIDs
            sales_df_before = len(sales_df)
            sales_df = sales_df[sales_df['SID'].astype(str).isin(filtered_sids)]
            print(f"📊 After CONTACT filter ({contact}): {len(sales_df)} records (was {sales_df_before})")
        
        if sales_df.empty:
            return jsonify({'error': 'No sales found for this item in the specified period'}), 404
        
        # Ensure FTYPE and QTY columns exist
        if 'FTYPE' not in sales_df.columns:
            return jsonify({'error': 'FTYPE column not found in sales details'}), 400
        
        # Get quantity column
        qty_col = 'QTY' if 'QTY' in sales_df.columns else ('QTY1' if 'QTY1' in sales_df.columns else None)
        if qty_col is None:
            return jsonify({'error': 'No quantity column (QTY/QTY1) found in sales details'}), 400
        
        # Fill NaN values
        sales_df[qty_col] = sales_df[qty_col].fillna(0)
        
        # Fill NaN values for USD calculation fields
        if 'CREDITUS' in sales_df.columns:
            sales_df['CREDITUS'] = sales_df['CREDITUS'].fillna(0)
        if 'DEBITUS' in sales_df.columns:
            sales_df['DEBITUS'] = sales_df['DEBITUS'].fillna(0)
        if 'CREDITVATAMOUNT' in sales_df.columns:
            sales_df['CREDITVATAMOUNT'] = sales_df['CREDITVATAMOUNT'].fillna(0)
        if 'DEBITVATAMOUNT' in sales_df.columns:
            sales_df['DEBITVATAMOUNT'] = sales_df['DEBITVATAMOUNT'].fillna(0)
        
        # Filter for FTYPE 1 (sales) and FTYPE 2 (returns)
        sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]
        
        # Calculate sales (FTYPE = 1) and returns (FTYPE = 2) by SID (client)
        sales_by_client = sales_df.groupby(['SID', 'FTYPE'])[qty_col].sum().reset_index()
        
        # Separate sales and returns
        sales_only = sales_by_client[sales_by_client['FTYPE'] == 1].copy()
        returns_only = sales_by_client[sales_by_client['FTYPE'] == 2].copy()
        
        # Calculate number of invoices (distinct MID) per client
        invoice_counts = {}
        if 'MID' in sales_df.columns:
            # Count distinct MID per SID
            invoice_counts_df = sales_df.groupby('SID')['MID'].nunique().reset_index()
            invoice_counts_df.columns = ['SID', 'INVOICE_COUNT']
            invoice_counts = dict(zip(invoice_counts_df['SID'].astype(str), invoice_counts_df['INVOICE_COUNT']))
            print(f"📊 Calculated invoice counts for {len(invoice_counts)} clients")
        else:
            print("⚠️ MID column not found - invoice count will be 0")
        
        # Calculate USD amounts per client using CREDITUS-DEBITUS formula (same as sales by item report)
        usd_amounts = {}
        if 'CREDITUS' in sales_df.columns and 'DEBITUS' in sales_df.columns:
            # Calculate base amount: SUM(CREDITUS - DEBITUS) by SID
            sales_df['BASE_AMOUNT'] = sales_df['CREDITUS'] - sales_df['DEBITUS']
            base_amounts = sales_df.groupby('SID')['BASE_AMOUNT'].sum().reset_index()
            base_amounts.columns = ['SID', 'BASE_AMOUNT']
            
            # Calculate VAT amount if available: SUM(CREDITVATAMOUNT - DEBITVATAMOUNT) by SID
            vat_amounts = pd.Series(0, index=sales_df['SID'].unique())
            if 'CREDITVATAMOUNT' in sales_df.columns and 'DEBITVATAMOUNT' in sales_df.columns:
                sales_df['VAT_AMOUNT'] = sales_df['CREDITVATAMOUNT'] - sales_df['DEBITVATAMOUNT']
                vat_amounts_df = sales_df.groupby('SID')['VAT_AMOUNT'].sum().reset_index()
                vat_amounts_df.columns = ['SID', 'VAT_AMOUNT']
                vat_amounts = dict(zip(vat_amounts_df['SID'].astype(str), vat_amounts_df['VAT_AMOUNT']))
            
            # Merge base amounts with VAT amounts
            for _, row in base_amounts.iterrows():
                sid_str = str(row['SID'])
                base_amount = float(row['BASE_AMOUNT'])
                vat_amount = float(vat_amounts.get(sid_str, 0))
                # Final USD amount = BASE_AMOUNT + VAT_AMOUNT (same as sales by item report)
                usd_amounts[sid_str] = base_amount + vat_amount
            
            print(f"📊 Calculated USD amounts for {len(usd_amounts)} clients using CREDITUS-DEBITUS formula")
        else:
            print("⚠️ CREDITUS/DEBITUS columns not found - USD amounts will be 0")
        
        # Create result dataframe
        result_data = []
        all_sids = sales_df['SID'].unique()
        
        for sid in all_sids:
            sid_str = str(sid)
            
            # Get sales quantity (FTYPE = 1)
            sales_qty = sales_only[sales_only['SID'] == sid][qty_col].sum() if not sales_only[sales_only['SID'] == sid].empty else 0
            
            # Get returns quantity (FTYPE = 2)
            returns_qty = returns_only[returns_only['SID'] == sid][qty_col].sum() if not returns_only[returns_only['SID'] == sid].empty else 0
            
            # Calculate total (sales - returns)
            total_qty = sales_qty - returns_qty
            
            # Get number of invoices
            num_invoices = invoice_counts.get(sid_str, 0)
            
            # Get USD amount
            usd_amount = usd_amounts.get(sid_str, 0.0)
            
            result_data.append({
                'SID': sid_str,
                'SALES_QTY': float(sales_qty),
                'RETURNS_QTY': float(returns_qty),
                'TOTAL_QTY': float(total_qty),
                'NUM_INVOICES': int(num_invoices),
                'QUANTITY_USD': float(usd_amount)
            })
        
        # Get client names from SUB table (accounts dataframe)
        client_names = {}
        if 'accounts' in dataframes and dataframes['accounts'] is not None:
            accounts_df = dataframes['accounts'].copy()
            if 'SID' in accounts_df.columns and 'SNAME' in accounts_df.columns:
                # Convert SID to string for matching
                accounts_df['SID'] = accounts_df['SID'].astype(str)
                # Get unique SID-SNAME mappings
                sid_name_map = accounts_df[['SID', 'SNAME']].drop_duplicates()
                client_names = dict(zip(sid_name_map['SID'], sid_name_map['SNAME']))
                print(f"📍 Retrieved client names for {len(client_names)} clients from SUB table")
        
        # Add client names to result data
        for row in result_data:
            sid = row['SID']
            row['CLIENT_NAME'] = client_names.get(sid, f"Client {sid}")
        
        # Get item information
        item_name = f"Item {item_code}"
        if 'inventory_items' in dataframes and dataframes['inventory_items'] is not None:
            items_df = dataframes['inventory_items'][['ITEM', 'DESCR1']].drop_duplicates()
            items_df['ITEM'] = items_df['ITEM'].astype(str)
            item_info = items_df[items_df['ITEM'] == str(item_code)]
            if not item_info.empty:
                item_name = str(item_info.iloc[0]['DESCR1']) if pd.notna(item_info.iloc[0]['DESCR1']) else f"Item {item_code}"
        
        # Sort by total quantity descending
        result_data.sort(key=lambda x: x['TOTAL_QTY'], reverse=True)
        
        # Calculate totals
        total_sales = sum(row['SALES_QTY'] for row in result_data)
        total_returns = sum(row['RETURNS_QTY'] for row in result_data)
        total_net = sum(row['TOTAL_QTY'] for row in result_data)
        total_invoices = sum(row['NUM_INVOICES'] for row in result_data)
        total_usd = sum(row['QUANTITY_USD'] for row in result_data)
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'item_code': str(item_code),
                'item_name': item_name,
                'total_clients': len(result_data),
                'from_date': from_date,
                'to_date': to_date,
                'total_sales_qty': float(total_sales),
                'total_returns_qty': float(total_returns),
                'total_net_qty': float(total_net),
                'total_invoices': int(total_invoices),
                'total_quantity_usd': float(total_usd),
                'filter': f'Item: {item_code}, SID starting with 411 (Office Clients)',
                'data_source': 'ITEMS table (sales_details)',
                'calculation_method': {
                    'sales': 'SUM(QTY) where FTYPE = 1',
                    'returns': 'SUM(QTY) where FTYPE = 2',
                    'total': 'SALES_QTY - RETURNS_QTY',
                    'num_invoices': 'COUNT(DISTINCT MID) per SID',
                    'quantity_usd': 'SUM(CREDITUS - DEBITUS) + SUM(CREDITVATAMOUNT - DEBITVATAMOUNT) per SID (same as sales by item report)'
                },
                'columns_info': {
                    'client_name': 'Client name from SUB.SNAME',
                    'sales_qty': 'Total sales quantity (FTYPE = 1)',
                    'returns_qty': 'Total returns quantity (FTYPE = 2)',
                    'total_qty': 'Net quantity (Sales - Returns)',
                    'num_invoices': 'Number of distinct invoices (MID) for this client',
                    'quantity_usd': 'Total amount in USD: SUM(CREDITUS-DEBITUS) + SUM(CREDITVATAMOUNT-DEBITVATAMOUNT)'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in Kinshasa Bureau Item Clients: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@api_bp.route('/kinshasa-bureau-items-report', methods=['POST'])
def api_kinshasa_bureau_items_report():
    """
    Generate Kinshasa Sales Bureau Top Items Report
    Shows top items by sales/returns for office clients (SID starting with 411)
    """
    try:
        data = request.get_json()
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        top_n = data.get('top_n', 50)  # Default to top 50 items
        site_sidno = data.get('site_sidno')  # Optional: filter by SIDNO from ALLSTOCK table (can be array or single value)
        contact = data.get('contact')  # Optional: filter by CONTACT (can be array or single value)
        
        # Convert top_n to int if it's a string
        try:
            top_n = int(top_n) if top_n else 0
        except (ValueError, TypeError):
            top_n = 50  # Default to 50 if conversion fails
        
        # Validate dates
        if not from_date or not to_date:
            return jsonify({'error': 'Both from_date and to_date are required'}), 400
        
        dataframes = get_dataframes()
        if not dataframes:
            return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400
        
        # Get sales details data (ITEMS table)
        if 'sales_details' not in dataframes or dataframes['sales_details'] is None:
            return jsonify({'error': 'Sales details data not available'}), 400
        
        sales_df = dataframes['sales_details'].copy()
        print(f"📊 Working with {len(sales_df)} sales detail records")
        
        # Filter for SID starting with "411" (office clients)
        if 'SID' in sales_df.columns:
            sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 After SID starts with '411' filter: {len(sales_df)} records")
        else:
            return jsonify({'error': 'SID column not found in sales details'}), 400
        
        # Filter by SIDNO from ALLSTOCK table if site_sidno is provided
        # Join: ITEMS.SITE = ALLSTOCK.ID, then filter by ALLSTOCK.SIDNO
        # site_sidno can be a single value or an array
        if site_sidno:
            if 'sites' not in dataframes or dataframes['sites'] is None:
                return jsonify({'error': 'Sites data (ALLSTOCK) not available'}), 400
            
            sites_df = dataframes['sites'].copy()
            
            # Check if SIDNO column exists
            if 'SIDNO' not in sites_df.columns:
                return jsonify({'error': 'SIDNO column not found in sites (ALLSTOCK) table'}), 400
            
            # Convert site_sidno to list if it's a single value
            if not isinstance(site_sidno, list):
                site_sidno = [site_sidno]
            
            # Convert all to strings for comparison
            site_sidno_str = [str(s) for s in site_sidno]
            
            # Filter sites by SIDNO (multiple values)
            filtered_sites = sites_df[sites_df['SIDNO'].astype(str).isin(site_sidno_str)]
            
            if filtered_sites.empty:
                return jsonify({'error': f'No sites found with SIDNO in {site_sidno_str}'}), 404
            
            # Get site IDs (ALLSTOCK.ID) that match any of the SIDNO values
            site_ids = filtered_sites['ID'].unique().tolist()
            print(f"📍 Found {len(site_ids)} sites with SIDNO in {site_sidno_str}")
            
            # Filter sales_df by SITE (ITEMS.SITE = ALLSTOCK.ID)
            if 'SITE' in sales_df.columns:
                # Convert both to string for proper matching
                sales_df['SITE'] = sales_df['SITE'].astype(str)
                site_ids_str = [str(sid) for sid in site_ids]
                sales_df = sales_df[sales_df['SITE'].isin(site_ids_str)]
                print(f"📊 After SIDNO filter ({site_sidno_str}): {len(sales_df)} records")
            else:
                return jsonify({'error': 'SITE column not found in sales details'}), 400
        
        # Filter by date range
        if 'FDATE' in sales_df.columns:
            sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
            from_date_dt = pd.to_datetime(from_date)
            to_date_dt = pd.to_datetime(to_date)
            
            sales_df = sales_df[
                (sales_df['FDATE'] >= from_date_dt) & 
                (sales_df['FDATE'] <= to_date_dt)
            ]
            print(f"📊 After date filter ({from_date} to {to_date}): {len(sales_df)} records")
        else:
            return jsonify({'error': 'FDATE column not found in sales details'}), 400
        
        # Filter by CONTACT if provided (via SUB table)
        if contact:
            if 'accounts' not in dataframes or dataframes['accounts'] is None:
                return jsonify({'error': 'Accounts data (SUB table) not available for CONTACT filtering'}), 400
            
            accounts_df = dataframes['accounts'].copy()
            
            # Check if CONTACT column exists in SUB table (case-insensitive)
            contact_col = None
            for col in accounts_df.columns:
                if col.upper() == 'CONTACT':
                    contact_col = col
                    break
            
            if not contact_col:
                return jsonify({'error': 'CONTACT column not found in SUB table (accounts)'}), 400
            
            # Convert contact to list if it's a single value
            if not isinstance(contact, list):
                contact = [contact]
            
            # Filter SUB table by SID starting with '411' first
            accounts_df = accounts_df[accounts_df['SID'].astype(str).str.startswith('411')]
            print(f"📊 Accounts with SID starting with 411: {len(accounts_df)}")
            
            # Try multiple matching strategies
            filtered_accounts = None
            
            # Strategy 1: Try numeric matching (CONTACT might be float/int)
            try:
                contact_values = [float(c) for c in contact]
                # Handle NaN values in CONTACT column
                accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_values)]
                if not filtered_accounts.empty:
                    print(f"📊 Matched {len(filtered_accounts)} accounts using numeric matching")
            except (ValueError, TypeError) as e:
                print(f"⚠️ Numeric matching failed: {e}")
            
            # Strategy 2: If numeric failed or returned empty, try string matching
            if filtered_accounts is None or filtered_accounts.empty:
                try:
                    contact_str = [str(c).strip() for c in contact]
                    accounts_df_clean = accounts_df[accounts_df[contact_col].notna()]
                    accounts_df_clean[contact_col] = accounts_df_clean[contact_col].astype(str).str.strip()
                    filtered_accounts = accounts_df_clean[accounts_df_clean[contact_col].isin(contact_str)]
                    if not filtered_accounts.empty:
                        print(f"📊 Matched {len(filtered_accounts)} accounts using string matching")
                except Exception as e:
                    print(f"⚠️ String matching failed: {e}")
            
            if filtered_accounts is None or filtered_accounts.empty:
                # Debug: show some sample CONTACT values
                sample_contacts = accounts_df[contact_col].dropna().unique()[:10]
                print(f"⚠️ No matches found. Sample CONTACT values in SUB table: {sample_contacts}")
                print(f"⚠️ Looking for CONTACT values: {contact}")
                return jsonify({'error': f'No clients found with CONTACT in {contact}. Please check the contact values.'}), 404
            
            # Get SIDs from filtered accounts
            filtered_sids = filtered_accounts['SID'].astype(str).unique().tolist()
            print(f"📊 Found {len(filtered_sids)} unique clients with CONTACT in {contact}")
            print(f"📊 Sample SIDs: {filtered_sids[:5] if len(filtered_sids) > 5 else filtered_sids}")
            
            # Filter sales_df by these SIDs
            sales_df_before = len(sales_df)
            sales_df = sales_df[sales_df['SID'].astype(str).isin(filtered_sids)]
            print(f"📊 After CONTACT filter ({contact}): {len(sales_df)} records (was {sales_df_before})")
        
        if sales_df.empty:
            return jsonify({'error': 'No sales found for the specified criteria'}), 404
        
        # Ensure FTYPE and QTY columns exist
        if 'FTYPE' not in sales_df.columns:
            return jsonify({'error': 'FTYPE column not found in sales details'}), 400
        
        # Get quantity column
        qty_col = 'QTY' if 'QTY' in sales_df.columns else ('QTY1' if 'QTY1' in sales_df.columns else None)
        if qty_col is None:
            return jsonify({'error': 'No quantity column (QTY/QTY1) found in sales details'}), 400
        
        # Fill NaN values
        sales_df[qty_col] = sales_df[qty_col].fillna(0)
        
        # Filter for FTYPE 1 (sales) and FTYPE 2 (returns)
        sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]
        
        # Calculate sales (FTYPE = 1) and returns (FTYPE = 2) by ITEM and SID (client)
        sales_by_item_client = sales_df.groupby(['ITEM', 'SID', 'FTYPE'])[qty_col].sum().reset_index()
        
        # Separate sales and returns
        sales_only = sales_by_item_client[sales_by_item_client['FTYPE'] == 1].copy()
        returns_only = sales_by_item_client[sales_by_item_client['FTYPE'] == 2].copy()
        
        # Get all unique item-client combinations
        all_combinations = sales_df[['ITEM', 'SID']].drop_duplicates()
        
        # Create result dataframe
        result_data = []
        
        for _, combo in all_combinations.iterrows():
            item_code = combo['ITEM']
            sid = combo['SID']
            
            # Get sales quantity (FTYPE = 1) for this item-client combination
            sales_qty = sales_only[(sales_only['ITEM'] == item_code) & (sales_only['SID'] == sid)][qty_col].sum() if not sales_only[(sales_only['ITEM'] == item_code) & (sales_only['SID'] == sid)].empty else 0
            
            # Get returns quantity (FTYPE = 2) for this item-client combination
            returns_qty = returns_only[(returns_only['ITEM'] == item_code) & (returns_only['SID'] == sid)][qty_col].sum() if not returns_only[(returns_only['ITEM'] == item_code) & (returns_only['SID'] == sid)].empty else 0
            
            # Calculate total (sales - returns)
            total_qty = sales_qty - returns_qty
            
            # Only include if there's actual sales/returns activity
            if sales_qty != 0 or returns_qty != 0:
                result_data.append({
                    'ITEM_CODE': str(item_code),
                    'SID': str(sid),
                    'SALES_QTY': float(sales_qty),
                    'RETURNS_QTY': float(returns_qty),
                    'TOTAL_QTY': float(total_qty)
                })
        
        # Convert to DataFrame for easier processing
        result_df = pd.DataFrame(result_data)
        
        # Get item information (names, categories, weight) from inventory_items
        if 'inventory_items' not in dataframes or dataframes['inventory_items'] is None:
            return jsonify({'error': 'Inventory items data not available'}), 400
        
        # Include NWEIGHT column for weight calculation
        items_df = dataframes['inventory_items'][['ITEM', 'DESCR1', 'CATEGORY', 'NWEIGHT']].drop_duplicates()
        items_df.columns = ['ITEM_CODE', 'ITEM_NAME', 'CATEGORY_ID', 'NWEIGHT']
        items_df['ITEM_CODE'] = items_df['ITEM_CODE'].astype(str)
        result_df['ITEM_CODE'] = result_df['ITEM_CODE'].astype(str)
        
        # Merge with item information
        result_df = result_df.merge(items_df, on='ITEM_CODE', how='left')
        result_df['ITEM_NAME'] = result_df['ITEM_NAME'].fillna('Unknown Item')
        result_df['CATEGORY_ID'] = result_df['CATEGORY_ID'].fillna('')
        
        # Fill NaN values for NWEIGHT and calculate weight (NWEIGHT * TOTAL_QTY), then convert to tons (divide by 1000)
        result_df['NWEIGHT'] = result_df['NWEIGHT'].fillna(0)
        result_df['WEIGHT'] = (result_df['NWEIGHT'] * result_df['TOTAL_QTY']) / 1000  # Convert to tons
        
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
        
        # Get client names from SUB table (accounts dataframe)
        if 'accounts' in dataframes and dataframes['accounts'] is not None:
            accounts_df = dataframes['accounts'].copy()
            if 'SID' in accounts_df.columns and 'SNAME' in accounts_df.columns:
                # Convert SID to string for matching
                accounts_df['SID'] = accounts_df['SID'].astype(str)
                result_df['SID'] = result_df['SID'].astype(str)
                # Get unique SID-SNAME mappings
                sid_name_map = accounts_df[['SID', 'SNAME']].drop_duplicates()
                result_df = result_df.merge(sid_name_map, on='SID', how='left')
                result_df['CLIENT_NAME'] = result_df['SNAME'].fillna(result_df['SID'])
                print(f"📍 Retrieved client names for {len(sid_name_map)} clients from SUB table")
            else:
                result_df['CLIENT_NAME'] = result_df['SID']
        else:
            result_df['CLIENT_NAME'] = result_df['SID']
        
        # Filter out rows with no sales
        result_df = result_df[result_df['TOTAL_QTY'] != 0]
        
        # Sort by total quantity descending to get top item-client combinations
        result_df = result_df.sort_values('TOTAL_QTY', ascending=False)
        
        # Limit to top N item-client combinations (top_n is already converted to int at the beginning)
        if top_n > 0:
            result_df = result_df.head(top_n)
        
        # Convert to list of dictionaries for JSON response
        result_data = []
        for _, row in result_df.iterrows():
            row_data = {
                'ITEM_CODE': str(row['ITEM_CODE']),
                'ITEM_NAME': str(row['ITEM_NAME']),
                'CATEGORY': str(row['CATEGORY_NAME']),
                'SID': str(row['SID']),
                'CLIENT_NAME': str(row['CLIENT_NAME']),
                'SALES_QTY': float(row['SALES_QTY']),
                'RETURNS_QTY': float(row['RETURNS_QTY']),
                'TOTAL_QTY': float(row['TOTAL_QTY']),
                'WEIGHT': float(row['WEIGHT'])
            }
            result_data.append(row_data)
        
        # Calculate totals
        total_sales = result_df['SALES_QTY'].sum()
        total_returns = result_df['RETURNS_QTY'].sum()
        total_net = result_df['TOTAL_QTY'].sum()
        total_weight = result_df['WEIGHT'].sum()
        
        return jsonify({
            'data': result_data,
            'metadata': {
                'total_items': len(result_data),
                'top_n': int(top_n) if top_n and str(top_n) != '0' else 'all',
                'from_date': from_date,
                'to_date': to_date,
                'total_sales_qty': float(total_sales),
                'total_returns_qty': float(total_returns),
                'total_net_qty': float(total_net),
                'total_weight': float(total_weight),
                'site_sidno': site_sidno,
                'contact': contact if contact else None,
                'filter': 'SID starting with 411 (Office Clients)' + (f', SIDNO in {site_sidno if isinstance(site_sidno, list) else [site_sidno]}' if site_sidno else '') + (f', CONTACT in {contact if isinstance(contact, list) else [contact]}' if contact else ''),
                'data_source': 'ITEMS table (sales_details)',
                'calculation_method': {
                    'sales': 'SUM(QTY) where FTYPE = 1',
                    'returns': 'SUM(QTY) where FTYPE = 2',
                    'total': 'SALES_QTY - RETURNS_QTY',
                    'weight': 'NWEIGHT (from STOCK table) × TOTAL_QTY / 1000 (in tons)',
                    'sorting': 'Sorted by TOTAL_QTY descending (top item-client combinations)'
                },
                'columns_info': {
                    'item_code': 'Item code',
                    'item_name': 'Item description (DESCR1)',
                    'category': 'Category name from categories.DESCR',
                    'sid': 'Client SID (from ITEMS.SID)',
                    'client_name': 'Client name from SUB.SNAME',
                    'weight': 'Total weight in tons = (NWEIGHT × TOTAL_QTY) / 1000 (from STOCK.NWEIGHT)',
                    'sales_qty': 'Total sales quantity (FTYPE = 1) for this item-client combination',
                    'returns_qty': 'Total returns quantity (FTYPE = 2) for this item-client combination',
                    'total_qty': 'Net quantity (Sales - Returns) for this item-client combination'
                }
            }
        })
        
    except Exception as e:
        print(f"Error in Kinshasa Bureau Items report: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500