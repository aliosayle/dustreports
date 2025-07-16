"""API routes for data operations"""

from flask import Blueprint, request, jsonify
import pandas as pd
from services.database_service import (
    load_dataframes, get_dataframes, is_cache_loading, get_cache_lock
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
    return jsonify({
        'cached': len(dataframes) > 0,
        'loading': is_cache_loading(),
        'tables': list(dataframes.keys()) if dataframes else []
    })

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
            return jsonify([])
        
        # Filter out categories with 0 items
        if 'inventory_items' in dataframes and dataframes['inventory_items'] is not None:
            categories_with_items = dataframes['inventory_items']['CATEGORY'].dropna().unique()
            
            categories = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            categories = categories[categories['ID'].isin(categories_with_items)]
            
            categories_list = [{'id': row['ID'], 'name': row['DESCR'] or ''} for _, row in categories.iterrows()]
            categories_list.sort(key=lambda x: x['name'])
            
            print(f"📋 Returning {len(categories_list)} categories with items")
            return jsonify(categories_list)
        else:
            categories = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            categories_list = [{'id': row['ID'], 'name': row['DESCR'] or ''} for _, row in categories.iterrows()]
            categories_list.sort(key=lambda x: x['name'])
            return jsonify(categories_list)
    except Exception as e:
        print(f"Error in api_categories: {e}")
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
        
        # Sort by site name
        result_data.sort(key=lambda x: x['SITE_NAME'])
        
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
