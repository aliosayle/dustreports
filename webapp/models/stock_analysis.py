"""Stock analysis and calculation models - REWRITTEN FROM SCRATCH"""

import pandas as pd
from services.database_service import get_dataframes


class StockAnalyzer:
    """Simplified stock analysis using exact notebook logic"""
    
    def __init__(self):
        self.dataframes = get_dataframes()
    
    def calculate_stock_and_sales(self, item_code=None, site_code=None, from_date=None, 
                                 to_date=None, as_of_date=None, site_codes=None, category_id=None):
        """
        Calculate stock and sales using EXACT notebook logic - OPTIMIZED
        """
        print(f"\n🔍 OPTIMIZED calculate_stock_and_sales:")
        print(f"   📊 Parameters: item_code={item_code}, site_code={site_code}, from_date={from_date}, to_date={to_date}, as_of_date={as_of_date}")
        
        if not self.dataframes:
            print("   ❌ No dataframes available")
            return None
        
        # Get required data
        inventory_df = self.dataframes.get('inventory_transactions')
        sales_df = self.dataframes.get('sales_details')
        items_master = self.dataframes.get('inventory_items')
        sites_master = self.dataframes.get('sites')
        categories_master = self.dataframes.get('categories')
        
        if inventory_df is None or sales_df is None:
            print("   ❌ Required data not available")
            return None
        
        print(f"   📊 Data available: inventory={len(inventory_df)}, sales={len(sales_df)}")
        
        # Calculate stock first
        stock_results = self._calculate_stock_simple(inventory_df, item_code, site_code, site_codes, as_of_date)
        
        if stock_results.empty:
            print("   ❌ No stock data found")
            return None
        
        print(f"   📊 Stock results: {len(stock_results)} items")
        
        # 🚀 PERFORMANCE OPTIMIZATION: Calculate ALL sales at once instead of item by item
        print(f"   🚀 Calculating sales for all items at once...")
        
        # For stock by site reports, if as_of_date is provided but from_date/to_date are not,
        # we need to calculate sales for a reasonable period (e.g., last 30 days)
        if as_of_date and (from_date is None or to_date is None):
            # Use last 30 days from as_of_date for sales calculation
            as_of_dt = pd.to_datetime(as_of_date)
            from_date = (as_of_dt - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
            to_date = as_of_date
            print(f"   📅 Using sales period: {from_date} to {to_date} (30 days from as_of_date)")
        
        # Use sales details with FTYPE logic (1=sale, 2=return -> subtract)
        all_sales = self._calculate_all_sales_optimized(stock_results, from_date, to_date)
        
        # Merge sales with stock results
        stock_results = stock_results.merge(all_sales, on=['SITE', 'ITEM'], how='left')
        
        # Fill missing sales values (all columns from optimized calculation)
        stock_results['TOTAL_SALES_QTY'] = stock_results['TOTAL_SALES_QTY'].fillna(0)
        stock_results['SALES_TRANSACTIONS'] = stock_results['SALES_TRANSACTIONS'].fillna(0)
        stock_results['MAX_DAILY_SALES'] = stock_results['MAX_DAILY_SALES'].fillna(0)
        stock_results['MIN_DAILY_SALES'] = stock_results['MIN_DAILY_SALES'].fillna(0)
        
        # Calculate period days
        period_days = self._calculate_period_days(from_date, to_date)
        stock_results['SALES_PERIOD_DAYS'] = period_days
        
        # Calculate average daily sales: total sales ÷ number of days
        stock_results['AVG_DAILY_SALES'] = stock_results['TOTAL_SALES_QTY'] / period_days if period_days > 0 else 0
        
        # Calculate autonomy using average daily sales
        def calculate_autonomy(row):
            current_stock = row['CURRENT_STOCK']
            avg_daily_sales = row['AVG_DAILY_SALES']
            if current_stock <= 0:
                return -1  # No stock
            elif avg_daily_sales > 0:
                return current_stock / avg_daily_sales
            else:
                return 9999  # Stock but no sales
        
        stock_results['STOCK_AUTONOMY_DAYS'] = stock_results.apply(calculate_autonomy, axis=1)
        
        # Add master data (with proper categories, prices, and depot quantities)
        stock_results = self._add_master_data_optimized(stock_results, items_master, sites_master, categories_master)
        
        print(f"   🎯 Final results: {len(stock_results)} items with sales calculated")
        
        return stock_results
    
    def _calculate_stock_simple(self, inventory_df, item_code, site_code, site_codes, as_of_date):
        """Simple stock calculation using DEBITQTY and CREDITQTY like stock_by_site report"""
        
        print(f"   📊 Calculating stock...")
        print(f"   📊 Available columns: {list(inventory_df.columns)}")
        
        # Apply basic filters
        df = inventory_df.copy()
        
        if item_code:
            df = df[df['ITEM'] == item_code]
        if site_code:
            df = df[df['SITE'] == site_code]
        if site_codes and isinstance(site_codes, list):
            df = df[df['SITE'].isin(site_codes)]
        
        if df.empty:
            return pd.DataFrame()
        
        print(f"   📊 After filters: {len(df)} inventory records")
        
        # Fill NaN values with 0 for calculations (same as stock_by_site)
        df['DEBITQTY'] = df['DEBITQTY'].fillna(0)
        df['CREDITQTY'] = df['CREDITQTY'].fillna(0)
        
        # Group by SITE and ITEM, calculate stock as DEBITQTY - CREDITQTY (same as stock_by_site)
        stock_summary = df.groupby(['SITE', 'ITEM']).agg({
            'DEBITQTY': 'sum',
            'CREDITQTY': 'sum'
        }).reset_index()
        
        # Calculate current stock: DEBITQTY (incoming) - CREDITQTY (outgoing)
        stock_summary['CURRENT_STOCK'] = stock_summary['DEBITQTY'] - stock_summary['CREDITQTY']
        
        # Add additional columns for compatibility
        stock_summary['TOTAL_IN'] = stock_summary['DEBITQTY']
        stock_summary['TOTAL_OUT'] = stock_summary['CREDITQTY']
        
        # Drop the temporary columns
        stock_summary = stock_summary[['SITE', 'ITEM', 'CURRENT_STOCK', 'TOTAL_IN', 'TOTAL_OUT']]
        
        print(f"   📊 Stock summary: {len(stock_summary)} unique item/site combinations")
        print(f"   📊 Sample stock calculations:")
        for idx, row in stock_summary.head(3).iterrows():
            print(f"      SITE={row['SITE']}, ITEM={row['ITEM']}, IN={row['TOTAL_IN']}, OUT={row['TOTAL_OUT']}, STOCK={row['CURRENT_STOCK']}")
        
        return stock_summary
    
    def _calculate_period_days(self, from_date, to_date):
        """Calculate period days"""
        if from_date and to_date:
            from_dt = pd.to_datetime(from_date)
            to_dt = pd.to_datetime(to_date)
            return (to_dt - from_dt).days + 1
        return 1
    
    def _calculate_all_sales_optimized(self, stock_results, from_date, to_date):
        """Calculate sales using sales_details with FTYPE handling (1=sale, 2=return -> subtract)."""
        
        print(f"   📊 Optimized sales calculation for {len(stock_results)} items...")
        print(f"   📊 Using sales_details with FTYPE (1 sale, 2 return) for sales calculation")
        
        # Get unique site/item combinations from stock results
        stock_items = stock_results[['SITE', 'ITEM']].copy()
        
        # Handle None date parameters by using a default range
        if from_date is None or to_date is None:
            # Use last 30 days as default
            to_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            from_date = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime('%Y-%m-%d')
            print(f"   📅 Using default date range: {from_date} to {to_date}")
        
        sales_df = self.dataframes.get('sales_details')
        
        if sales_df is None:
            print("   ❌ sales_details not available; returning zeros")
            stock_items['TOTAL_SALES_QTY'] = 0
            stock_items['SALES_TRANSACTIONS'] = 0
            stock_items['MAX_DAILY_SALES'] = 0
            stock_items['MIN_DAILY_SALES'] = 0
            return stock_items
        
        df = sales_df.copy()
        
        # No fallback joins; rely strictly on sales_details content
        
        # Ensure required columns exist (strict: must have ITEM, SITE, FTYPE)
        if 'ITEM' not in df.columns or 'SITE' not in df.columns or 'FTYPE' not in df.columns:
            print("   ❌ Required columns missing in sales_details (need ITEM, SITE, FTYPE); returning zeros")
            stock_items['TOTAL_SALES_QTY'] = 0
            stock_items['SALES_TRANSACTIONS'] = 0
            stock_items['MAX_DAILY_SALES'] = 0
            stock_items['MIN_DAILY_SALES'] = 0
            return stock_items
        
        # Date filter
        if 'FDATE' in df.columns:
            try:
                df['FDATE'] = pd.to_datetime(df['FDATE'], errors='coerce')
                df = df[(df['FDATE'] >= pd.to_datetime(from_date)) & (df['FDATE'] <= pd.to_datetime(to_date))]
            except Exception as e:
                print(f"   ⚠️ Error parsing FDATE for sales filter: {e}")
        else:
            print("   ⚠️ No FDATE available in sales data; skipping date filter")
        
        if df.empty:
            stock_items['TOTAL_SALES_QTY'] = 0
            stock_items['SALES_TRANSACTIONS'] = 0
            stock_items['MAX_DAILY_SALES'] = 0
            stock_items['MIN_DAILY_SALES'] = 0
            return stock_items
        
        # Quantity column
        qty_col = 'QTY' if 'QTY' in df.columns else ('QTY1' if 'QTY1' in df.columns else None)
        if qty_col is None:
            print("   ❌ No quantity column (QTY/QTY1) in sales_details; returning zeros")
            stock_items['TOTAL_SALES_QTY'] = 0
            stock_items['SALES_TRANSACTIONS'] = 0
            stock_items['MAX_DAILY_SALES'] = 0
            stock_items['MIN_DAILY_SALES'] = 0
            return stock_items
        df[qty_col] = df[qty_col].fillna(0)
        
        # Apply FTYPE logic strictly: include only 1 or 2; 1 = +, 2 = -
        df = df[df['FTYPE'].isin([1, 2])]
        df['SIGNED_QTY'] = df.apply(lambda r: r[qty_col] if r['FTYPE'] == 1 else -r[qty_col], axis=1)
        
        # Compute daily sales when dates are available
        if 'FDATE' in df.columns:
            df['FDATE_ONLY'] = df['FDATE'].dt.date
            daily_sales = df.groupby(['SITE', 'ITEM', 'FDATE_ONLY']).agg({'SIGNED_QTY': 'sum'}).reset_index()
            daily_stats = daily_sales.groupby(['SITE', 'ITEM']).agg({'SIGNED_QTY': ['max', 'min']}).reset_index()
            daily_stats.columns = ['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES']
        else:
            daily_stats = pd.DataFrame(columns=['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES'])
        
        # Totals and transaction counts
        sales_summary = df.groupby(['SITE', 'ITEM']).agg({
            'SIGNED_QTY': 'sum',
            qty_col: 'count'
        }).reset_index()
        sales_summary.rename(columns={'SIGNED_QTY': 'TOTAL_SALES_QTY', qty_col: 'SALES_TRANSACTIONS'}, inplace=True)
        
        # Merge stats
        sales_summary = sales_summary.merge(daily_stats, on=['SITE', 'ITEM'], how='left')
        sales_summary['MAX_DAILY_SALES'] = sales_summary['MAX_DAILY_SALES'].fillna(0)
        sales_summary['MIN_DAILY_SALES'] = sales_summary['MIN_DAILY_SALES'].fillna(0)
        
        # Merge with stock items to include items with no sales
        result = stock_items.merge(sales_summary, on=['SITE', 'ITEM'], how='left')
        
        print(f"   📊 Sales calculated (FTYPE-aware): {len(sales_summary)} items have sales, {len(result)} total items")
        
        # Debug for F858
        f858_sales = result[(result['ITEM'] == 'F858') & (result['TOTAL_SALES_QTY'] > 0)]
        if not f858_sales.empty:
            for idx, row in f858_sales.iterrows():
                print(f"   🔍 F858 FOUND: SITE={row['SITE']}, SALES={row['TOTAL_SALES_QTY']}, MAX_DAILY={row['MAX_DAILY_SALES']}")
        
        return result
    
    def _add_master_data_optimized(self, results_df, items_master, sites_master, categories_master):
        """Add item names, site names, categories, prices, and depot quantities"""
        
        # Add item names, categories, and prices
        if items_master is not None:
            items_subset = items_master[['ITEM', 'DESCR1', 'CATEGORY', 'POSPRICE1', 'SUNIT']].drop_duplicates()
            results_df = results_df.merge(items_subset, on='ITEM', how='left')
            results_df.rename(columns={'DESCR1': 'ITEM_NAME'}, inplace=True)
        else:
            results_df['ITEM_NAME'] = 'Unknown Item'
            results_df['CATEGORY'] = ''
            results_df['POSPRICE1'] = 0
            results_df['SUNIT'] = ''
        
        # Add category names from categories master
        if categories_master is not None and 'CATEGORY' in results_df.columns:
            categories_subset = categories_master[['ID', 'DESCR']].drop_duplicates()
            categories_subset.rename(columns={'ID': 'CATEGORY', 'DESCR': 'CATEGORY_NAME'}, inplace=True)
            
            # Convert to string for proper matching
            results_df['CATEGORY'] = results_df['CATEGORY'].astype(str)
            categories_subset['CATEGORY'] = categories_subset['CATEGORY'].astype(str)
            
            results_df = results_df.merge(categories_subset, on='CATEGORY', how='left')
        else:
            results_df['CATEGORY_NAME'] = 'General'
        
        # Add site names  
        if sites_master is not None and 'SITE' in sites_master.columns:
            sites_subset = sites_master[['ID', 'SITE']].drop_duplicates()
            results_df = results_df.merge(sites_subset, left_on='SITE', right_on='ID', how='left')
            results_df.rename(columns={'SITE_y': 'SITE_NAME'}, inplace=True)
            results_df.rename(columns={'SITE_x': 'SITE'}, inplace=True)
            results_df.drop('ID', axis=1, inplace=True, errors='ignore')
        else:
            results_df['SITE_NAME'] = results_df['SITE']
        
        # Fill missing values
        results_df['ITEM_NAME'] = results_df['ITEM_NAME'].fillna('Unknown Item')
        results_df['SITE_NAME'] = results_df['SITE_NAME'].fillna(results_df['SITE'])
        results_df['CATEGORY_NAME'] = results_df['CATEGORY_NAME'].fillna('General')
        results_df['POSPRICE1'] = results_df['POSPRICE1'].fillna(0)
        results_df['SUNIT'] = results_df['SUNIT'].fillna('')
        
        # Calculate stock value (current stock × price)
        results_df['STOCK_VALUE'] = results_df['CURRENT_STOCK'] * results_df['POSPRICE1']
        
        # Calculate depot quantities using the same approach as notebook
        if sites_master is not None and 'SIDNO' in sites_master.columns:
            try:
                # Get depot sites where SIDNO = '3700004' (string value)
                depot_sites_info = sites_master[sites_master['SIDNO'] == '3700004'].copy()
                
                if not depot_sites_info.empty:
                    depot_site_ids = depot_sites_info['ID'].tolist()
                    
                    # Get inventory transactions for depot sites only
                    inventory_df = self.dataframes.get('inventory_transactions')
                    if inventory_df is not None:
                        df_depot = inventory_df[inventory_df['SITE'].isin(depot_site_ids)].copy()
                        
                        if not df_depot.empty:
                            # Fill NaN values and calculate depot quantities by item
                            df_depot['DEBITQTY'] = df_depot['DEBITQTY'].fillna(0)
                            df_depot['CREDITQTY'] = df_depot['CREDITQTY'].fillna(0)
                            
                            # Calculate depot quantities by item (sum across all depot sites)
                            depot_qty = df_depot.groupby('ITEM').agg({
                                'DEBITQTY': 'sum',
                                'CREDITQTY': 'sum'
                            }).reset_index()
                            
                            depot_qty['DEPOT_QUANTITY'] = depot_qty['DEBITQTY'] - depot_qty['CREDITQTY']
                            
                            # Create dictionary for easy lookup
                            depot_dict = dict(zip(depot_qty['ITEM'], depot_qty['DEPOT_QUANTITY']))
                            
                            # Add depot quantity column to results_df
                            results_df['DEPOT_QUANTITY'] = results_df['ITEM'].map(depot_dict).fillna(0)
                        else:
                            results_df['DEPOT_QUANTITY'] = 0
                    else:
                        results_df['DEPOT_QUANTITY'] = 0
                else:
                    results_df['DEPOT_QUANTITY'] = 0
            except Exception as e:
                print(f"   ⚠️ Error calculating depot quantities: {e}")
                results_df['DEPOT_QUANTITY'] = 0
        else:
            results_df['DEPOT_QUANTITY'] = 0
        
        # Count stock transactions (from inventory transactions)
        try:
            inventory_df = self.dataframes.get('inventory_transactions')
            if inventory_df is not None:
                # Count transactions for each SITE/ITEM combination
                transaction_counts = inventory_df.groupby(['SITE', 'ITEM']).size().reset_index(name='STOCK_TRANSACTIONS')
                results_df = results_df.merge(transaction_counts, on=['SITE', 'ITEM'], how='left')
                results_df['STOCK_TRANSACTIONS'] = results_df['STOCK_TRANSACTIONS'].fillna(0)
            else:
                results_df['STOCK_TRANSACTIONS'] = 0
        except Exception as e:
            print(f"   ⚠️ Error counting transactions: {e}")
            results_df['STOCK_TRANSACTIONS'] = 0
        
        return results_df
