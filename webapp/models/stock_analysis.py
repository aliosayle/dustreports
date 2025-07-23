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
        print(f"   📊 Parameters: item_code={item_code}, site_code={site_code}, from_date={from_date}, to_date={to_date}")
        
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
        all_sales = self._calculate_all_sales_optimized(sales_df, stock_results, from_date, to_date)
        
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
        
        # Calculate depot quantity: Only for depot sites (SIDNO = "3700004")
        def calculate_depot_quantity(row):
            # Check if site is a depot by looking up SIDNO
            site_id = row['SITE']
            is_depot = False
            
            # Check if this site is a depot (SIDNO = "3700004")
            if sites_master is not None and 'SIDNO' in sites_master.columns:
                depot_sites = sites_master[sites_master['SIDNO'] == '3700004']
                is_depot = site_id in depot_sites['ID'].values
            
            if not is_depot:
                return 0  # Not a depot site
                
            # For depot sites: (7 × Average Daily Sales) - Current Stock
            avg_daily_sales = row['AVG_DAILY_SALES']
            current_stock = row['CURRENT_STOCK']
            target_stock = 7 * avg_daily_sales  # 7 days of stock
            depot_needed = target_stock - current_stock
            return max(0, depot_needed)  # 0 if already sufficient
        
        stock_results['DEPOT_QUANTITY'] = stock_results.apply(calculate_depot_quantity, axis=1)
        
        # Add master data (with proper categories)
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
    
    def _calculate_all_sales_optimized(self, sales_df, stock_results, from_date, to_date):
        """Calculate sales for all items at once - MUCH FASTER with daily stats"""
        
        print(f"   📊 Optimized sales calculation for {len(stock_results)} items...")
        
        # Get unique site/item combinations from stock results
        stock_items = stock_results[['SITE', 'ITEM']].copy()
        
        # Filter sales data for the date range and FTYPE first
        sales_filtered = sales_df[
            (sales_df['FTYPE'].isin([1, 2])) &
            (pd.to_datetime(sales_df['FDATE']) >= pd.to_datetime(from_date)) &
            (pd.to_datetime(sales_df['FDATE']) <= pd.to_datetime(to_date))
        ].copy()
        
        print(f"   📊 Filtered sales to {len(sales_filtered)} records for date range")
        
        if sales_filtered.empty:
            # Return zeros for all items
            stock_items['TOTAL_SALES_QTY'] = 0
            stock_items['SALES_TRANSACTIONS'] = 0
            stock_items['MAX_DAILY_SALES'] = 0
            stock_items['MIN_DAILY_SALES'] = 0
            return stock_items
        
        # Fill NaN values
        sales_filtered['CREDITQTY'] = sales_filtered['CREDITQTY'].fillna(0)
        sales_filtered['DEBITQTY'] = sales_filtered['DEBITQTY'].fillna(0)
        sales_filtered['NET_SALES'] = sales_filtered['CREDITQTY'] - sales_filtered['DEBITQTY']
        
        # Group by SITE, ITEM, and DATE to calculate daily sales
        sales_filtered['FDATE'] = pd.to_datetime(sales_filtered['FDATE']).dt.date
        daily_sales = sales_filtered.groupby(['SITE', 'ITEM', 'FDATE']).agg({
            'NET_SALES': 'sum'
        }).reset_index()
        
        # Calculate total sales and transaction count
        sales_summary = sales_filtered.groupby(['SITE', 'ITEM']).agg({
            'NET_SALES': 'sum',
            'FDATE': 'count'  # Count transactions using a different column
        }).reset_index()
        sales_summary.rename(columns={'NET_SALES': 'TOTAL_SALES_QTY', 'FDATE': 'SALES_TRANSACTIONS'}, inplace=True)
        
        # Calculate max and min daily sales
        daily_stats = daily_sales.groupby(['SITE', 'ITEM']).agg({
            'NET_SALES': ['max', 'min']
        }).reset_index()
        daily_stats.columns = ['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES']
        
        # Merge all sales data
        sales_summary = sales_summary.merge(daily_stats, on=['SITE', 'ITEM'], how='left')
        
        # Fill missing values for items with no daily variation
        sales_summary['MAX_DAILY_SALES'] = sales_summary['MAX_DAILY_SALES'].fillna(0)
        sales_summary['MIN_DAILY_SALES'] = sales_summary['MIN_DAILY_SALES'].fillna(0)
        
        # Merge with stock items to include items with no sales
        result = stock_items.merge(sales_summary, on=['SITE', 'ITEM'], how='left')
        
        print(f"   📊 Sales calculated: {len(sales_summary)} items have sales, {len(result)} total items")
        
        # Debug for F858
        f858_sales = result[(result['ITEM'] == 'F858') & (result['TOTAL_SALES_QTY'] > 0)]
        if not f858_sales.empty:
            for idx, row in f858_sales.iterrows():
                print(f"   🔍 F858 FOUND: SITE={row['SITE']}, SALES={row['TOTAL_SALES_QTY']}, MAX_DAILY={row['MAX_DAILY_SALES']}")
        
        return result
    
    def _add_master_data_optimized(self, results_df, items_master, sites_master, categories_master):
        """Add item names, site names, and PROPER categories"""
        
        # Add item names and categories
        if items_master is not None:
            items_subset = items_master[['ITEM', 'DESCR1', 'CATEGORY']].drop_duplicates()
            results_df = results_df.merge(items_subset, on='ITEM', how='left')
            results_df.rename(columns={'DESCR1': 'ITEM_NAME'}, inplace=True)
        else:
            results_df['ITEM_NAME'] = 'Unknown Item'
            results_df['CATEGORY'] = ''
        
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
        
        return results_df
