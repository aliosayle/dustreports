"""Stock analysis and calculation models"""

import pandas as pd
from services.database_service import get_dataframes


class StockAnalyzer:
    """Main class for stock and sales analysis calculations"""
    
    def __init__(self):
        self.dataframes = get_dataframes()
    
    def calculate_stock_and_sales(self, item_code=None, site_code=None, from_date=None, 
                                 to_date=None, as_of_date=None, site_codes=None, category_id=None):
        """
        Calculate current stock and sales (matching notebook exactly)
        """
        if not self.dataframes:
            return None
            
        # Start with inventory transactions for stock calculation
        df_stock = self.dataframes['inventory_transactions'].copy()
        
        # Apply filters
        df_stock = self._apply_stock_filters(df_stock, item_code, site_code, site_codes, 
                                           category_id, as_of_date)
        
        if df_stock is None:
            return None
        
        # Calculate stock metrics
        result_df = self._calculate_stock_metrics(df_stock, item_code, site_code, site_codes)
        
        # Calculate sales analytics
        result_df = self._calculate_sales_analytics(result_df, item_code, site_code, 
                                                   site_codes, from_date, to_date)
        
        # Calculate derived metrics
        result_df = self._calculate_derived_metrics(result_df)
        
        # Add enrichment data
        result_df = self._enrich_with_master_data(result_df, site_codes)
        
        # Apply business rules
        result_df = self._apply_business_rules(result_df, site_codes, site_code)
        
        return result_df
    
    def _apply_stock_filters(self, df_stock, item_code, site_code, site_codes, category_id, as_of_date):
        """Apply various filters to stock data"""
        
        # Filter by date if specified
        if as_of_date:
            as_of_dt = pd.to_datetime(as_of_date)
            if 'FDATE' in df_stock.columns:
                df_stock['FDATE'] = pd.to_datetime(df_stock['FDATE'], errors='coerce')
                df_stock = df_stock[df_stock['FDATE'] <= as_of_dt]
                print(f"📅 Filtering stock transactions up to {as_of_date} - {len(df_stock)} transactions found")
            else:
                print("⚠️ FDATE column not found in inventory transactions - cannot filter by date")
        
        # Filter by item if specified
        if item_code:
            df_stock = df_stock[df_stock['ITEM'] == item_code]
            if df_stock.empty:
                print(f"❌ No stock transactions found for item: {item_code}")
                return None
        
        # Filter by site if specified (single site - legacy support)
        if site_code:
            df_stock = df_stock[df_stock['SITE'] == site_code]
            if df_stock.empty:
                print(f"❌ No stock transactions found for site: {site_code}")
                return None
        
        # Filter by multiple sites if specified
        if site_codes and isinstance(site_codes, list) and len(site_codes) > 0:
            df_stock = df_stock[df_stock['SITE'].isin(site_codes)]
            if df_stock.empty:
                print(f"❌ No stock transactions found for sites: {site_codes}")
                return None
            print(f"📍 Filtering for {len(site_codes)} sites: {site_codes}")
        
        # Filter by category if specified
        if category_id:
            if 'inventory_items' in self.dataframes and self.dataframes['inventory_items'] is not None:
                items_in_category = self.dataframes['inventory_items'][
                    self.dataframes['inventory_items']['CATEGORY'].astype(str) == str(category_id)
                ]['ITEM'].unique()
                
                if len(items_in_category) == 0:
                    print(f"❌ No items found in category: {category_id}")
                    return None
                
                df_stock = df_stock[df_stock['ITEM'].isin(items_in_category)]
                if df_stock.empty:
                    print(f"❌ No stock transactions found for items in category: {category_id}")
                    return None
                print(f"📋 Filtering for category {category_id}: {len(items_in_category)} items")
            else:
                print("⚠️ Inventory items data not available for category filtering")
                return None
        
        # Fill NaN values with 0 for calculations
        df_stock['DEBITQTY'] = df_stock['DEBITQTY'].fillna(0)
        df_stock['CREDITQTY'] = df_stock['CREDITQTY'].fillna(0)
        
        return df_stock
    
    def _calculate_stock_metrics(self, df_stock, item_code, site_code, site_codes):
        """Calculate basic stock metrics based on grouping"""
        
        if item_code and site_code:
            # Single item, single site
            result_df = pd.DataFrame({
                'SITE': [site_code],
                'ITEM': [item_code],
                'TOTAL_IN': [df_stock['DEBITQTY'].sum()],
                'TOTAL_OUT': [df_stock['CREDITQTY'].sum()],
                'CURRENT_STOCK': [df_stock['DEBITQTY'].sum() - df_stock['CREDITQTY'].sum()],
                'STOCK_TRANSACTIONS': [len(df_stock)],
                'SELECTED_SITES': ['']
            })
        elif item_code:
            # Single item, all sites (or selected sites)
            if site_codes and isinstance(site_codes, list) and len(site_codes) > 0:
                # Multiple sites selected - aggregate across all selected sites for this item
                site_names_display = self._get_site_names_display(site_codes)
                
                result_df = pd.DataFrame({
                    'SITE': [f"Multiple Sites ({len(site_codes)})"],
                    'ITEM': [item_code],
                    'TOTAL_IN': [df_stock['DEBITQTY'].sum()],
                    'TOTAL_OUT': [df_stock['CREDITQTY'].sum()],
                    'CURRENT_STOCK': [df_stock['DEBITQTY'].sum() - df_stock['CREDITQTY'].sum()],
                    'STOCK_TRANSACTIONS': [len(df_stock)],
                    'SELECTED_SITES': [site_names_display]
                })
            else:
                # All sites - show by site
                result_df = df_stock.groupby('SITE').agg({
                    'DEBITQTY': 'sum',
                    'CREDITQTY': 'sum',
                    'ITEM': 'first'
                }).reset_index()
                result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
                result_df['STOCK_TRANSACTIONS'] = df_stock.groupby('SITE').size().values
                result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
                result_df['SELECTED_SITES'] = ''
                result_df = result_df[['SITE', 'ITEM', 'TOTAL_IN', 'TOTAL_OUT', 'CURRENT_STOCK', 'STOCK_TRANSACTIONS', 'SELECTED_SITES']]
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
            result_df['SELECTED_SITES'] = ''
            result_df = result_df[['SITE', 'ITEM', 'TOTAL_IN', 'TOTAL_OUT', 'CURRENT_STOCK', 'STOCK_TRANSACTIONS', 'SELECTED_SITES']]
        elif site_codes and isinstance(site_codes, list) and len(site_codes) > 0:
            # Multiple sites selected - aggregate by item across selected sites
            result_df = df_stock.groupby('ITEM').agg({
                'DEBITQTY': 'sum',
                'CREDITQTY': 'sum'
            }).reset_index()
            result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
            result_df['STOCK_TRANSACTIONS'] = df_stock.groupby('ITEM').size().values
            result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
            
            site_names_display = self._get_site_names_display(site_codes)
            
            result_df['SITE'] = f"Multiple Sites ({len(site_codes)})"
            result_df['SELECTED_SITES'] = site_names_display
            result_df = result_df[['SITE', 'ITEM', 'TOTAL_IN', 'TOTAL_OUT', 'CURRENT_STOCK', 'STOCK_TRANSACTIONS', 'SELECTED_SITES']]
        else:
            # All items, all sites
            result_df = df_stock.groupby(['SITE', 'ITEM']).agg({
                'DEBITQTY': 'sum',
                'CREDITQTY': 'sum'
            }).reset_index()
            result_df['CURRENT_STOCK'] = result_df['DEBITQTY'] - result_df['CREDITQTY']
            result_df['STOCK_TRANSACTIONS'] = df_stock.groupby(['SITE', 'ITEM']).size().values
            result_df = result_df.rename(columns={'DEBITQTY': 'TOTAL_IN', 'CREDITQTY': 'TOTAL_OUT'})
            result_df['SELECTED_SITES'] = ''
        
        # Apply performance optimization
        if len(result_df) > 1000:
            print(f"📊 Large dataset detected ({len(result_df)} rows), applying performance optimizations...")
            result_df = result_df.sort_values('CURRENT_STOCK', ascending=False)
            result_df = result_df.head(1000)
            print(f"📊 Limited to top 1000 items by current stock for performance")
        
        print(f"📊 Processing {len(result_df)} rows for calculations...")
        return result_df
    
    def _get_site_names_display(self, site_codes):
        """Get display names for multiple sites"""
        site_names_display = "Multiple Sites"
        if 'sites' in self.dataframes and self.dataframes['sites'] is not None:
            site_name_map = dict(zip(self.dataframes['sites']['ID'], self.dataframes['sites']['SITE']))
            selected_site_names = [site_name_map.get(site_id, site_id) for site_id in site_codes]
            site_names_display = f"{', '.join(selected_site_names[:3])}" + (f" (+{len(selected_site_names)-3} more)" if len(selected_site_names) > 3 else "")
        return site_names_display
    
    def _calculate_sales_analytics(self, result_df, item_code, site_code, site_codes, from_date, to_date):
        """Calculate sales analytics and merge with stock data"""
        
        if 'sales_details' not in self.dataframes or self.dataframes['sales_details'] is None:
            # Add empty sales columns if sales data not available
            result_df['MAX_DAILY_SALES'] = 0
            result_df['MIN_DAILY_SALES'] = 0
            result_df['AVG_DAILY_SALES'] = 0
            result_df['SALES_TRANSACTIONS'] = 0
            result_df['TOTAL_SALES_QTY'] = 0
            result_df['SALES_PERIOD_DAYS'] = 0
            return result_df
        
        # Use the sales analytics calculator
        sales_calculator = SalesAnalyticsCalculator(self.dataframes)
        sales_analytics = sales_calculator.calculate_analytics(
            item_code, site_code, site_codes, from_date, to_date
        )
        
        # Merge with result_df
        if not sales_analytics.empty:
            result_df = result_df.merge(
                sales_analytics[['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES', 
                               'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS']], 
                on=['SITE', 'ITEM'], how='left'
            )
            # Fill missing values
            for col in ['MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS']:
                result_df[col] = result_df[col].fillna(0)
        else:
            # Add empty sales columns
            for col in ['MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS']:
                result_df[col] = 0
        
        return result_df
    
    def _calculate_derived_metrics(self, result_df):
        """Calculate derived metrics like stock autonomy and depot quantity"""
        
        # Calculate stock autonomy
        def calculate_autonomy(row):
            if row['CURRENT_STOCK'] <= 0:
                return -1  # Use -1 to represent N/A for zero stock
            elif row['AVG_DAILY_SALES'] > 0:
                return row['CURRENT_STOCK'] / row['AVG_DAILY_SALES']
            else:
                return 9999  # Use 9999 for infinite autonomy (stock but no sales)
        
        result_df['STOCK_AUTONOMY_DAYS'] = result_df.apply(calculate_autonomy, axis=1)
        
        # Calculate depot quantity
        depot_calculator = DepotQuantityCalculator(self.dataframes)
        result_df = depot_calculator.add_depot_quantities(result_df)
        
        return result_df
    
    def _enrich_with_master_data(self, result_df, site_codes):
        """Add site names, item names, categories, etc."""
        
        # Add site names
        if 'sites' in self.dataframes and self.dataframes['sites'] is not None:
            site_names = self.dataframes['sites'][['ID', 'SITE']].drop_duplicates()
            site_names = site_names.rename(columns={'ID': 'SITE', 'SITE': 'SITE_NAME'})
            result_df = result_df.merge(site_names, on='SITE', how='left')
            
            # For multi-site aggregation, set SITE_NAME to match SELECTED_SITES if available
            multi_site_mask = result_df['SITE'].str.contains('Multiple Sites', na=False)
            result_df.loc[multi_site_mask, 'SITE_NAME'] = result_df.loc[multi_site_mask, 'SELECTED_SITES']
        else:
            result_df['SITE_NAME'] = result_df['SITE']
        
        # Add item information
        if 'inventory_items' in self.dataframes and self.dataframes['inventory_items'] is not None:
            item_enricher = ItemDataEnricher(self.dataframes)
            result_df = item_enricher.enrich_with_item_data(result_df)
        else:
            # Add empty columns if no item data available
            for col in ['ITEM_NAME', 'CATEGORY', 'CATEGORY_NAME', 'SUNIT', 'POSPRICE1', 'STOCK_VALUE']:
                result_df[col] = '' if col in ['ITEM_NAME', 'CATEGORY', 'CATEGORY_NAME', 'SUNIT'] else 0
        
        return result_df
    
    def _apply_business_rules(self, result_df, site_codes, site_code):
        """Apply business rules like 6-month sales filtering"""
        
        # Apply 6-month sales filter
        sales_filter = SalesHistoryFilter(self.dataframes)
        result_df = sales_filter.filter_by_recent_sales(result_df, site_codes, site_code)
        
        return result_df


class SalesAnalyticsCalculator:
    """Handles sales analytics calculations"""
    
    def __init__(self, dataframes):
        self.dataframes = dataframes
    
    def calculate_analytics(self, item_code, site_code, site_codes, from_date, to_date):
        """Calculate sales analytics based on parameters"""
        
        df_sales = self.dataframes['sales_details']
        
        # Apply filters and get sales data
        sales_only = self._filter_sales_data(df_sales, item_code, site_code, site_codes, from_date, to_date)
        
        # Skip analytics for multi-site aggregation (performance)
        if site_codes and isinstance(site_codes, list) and len(site_codes) > 1:
            print("📊 Skipping detailed sales analytics for multi-site aggregation (performance optimization)")
            return pd.DataFrame()
        
        # Calculate analytics based on grouping
        if item_code and site_code:
            return self._calculate_single_item_single_site(sales_only, item_code, site_code, from_date, to_date)
        elif item_code:
            return self._calculate_single_item_multiple_sites(sales_only, item_code, from_date, to_date)
        elif site_code:
            return self._calculate_multiple_items_single_site(sales_only, site_code, from_date, to_date)
        else:
            print("📊 Skipping detailed sales analytics for all items/all sites (performance optimization)")
            return pd.DataFrame()
    
    def _filter_sales_data(self, df_sales, item_code, site_code, site_codes, from_date, to_date):
        """Filter sales data based on parameters"""
        
        print("📊 Starting sales analytics calculation...")
        
        # Apply filters
        sales_filters = []
        if item_code:
            sales_filters.append(df_sales['ITEM'] == item_code)
        if site_code:
            sales_filters.append(df_sales['SITE'] == site_code)
        if site_codes and isinstance(site_codes, list) and len(site_codes) > 0:
            sales_filters.append(df_sales['SITE'].isin(site_codes))
        
        # Apply all filters at once if any exist
        if sales_filters:
            combined_filter = sales_filters[0]
            for filt in sales_filters[1:]:
                combined_filter = combined_filter & filt
            df_sales = df_sales[combined_filter]
            print(f"📊 Filtered sales data to {len(df_sales)} records for analysis")
        
        # Date filtering
        if 'FDATE' in df_sales.columns and not df_sales.empty:
            df_sales = df_sales.copy()
            df_sales['FDATE'] = pd.to_datetime(df_sales['FDATE'], errors='coerce')
            
            if from_date:
                from_date_dt = pd.to_datetime(from_date)
                df_sales = df_sales[df_sales['FDATE'] >= from_date_dt]
            if to_date:
                to_date_dt = pd.to_datetime(to_date)
                df_sales = df_sales[df_sales['FDATE'] <= to_date_dt]
        
        # Filter for sales transactions
        if 'FTYPE' in df_sales.columns:
            sales_only = df_sales[df_sales['FTYPE'].isin([1, 2])]
        else:
            sales_only = df_sales
        
        # Filter for site sales only
        if 'SID' in sales_only.columns:
            sales_only = sales_only[sales_only['SID'].astype(str).str.startswith('530')]
            print(f"📊 Filtered to {len(sales_only)} sales transactions with SID starting with '530' (site sales only)")
        else:
            print("⚠️ SID column not found - unable to filter for site sales")
        
        sales_only['QTY'] = sales_only['QTY'].fillna(0)
        return sales_only
    
    def _calculate_single_item_single_site(self, sales_only, item_code, site_code, from_date, to_date):
        """Calculate analytics for single item, single site"""
        
        if not sales_only.empty:
            analytics = self._calculate_group_analytics(sales_only, from_date, to_date)
            return pd.DataFrame({
                'SITE': [site_code],
                'ITEM': [item_code],
                'MAX_DAILY_SALES': [analytics['MAX_DAILY_SALES']],
                'MIN_DAILY_SALES': [analytics['MIN_DAILY_SALES']], 
                'AVG_DAILY_SALES': [analytics['AVG_DAILY_SALES']],
                'SALES_TRANSACTIONS': [analytics['SALES_TRANSACTIONS']],
                'TOTAL_SALES_QTY': [analytics['TOTAL_SALES_QTY']],
                'SALES_PERIOD_DAYS': [analytics['SALES_PERIOD_DAYS']]
            })
        else:
            return pd.DataFrame({
                'SITE': [site_code],
                'ITEM': [item_code],
                'MAX_DAILY_SALES': [0],
                'MIN_DAILY_SALES': [0], 
                'AVG_DAILY_SALES': [0],
                'SALES_TRANSACTIONS': [0],
                'TOTAL_SALES_QTY': [0],
                'SALES_PERIOD_DAYS': [0]
            })
    
    def _calculate_single_item_multiple_sites(self, sales_only, item_code, from_date, to_date):
        """Calculate analytics for single item, multiple sites"""
        
        if not sales_only.empty:
            # Limit to top 20 sites by sales volume for performance
            top_sites = sales_only.groupby('SITE')['QTY'].sum().nlargest(20).index
            sales_only_limited = sales_only[sales_only['SITE'].isin(top_sites)]
            sales_analytics = sales_only_limited.groupby('SITE').apply(
                lambda x: self._calculate_group_analytics(x, from_date, to_date), 
                include_groups=False
            ).reset_index()
            sales_analytics['ITEM'] = item_code
            print(f"📊 Limited sales analytics to top {len(top_sites)} sites for performance")
            return sales_analytics
        else:
            return pd.DataFrame(columns=['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS'])
    
    def _calculate_multiple_items_single_site(self, sales_only, site_code, from_date, to_date):
        """Calculate analytics for multiple items, single site"""
        
        if not sales_only.empty:
            # Limit to top 100 items by sales volume for performance
            top_items = sales_only.groupby('ITEM')['QTY'].sum().nlargest(100).index
            sales_only_limited = sales_only[sales_only['ITEM'].isin(top_items)]
            sales_analytics = sales_only_limited.groupby('ITEM').apply(
                lambda x: self._calculate_group_analytics(x, from_date, to_date), 
                include_groups=False
            ).reset_index()
            sales_analytics['SITE'] = site_code
            print(f"📊 Limited sales analytics to top {len(top_items)} items for performance")
            return sales_analytics
        else:
            return pd.DataFrame(columns=['SITE', 'ITEM', 'MAX_DAILY_SALES', 'MIN_DAILY_SALES', 'AVG_DAILY_SALES', 'SALES_TRANSACTIONS', 'TOTAL_SALES_QTY', 'SALES_PERIOD_DAYS'])
    
    def _calculate_group_analytics(self, group, from_date_param=None, to_date_param=None):
        """Calculate analytics for a group of sales data"""
        
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
        total_sales_qty = group['QTY'].sum()
        
        # Calculate period days
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
            min_date = group['FDATE'].min()
            max_date = group['FDATE'].max()
            period_days = (max_date - min_date).days + 1 if min_date != max_date else 1
        
        # Calculate metrics
        non_zero_sales = daily_sales[daily_sales > 0]
        max_sales = daily_sales.max() if not daily_sales.empty else 0
        min_sales = non_zero_sales.min() if not non_zero_sales.empty else 0
        avg_sales = round(total_sales_qty / period_days) if period_days > 0 else 0
        total_transactions = len(group)
        
        return pd.Series({
            'MAX_DAILY_SALES': max_sales,
            'MIN_DAILY_SALES': min_sales,
            'AVG_DAILY_SALES': avg_sales,
            'SALES_TRANSACTIONS': total_transactions,
            'TOTAL_SALES_QTY': total_sales_qty,
            'SALES_PERIOD_DAYS': period_days
        })


class DepotQuantityCalculator:
    """Handles depot quantity calculations"""
    
    def __init__(self, dataframes):
        self.dataframes = dataframes
    
    def add_depot_quantities(self, result_df):
        """Add depot quantity column to result dataframe"""
        
        try:
            if 'sites' not in self.dataframes or self.dataframes['sites'] is None:
                print("⚠️ Sites data not available for depot quantity calculation")
                result_df['DEPOT_QUANTITY'] = 0
                return result_df
            
            # Get depot sites where SIDNO = '3700004'
            depot_sites_info = self.dataframes['sites'][self.dataframes['sites']['SIDNO'] == '3700004'].copy()
            
            if depot_sites_info.empty:
                print("⚠️ No depot sites found with SIDNO = '3700004'")
                result_df['DEPOT_QUANTITY'] = 0
                return result_df
            
            depot_site_ids = depot_sites_info['ID'].tolist()
            print(f"📦 Found {len(depot_site_ids)} depot sites for quantity calculation")
            
            # Get inventory transactions for depot sites only
            df_depot = self.dataframes['inventory_transactions'][
                self.dataframes['inventory_transactions']['SITE'].isin(depot_site_ids)
            ].copy()
            
            if df_depot.empty:
                print("⚠️ No inventory transactions found for depot sites")
                result_df['DEPOT_QUANTITY'] = 0
                return result_df
            
            # Calculate depot quantities
            df_depot['DEBITQTY'] = df_depot['DEBITQTY'].fillna(0)
            df_depot['CREDITQTY'] = df_depot['CREDITQTY'].fillna(0)
            
            depot_qty = df_depot.groupby('ITEM').agg({
                'DEBITQTY': 'sum',
                'CREDITQTY': 'sum'
            }).reset_index()
            
            depot_qty['DEPOT_QUANTITY'] = depot_qty['DEBITQTY'] - depot_qty['CREDITQTY']
            depot_dict = dict(zip(depot_qty['ITEM'], depot_qty['DEPOT_QUANTITY']))
            
            result_df['DEPOT_QUANTITY'] = result_df['ITEM'].map(depot_dict).fillna(0)
            
            total_depot_qty = depot_qty['DEPOT_QUANTITY'].sum()
            items_with_depot_stock = (depot_qty['DEPOT_QUANTITY'] > 0).sum()
            print(f"📊 Depot calculation: {total_depot_qty:,.0f} total units across {items_with_depot_stock:,} items")
            
        except Exception as e:
            print(f"❌ Error calculating depot quantities: {e}")
            result_df['DEPOT_QUANTITY'] = 0
        
        return result_df


class ItemDataEnricher:
    """Handles item master data enrichment"""
    
    def __init__(self, dataframes):
        self.dataframes = dataframes
    
    def enrich_with_item_data(self, result_df):
        """Add item names, categories, prices, etc."""
        
        item_info = self.dataframes['inventory_items'][['ITEM', 'DESCR1', 'CATEGORY', 'SUNIT', 'POSPRICE1']].drop_duplicates()
        item_info['ITEM_NAME'] = item_info['DESCR1'].fillna('').astype(str)
        
        # Add category descriptions if available
        if 'categories' in self.dataframes and self.dataframes['categories'] is not None:
            category_descriptions = self.dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
            category_descriptions['ID'] = category_descriptions['ID'].astype(str)
            category_descriptions = category_descriptions.rename(columns={'ID': 'CATEGORY', 'DESCR': 'CATEGORY_NAME'})
            
            item_info['CATEGORY'] = item_info['CATEGORY'].astype(str)
            item_info = item_info.merge(category_descriptions, on='CATEGORY', how='left')
            item_info['CATEGORY_NAME'] = item_info['CATEGORY_NAME'].fillna('').astype(str)
        else:
            item_info['CATEGORY_NAME'] = ''
        
        # Merge with result_df
        result_df = result_df.merge(
            item_info[['ITEM', 'ITEM_NAME', 'CATEGORY', 'CATEGORY_NAME', 'SUNIT', 'POSPRICE1']], 
            on='ITEM', how='left'
        )
        
        # Fill missing values
        result_df['ITEM_NAME'] = result_df['ITEM_NAME'].fillna('')
        result_df['CATEGORY'] = result_df['CATEGORY'].fillna('')
        result_df['CATEGORY_NAME'] = result_df['CATEGORY_NAME'].fillna('')
        result_df['SUNIT'] = result_df['SUNIT'].fillna('')
        result_df['POSPRICE1'] = result_df['POSPRICE1'].fillna(0)
        
        # Calculate stock value
        result_df['STOCK_VALUE'] = result_df['POSPRICE1'] * result_df['CURRENT_STOCK']
        
        return result_df


class SalesHistoryFilter:
    """Handles filtering based on sales history"""
    
    def __init__(self, dataframes):
        self.dataframes = dataframes
    
    def filter_by_recent_sales(self, result_df, site_codes, site_code):
        """Filter items based on 6-month sales history"""
        
        six_months_ago = pd.to_datetime('today') - pd.DateOffset(months=6)
        
        if 'sales_details' not in self.dataframes or self.dataframes['sales_details'] is None:
            print("⚠️ No sales data available for 6-month filtering")
            return result_df
        
        print("📊 Starting optimized 6-month filtering...")
        
        # Get relevant sites
        sites_to_check = []
        if site_codes and isinstance(site_codes, list) and len(site_codes) > 0:
            sites_to_check = site_codes
        elif site_code:
            sites_to_check = [site_code]
        
        # Check for depot sites
        depot_items = set()
        is_depot_site_selected = False
        
        if 'sites' in self.dataframes and self.dataframes['sites'] is not None:
            depot_sites_info = self.dataframes['sites'][self.dataframes['sites']['SIDNO'] == '3700004'].copy()
            if not depot_sites_info.empty:
                depot_site_ids = depot_sites_info['ID'].tolist()
                
                if sites_to_check:
                    selected_depot_sites = [site for site in sites_to_check if site in depot_site_ids]
                    if selected_depot_sites:
                        is_depot_site_selected = True
                        depot_items = set(result_df['ITEM'].unique())
                        print(f"🏪 Selected sites include depot sites - keeping all {len(depot_items)} items regardless of sales")
                else:
                    print("📦 No specific sites selected - applying 6-month sales filter")
        
        # Apply sales filtering if not depot site
        if not is_depot_site_selected:
            items_with_recent_sales = self._get_items_with_recent_sales(six_months_ago, sites_to_check)
            items_to_keep = set(items_with_recent_sales) | depot_items
        else:
            items_to_keep = depot_items
        
        # Filter result
        result_df = result_df[result_df['ITEM'].isin(items_to_keep)]
        print(f"📊 Final result: {len(result_df)} items after 6-month filtering (since {six_months_ago.strftime('%Y-%m-%d')})")
        
        return result_df
    
    def _get_items_with_recent_sales(self, six_months_ago, sites_to_check):
        """Get items that had sales in the last 6 months"""
        
        df_sales = self.dataframes['sales_details']
        
        if 'FDATE' not in df_sales.columns:
            print("⚠️ FDATE column not found in sales data")
            return []
        
        print("📊 Converting dates and applying filters...")
        
        # Create boolean masks for efficient filtering
        date_mask = pd.to_datetime(df_sales['FDATE'], errors='coerce') >= six_months_ago
        ftype_mask = df_sales['FTYPE'].isin([1, 2]) if 'FTYPE' in df_sales.columns else pd.Series([True] * len(df_sales))
        sid_mask = df_sales['SID'].astype(str).str.startswith('530') if 'SID' in df_sales.columns else pd.Series([True] * len(df_sales))
        
        if sites_to_check:
            site_mask = df_sales['SITE'].isin(sites_to_check)
        else:
            site_mask = pd.Series([True] * len(df_sales))
        
        # Combine all masks and apply
        combined_mask = date_mask & ftype_mask & sid_mask & site_mask
        
        print(f"📊 Applying combined filters to {len(df_sales)} total sales records...")
        df_sales_filtered = df_sales[combined_mask]
        
        print(f"📊 Filtered to {len(df_sales_filtered)} relevant sales transactions")
        
        items_with_recent_sales = df_sales_filtered['ITEM'].unique()
        print(f"🛍️ Found {len(items_with_recent_sales)} items with recent sales")
        
        return items_with_recent_sales
