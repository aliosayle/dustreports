# Reports 7 & 8 Analysis and Understanding

## Overview

This document provides a comprehensive understanding of **Report 7 (Kinshasa Bureau Client Report)** and **Report 8 (Kinshasa Bureau Items Report)**, including their data sources, calculations, table structures, and how dataframes translate to the reports.

---

## Report 7: Kinshasa Bureau Client Report

### Purpose
Shows sales and returns analysis for office clients (SID starting with 411) grouped by client.

### Route Information
- **Frontend Route**: `/kinshasa-bureau-client`
- **API Endpoint**: `/api/kinshasa-bureau-client-report` (POST)
- **Export Endpoint**: `/api/export-bureau-client-report` (POST)
- **Template**: `webapp/templates/kinshasa_bureau_client.html`

### Data Sources (Database Tables → DataFrames)

#### Primary Table: `ITEMS` → `sales_details` DataFrame
- **Purpose**: Contains all sales transaction details
- **Key Columns Used**:
  - `SID`: Client identifier (filtered for SID starting with '411')
  - `FDATE`: Transaction date (filtered by date range)
  - `FTYPE`: Transaction type (1=Sales, 2=Returns)
  - `QTY` or `QTY1`: Quantity for sales/returns
  - `MID`: Invoice ID (used for counting distinct invoices)
  - `CREDITUS`: Credit amount in USD
  - `DEBITUS`: Debit amount in USD
  - `CREDITVATAMOUNT`: Credit VAT amount
  - `DEBITVATAMOUNT`: Debit VAT amount
  - `SITE`: Site ID (used for SIDNO filtering via join with ALLSTOCK)

#### Secondary Table: `SUB` → `accounts` DataFrame
- **Purpose**: Contains client/account master data
- **Key Columns Used**:
  - `SID`: Client identifier (joins with sales_details.SID)
  - `SNAME`: Client name for display

#### Filtering Table: `ALLSTOCK` → `sites` DataFrame
- **Purpose**: Site/location master data for SIDNO filtering
- **Key Columns Used**:
  - `ID`: Site ID (joins with ITEMS.SITE)
  - `SIDNO`: Site number (3700002=Kinshasa, 3700004=Depot Kinshasa, 3700003=Interieur)

### Data Flow and Processing

1. **Load DataFrames**:
   ```python
   dataframes = get_dataframes()
   sales_df = dataframes['sales_details'].copy()  # ITEMS table
   accounts_df = dataframes['accounts'].copy()     # SUB table
   sites_df = dataframes['sites'].copy()          # ALLSTOCK table
   ```

2. **Filter sales_details**:
   - Filter by `SID LIKE '411%'` (Office Clients)
   - Filter by date range: `FDATE BETWEEN from_date AND to_date`
   - Filter by transaction type: `FTYPE IN (1, 2)` (Sales and Returns)
   - Optional: Filter by SIDNO via join:
     - Join: `ITEMS.SITE = ALLSTOCK.ID`
     - Filter: `ALLSTOCK.SIDNO IN [site_sidno array]`

3. **Group by SID (Client)** and calculate:
   - **Sales Qty**: `SUM(QTY) WHERE FTYPE = 1`
   - **Returns Qty**: `SUM(QTY) WHERE FTYPE = 2`
   - **Total (Net)**: `SALES_QTY - RETURNS_QTY`
   - **Number of Invoices**: `COUNT(DISTINCT MID)`
   - **Quantity in USD**: `SUM(CREDITUS - DEBITUS) + SUM(CREDITVATAMOUNT - DEBITVATAMOUNT)`

4. **Join with accounts** to get client names:
   - `sales_details.SID = accounts.SID`
   - Extract `SNAME` as `CLIENT_NAME`

5. **Sort**: By `TOTAL_QTY` descending

### Output Columns

| Column | Source | Calculation |
|--------|--------|-------------|
| **Client Name** | `accounts.SNAME` | Direct lookup from SUB table |
| **Client ID (SID)** | `sales_details.SID` | Client identifier |
| **Number of Invoices** | `sales_details.MID` | `COUNT(DISTINCT MID) GROUP BY SID` |
| **Sales Qty (FTYPE=1)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 1 GROUP BY SID` |
| **Returns Qty (FTYPE=2)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 2 GROUP BY SID` |
| **Total (Net)** | Calculated | `SALES_QTY - RETURNS_QTY` |
| **Quantity in USD** | `CREDITUS, DEBITUS, VAT amounts` | `SUM(CREDITUS - DEBITUS) + SUM(CREDITVATAMOUNT - DEBITVATAMOUNT)` |

### Key Calculations

#### USD Amount Formula (Same as Sales by Item Report)
```python
# Base amount
sales_df['BASE_AMOUNT'] = sales_df['CREDITUS'] - sales_df['DEBITUS']
base_amounts = sales_df.groupby('SID')['BASE_AMOUNT'].sum()

# VAT amount
sales_df['VAT_AMOUNT'] = sales_df['CREDITVATAMOUNT'] - sales_df['DEBITVATAMOUNT']
vat_amounts = sales_df.groupby('SID')['VAT_AMOUNT'].sum()

# Final USD amount per client
usd_amounts = base_amounts + vat_amounts
```

### API Response Structure
```json
{
  "data": [
    {
      "SID": "4112001",
      "CLIENT_NAME": "Client Name",
      "SALES_QTY": 100.0,
      "RETURNS_QTY": 5.0,
      "TOTAL_QTY": 95.0,
      "NUM_INVOICES": 12,
      "QUANTITY_USD": 15000.50
    }
  ],
  "metadata": {
    "total_clients": 50,
    "total_sales_qty": 5000.0,
    "total_returns_qty": 200.0,
    "total_net_qty": 4800.0,
    "total_invoices": 500,
    "total_quantity_usd": 750000.00,
    "from_date": "2024-01-01",
    "to_date": "2024-12-31",
    "site_sidno": ["3700002"],
    "filter": "SID starting with 411 (Office Clients), SIDNO in ['3700002']"
  }
}
```

---

## Report 8: Kinshasa Bureau Items Report

### Purpose
Shows top items by sales/returns for office clients (SID starting with 411) grouped by item.

### Route Information
- **Frontend Route**: `/kinshasa-bureau-items`
- **API Endpoint**: `/api/kinshasa-bureau-items-report` (POST)
- **Export Endpoint**: `/api/export-bureau-items-report` (POST)
- **Template**: `webapp/templates/kinshasa_bureau_items.html`

### Data Sources (Database Tables → DataFrames)

#### Primary Table: `ITEMS` → `sales_details` DataFrame
- **Purpose**: Contains all sales transaction details
- **Key Columns Used**:
  - `SID`: Client identifier (filtered for SID starting with '411')
  - `FDATE`: Transaction date (filtered by date range)
  - `FTYPE`: Transaction type (1=Sales, 2=Returns)
  - `QTY` or `QTY1`: Quantity for sales/returns
  - `ITEM`: Item code
  - `SITE`: Site ID (used for SIDNO filtering via join with ALLSTOCK)

#### Secondary Table: `STOCK` → `inventory_items` DataFrame
- **Purpose**: Contains item master data
- **Key Columns Used**:
  - `ITEM`: Item code (joins with sales_details.ITEM)
  - `DESCR1`: Item description/name
  - `CATEGORY`: Category ID
  - `NWEIGHT`: Item weight (used for weight calculation)

#### Tertiary Table: `DETDESCR` → `categories` DataFrame
- **Purpose**: Category definitions
- **Key Columns Used**:
  - `ID`: Category ID (joins with inventory_items.CATEGORY)
  - `DESCR`: Category name/description

#### Filtering Table: `ALLSTOCK` → `sites` DataFrame
- **Purpose**: Site/location master data for SIDNO filtering
- **Key Columns Used**:
  - `ID`: Site ID (joins with ITEMS.SITE)
  - `SIDNO`: Site number (3700002=Kinshasa, 3700004=Depot Kinshasa, 3700003=Interieur)

### Data Flow and Processing

1. **Load DataFrames**:
   ```python
   dataframes = get_dataframes()
   sales_df = dataframes['sales_details'].copy()      # ITEMS table
   items_df = dataframes['inventory_items'].copy()    # STOCK table
   categories_df = dataframes['categories'].copy()    # DETDESCR table
   sites_df = dataframes['sites'].copy()              # ALLSTOCK table
   ```

2. **Filter sales_details**:
   - Filter by `SID LIKE '411%'` (Office Clients)
   - Filter by date range: `FDATE BETWEEN from_date AND to_date`
   - Filter by transaction type: `FTYPE IN (1, 2)` (Sales and Returns)
   - Optional: Filter by SIDNO via join:
     - Join: `ITEMS.SITE = ALLSTOCK.ID`
     - Filter: `ALLSTOCK.SIDNO IN [site_sidno array]`

3. **Group by ITEM** and calculate:
   - **Sales Qty**: `SUM(QTY) WHERE FTYPE = 1`
   - **Returns Qty**: `SUM(QTY) WHERE FTYPE = 2`
   - **Total (Net)**: `SALES_QTY - RETURNS_QTY`

4. **Join with inventory_items** to get item information:
   - `sales_details.ITEM = inventory_items.ITEM`
   - Extract `DESCR1` as `ITEM_NAME`
   - Extract `CATEGORY` as `CATEGORY_ID`
   - Extract `NWEIGHT` for weight calculation

5. **Join with categories** to get category names:
   - `inventory_items.CATEGORY = categories.ID`
   - Extract `DESCR` as `CATEGORY_NAME`

6. **Calculate Weight**:
   - Formula: `(NWEIGHT × TOTAL_QTY) / 1000` (converts to tons)
   - `NWEIGHT` comes from `STOCK` table

7. **Filter and Sort**:
   - Filter out items with `TOTAL_QTY = 0`
   - Sort by `TOTAL_QTY` descending
   - Limit to top N items (default: 50, can be set to 0 for all items)

### Output Columns

| Column | Source | Calculation |
|--------|--------|-------------|
| **Item Code** | `sales_details.ITEM` | Item identifier |
| **Item Name (DESCR1)** | `inventory_items.DESCR1` | Item description from STOCK table |
| **Category** | `categories.DESCR` | Category name from DETDESCR table |
| **Sales Qty (FTYPE=1)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 1 GROUP BY ITEM` |
| **Returns Qty (FTYPE=2)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 2 GROUP BY ITEM` |
| **Total (Net)** | Calculated | `SALES_QTY - RETURNS_QTY` |
| **Weight (Tons)** | `inventory_items.NWEIGHT` | `(NWEIGHT × TOTAL_QTY) / 1000` |

### Key Calculations

#### Weight Calculation
```python
# Fill NaN values for NWEIGHT
result_df['NWEIGHT'] = result_df['NWEIGHT'].fillna(0)

# Calculate weight in tons
result_df['WEIGHT'] = (result_df['NWEIGHT'] * result_df['TOTAL_QTY']) / 1000
```

### API Response Structure
```json
{
  "data": [
    {
      "ITEM_CODE": "ITEM001",
      "ITEM_NAME": "Item Description",
      "CATEGORY": "Category Name",
      "SALES_QTY": 500.0,
      "RETURNS_QTY": 10.0,
      "TOTAL_QTY": 490.0,
      "WEIGHT": 12.5
    }
  ],
  "metadata": {
    "total_items": 50,
    "top_n": 50,
    "from_date": "2024-01-01",
    "to_date": "2024-12-31",
    "total_sales_qty": 25000.0,
    "total_returns_qty": 500.0,
    "total_net_qty": 24500.0,
    "total_weight": 625.0,
    "site_sidno": ["3700002"],
    "filter": "SID starting with 411 (Office Clients)"
  }
}
```

---

## Common Patterns and Data Translation

### DataFrame Loading Pattern
All reports use the same pattern to load dataframes:
```python
from services.database_service import get_dataframes

dataframes = get_dataframes()
if not dataframes:
    return jsonify({'error': 'No data loaded. Please load dataframes first.'}), 400

sales_df = dataframes['sales_details'].copy()      # ITEMS table
accounts_df = dataframes['accounts'].copy()        # SUB table
items_df = dataframes['inventory_items'].copy()    # STOCK table
categories_df = dataframes['categories'].copy()     # DETDESCR table
sites_df = dataframes['sites'].copy()              # ALLSTOCK table
```

### Table to DataFrame Mapping

| Database Table | DataFrame Key | Purpose |
|----------------|---------------|---------|
| `ITEMS` | `sales_details` | Sales transaction details |
| `SUB` | `accounts` | Client/account master data |
| `STOCK` | `inventory_items` | Item/product master data |
| `DETDESCR` | `categories` | Category definitions |
| `ALLSTOCK` | `sites` | Site/location master data |
| `INVOICE` | `invoice_headers` | Invoice headers |
| `PAYM` | `vouchers` | Payment vouchers |
| `ALLITEM` | `inventory_transactions` | All inventory transactions |

### Common Filtering Patterns

#### 1. Office Clients Filter
```python
sales_df = sales_df[sales_df['SID'].astype(str).str.startswith('411')]
```

#### 2. Date Range Filter
```python
sales_df['FDATE'] = pd.to_datetime(sales_df['FDATE'], errors='coerce')
from_date_dt = pd.to_datetime(from_date)
to_date_dt = pd.to_datetime(to_date)
sales_df = sales_df[
    (sales_df['FDATE'] >= from_date_dt) & 
    (sales_df['FDATE'] <= to_date_dt)
]
```

#### 3. Transaction Type Filter
```python
sales_df = sales_df[sales_df['FTYPE'].isin([1, 2])]  # 1=Sales, 2=Returns
```

#### 4. SIDNO Site Filter (via Join)
```python
# Filter sites by SIDNO
filtered_sites = sites_df[sites_df['SIDNO'].astype(str).isin(site_sidno_str)]
site_ids = filtered_sites['ID'].unique().tolist()

# Filter sales by SITE (ITEMS.SITE = ALLSTOCK.ID)
sales_df['SITE'] = sales_df['SITE'].astype(str)
site_ids_str = [str(sid) for sid in site_ids]
sales_df = sales_df[sales_df['SITE'].isin(site_ids_str)]
```

### Common Grouping Patterns

#### Group by Client (SID)
```python
sales_by_client = sales_df.groupby(['SID', 'FTYPE'])[qty_col].sum().reset_index()
sales_only = sales_by_client[sales_by_client['FTYPE'] == 1].copy()
returns_only = sales_by_client[sales_by_client['FTYPE'] == 2].copy()
```

#### Group by Item
```python
sales_by_item = sales_df.groupby(['ITEM', 'FTYPE'])[qty_col].sum().reset_index()
sales_only = sales_by_item[sales_by_item['FTYPE'] == 1].copy()
returns_only = sales_by_item[sales_by_item['FTYPE'] == 2].copy()
```

### Common Join Patterns

#### Join with Accounts (for Client Names)
```python
accounts_df['SID'] = accounts_df['SID'].astype(str)
sid_name_map = accounts_df[['SID', 'SNAME']].drop_duplicates()
client_names = dict(zip(sid_name_map['SID'], sid_name_map['SNAME']))
```

#### Join with Inventory Items (for Item Names)
```python
items_df = dataframes['inventory_items'][['ITEM', 'DESCR1', 'CATEGORY', 'NWEIGHT']].drop_duplicates()
items_df.columns = ['ITEM_CODE', 'ITEM_NAME', 'CATEGORY_ID', 'NWEIGHT']
items_df['ITEM_CODE'] = items_df['ITEM_CODE'].astype(str)
result_df['ITEM_CODE'] = result_df['ITEM_CODE'].astype(str)
result_df = result_df.merge(items_df, on='ITEM_CODE', how='left')
```

#### Join with Categories (for Category Names)
```python
categories_df = dataframes['categories'][['ID', 'DESCR']].drop_duplicates()
categories_df['ID'] = categories_df['ID'].astype(str)
categories_df.columns = ['CATEGORY_ID', 'CATEGORY_NAME']
result_df['CATEGORY_ID'] = result_df['CATEGORY_ID'].astype(str)
result_df = result_df.merge(categories_df, on='CATEGORY_ID', how='left')
```

---

## Key Differences Between Reports 7 and 8

| Aspect | Report 7 (Client Report) | Report 8 (Items Report) |
|--------|-------------------------|-------------------------|
| **Grouping** | By Client (SID) | By Item (ITEM) |
| **Primary Join** | With `accounts` (SUB) for client names | With `inventory_items` (STOCK) for item names |
| **Additional Columns** | Number of Invoices, Quantity in USD | Weight (Tons), Category |
| **Sorting** | By Total Qty descending | By Total Qty descending (Top N) |
| **Filtering** | All clients | Top N items (default 50, can be all) |
| **USD Calculation** | Yes (per client) | No |
| **Weight Calculation** | No | Yes (per item) |

---

## Export Functionality

Both reports support Excel export with:
- Formatted headers and styling
- Metadata rows (filters, totals, generation date)
- Totals row at the bottom
- Auto-adjusted column widths
- Filename includes date range and filters

### Export Endpoints
- Report 7: `/api/export-bureau-client-report` (POST)
- Report 8: `/api/export-bureau-items-report` (POST)

---

## Notes for Future Modifications

1. **Column Names**: Always check actual column names in dataframes (case-sensitive, may vary)
2. **Data Types**: Convert SID, ITEM, SITE to strings for proper matching
3. **NaN Handling**: Fill NaN values before calculations (especially for QTY, USD amounts, NWEIGHT)
4. **Date Handling**: Use `pd.to_datetime()` with `errors='coerce'` for date columns
5. **Join Keys**: Ensure data types match before merging (convert to string if needed)
6. **SIDNO Filtering**: Requires join between ITEMS.SITE and ALLSTOCK.ID, then filter by ALLSTOCK.SIDNO
7. **USD Formula**: Consistent across reports: `SUM(CREDITUS - DEBITUS) + SUM(CREDITVATAMOUNT - DEBITVATAMOUNT)`



