# Database Table Descriptions and Calculation Guide

## Overview
This document describes the database tables used in the IBA Inventory Management System and provides detailed instructions on how to calculate various metrics including sales, quantities, stock levels, and autonomy.

---

## Table Descriptions

### 1. **ALLITEM** - Inventory Transaction Records
**Purpose**: Primary table containing all inventory transactions (receipts, issues, transfers, adjustments)

**Key Fields**:
- `SITE` - Site/Location identifier (stores, warehouses, etc.)
- `ITEM` - Item/Product code identifier
- `DEBITQTY` - Quantity received/added to inventory (positive movements)
- `CREDITQTY` - Quantity issued/removed from inventory (negative movements)
- `FDATE` - Transaction date
- `LOGDATE` - System log timestamp

**What it represents**: 
- Each record is a single inventory transaction
- DEBIT transactions increase stock (receipts, returns, transfers in)
- CREDIT transactions decrease stock (sales, issues, transfers out, adjustments)

**Example Queries**:
```sql
-- Get total received quantity for an item at a site
SELECT SUM(DEBITQTY) as TOTAL_RECEIVED 
FROM ALLITEM 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001' AND DEBITQTY IS NOT NULL;

-- Get total issued quantity for an item at a site
SELECT SUM(CREDITQTY) as TOTAL_ISSUED 
FROM ALLITEM 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001' AND CREDITQTY IS NOT NULL;

-- Count total transactions for an item
SELECT COUNT(*) as TRANSACTION_COUNT 
FROM ALLITEM 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001';
```

---

### 2. **ITEMS** - Sales Transaction Records
**Purpose**: Contains sales and transaction details with pricing and customer information

**Key Fields**:
- `SITE` - Site/Location identifier
- `ITEM` - Item/Product code identifier  
- `QTY` - Quantity sold/transacted
- `FTYPE` - Transaction type code
- `FDATE` - Transaction date
- `PRICE` - Unit price (if available)
- `TOTAL` - Total transaction value (if available)

**FTYPE Values**:
- `FTYPE = 1` - Regular sales transactions
- `FTYPE = 2` - Return sales transactions (credits)
- Other values may represent different transaction types

**What it represents**:
- Each record is a sales transaction
- Used to calculate sales volumes, revenue, and sales patterns
- Different from ALLITEM which tracks inventory movements

**Example Queries**:
```sql
-- Get total sales quantity for an item (sales only)
SELECT SUM(QTY) as TOTAL_SALES 
FROM ITEMS 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001' 
AND (FTYPE = 1 OR FTYPE = 2) AND QTY IS NOT NULL;

-- Get sales by date range
SELECT FDATE, SUM(QTY) as DAILY_SALES 
FROM ITEMS 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001' 
AND (FTYPE = 1 OR FTYPE = 2) 
AND FDATE BETWEEN '2025-01-01' AND '2025-06-20'
GROUP BY FDATE ORDER BY FDATE;

-- Get oldest sales date for sales period calculation
SELECT MIN(FDATE) as OLDEST_SALES_DATE 
FROM ITEMS 
WHERE SITE = 'SITE001' AND ITEM = 'ITEM001' 
AND (FTYPE = 1 OR FTYPE = 2) AND FDATE IS NOT NULL;
```

---

### 3. **ALLSTOCK** - Site/Location Master Data
**Purpose**: Master data table containing site information and location details

**Key Fields**:
- `ID` - Site identifier (matches SITE in other tables)
- `SITE` - Site name/description
- `SIDNO` - Site number/category code
- `ITEM` - Item code (when present)

**Special Values**:
- `SIDNO = 3700004` - Identifies depot/warehouse locations

**What it represents**:
- Site master data and location hierarchy
- Used to identify depot sites for depot quantity calculations
- Links site codes to readable site names

**Example Queries**:
```sql
-- Get site name from site ID
SELECT SITE FROM ALLSTOCK WHERE ID = 'SITE001';

-- Find all depot sites
SELECT DISTINCT ID FROM ALLSTOCK WHERE SIDNO = 3700004;

-- Get all sites
SELECT DISTINCT ID, SITE FROM ALLSTOCK ORDER BY ID;
```

---

## Cache Tables (MySQL - iba_cache database)

### 4. **stock_cache** - Calculated Stock Data Cache
**Purpose**: Pre-calculated stock metrics for performance optimization

**Key Fields**:
- `site` - Site identifier
- `item` - Item identifier  
- `current_stock` - Calculated current stock level
- `total_debit` - Sum of all DEBIT transactions
- `total_credit` - Sum of all CREDIT transactions
- `total_sales` - Sum of all sales from ITEMS table
- `depot_qty` - Quantity available in depot locations
- `transaction_count` - Total number of transactions
- `average_monthly_sales` - Calculated average monthly sales
- `stock_autonomy_days` - Days of stock remaining at current sales rate
- `last_calculated` - When this record was last updated
- `cache_timestamp` - When this record was cached

### 5. **cache_metadata** - Cache Management
**Purpose**: Tracks cache status and update times

**Key Fields**:
- `cache_name` - Name of the cache ('stock_report')
- `last_updated` - When cache was last built
- `total_records` - Number of cached records
- `cache_status` - Current status (building, complete, stale, error)

---

## Key Calculations

### 1. **Current Stock Calculation**
```
Current Stock = Total DEBIT Quantity - Total CREDIT Quantity
```
```sql
-- SQL Implementation
SELECT 
    (COALESCE(SUM(DEBITQTY), 0) - COALESCE(SUM(CREDITQTY), 0)) as CURRENT_STOCK
FROM ALLITEM 
WHERE SITE = ? AND ITEM = ?;
```

### 2. **Depot Quantity Calculation**
```
Depot Quantity = Current Stock at Depot Sites
```
```sql
-- SQL Implementation
SELECT SUM(
    COALESCE(debit.TOTAL_DEBIT, 0) - COALESCE(credit.TOTAL_CREDIT, 0)
) as DEPOT_QUANTITY
FROM (
    SELECT DISTINCT ID FROM ALLSTOCK WHERE SIDNO = 3700004
) depot_sites
LEFT JOIN (
    SELECT SITE, SUM(DEBITQTY) as TOTAL_DEBIT 
    FROM ALLITEM WHERE ITEM = ? GROUP BY SITE
) debit ON depot_sites.ID = debit.SITE
LEFT JOIN (
    SELECT SITE, SUM(CREDITQTY) as TOTAL_CREDIT 
    FROM ALLITEM WHERE ITEM = ? GROUP BY SITE  
) credit ON depot_sites.ID = credit.SITE;
```

### 3. **Sales Calculations**

#### Total Sales
```sql
SELECT SUM(QTY) as TOTAL_SALES 
FROM ITEMS 
WHERE SITE = ? AND ITEM = ? 
AND (FTYPE = 1 OR FTYPE = 2) 
AND QTY IS NOT NULL;
```

#### Average Monthly Sales
```
Average Monthly Sales = (Total Sales / Sales Period in Days) * 30
```

#### Daily Average Sales  
```
Daily Average Sales = Total Sales / Sales Period in Days
```

### 4. **Stock Autonomy (Days of Stock)**
```
Stock Autonomy Days = Current Stock / Daily Average Sales
```

**Interpretation**:
- `> 30 days` - Good stock level
- `15-30 days` - Moderate stock level  
- `< 15 days` - Low stock level, consider reordering
- `0 days` - Out of stock
- `Negative` - Oversold/shortage situation

### 5. **Sales Period Calculation**
```sql
-- Get sales period in days
SELECT DATEDIFF(CURRENT_DATE, MIN(FDATE)) as SALES_PERIOD_DAYS
FROM ITEMS 
WHERE SITE = ? AND ITEM = ? 
AND (FTYPE = 1 OR FTYPE = 2) 
AND FDATE IS NOT NULL;
```

---


## Common Query Patterns

### 1. **Complete Stock Summary for Item**
```sql
SELECT 
    a.SITE,
    a.ITEM,
    SUM(COALESCE(a.DEBITQTY, 0)) as TOTAL_RECEIVED,
    SUM(COALESCE(a.CREDITQTY, 0)) as TOTAL_ISSUED,
    SUM(COALESCE(a.DEBITQTY, 0)) - SUM(COALESCE(a.CREDITQTY, 0)) as CURRENT_STOCK,
    COUNT(*) as TRANSACTION_COUNT,
    s.TOTAL_SALES
FROM ALLITEM a
LEFT JOIN (
    SELECT SITE, ITEM, SUM(QTY) as TOTAL_SALES
    FROM ITEMS 
    WHERE (FTYPE = 1 OR FTYPE = 2) AND QTY IS NOT NULL
    GROUP BY SITE, ITEM
) s ON a.SITE = s.SITE AND a.ITEM = s.ITEM
WHERE a.SITE = ? AND a.ITEM = ?
GROUP BY a.SITE, a.ITEM, s.TOTAL_SALES;
```

### 2. **Stock Autonomy Analysis**
```sql
-- From cache table (recommended)
SELECT 
    site,
    item,
    current_stock,
    average_monthly_sales,
    stock_autonomy_days,
    CASE 
        WHEN stock_autonomy_days > 30 THEN 'Good Stock'
        WHEN stock_autonomy_days BETWEEN 15 AND 30 THEN 'Moderate Stock'
        WHEN stock_autonomy_days BETWEEN 1 AND 14 THEN 'Low Stock'
        WHEN stock_autonomy_days <= 0 THEN 'Out of Stock'
        ELSE 'No Sales Data'
    END as STOCK_STATUS
FROM stock_cache
WHERE site = ? 
ORDER BY stock_autonomy_days ASC;
```

---

## Data Quality Notes

1. **NULL Handling**: Always check for NULL values in quantity fields
2. **Date Ranges**: Some records may have invalid or missing dates
3. **Site Matching**: Ensure SITE values match between tables
4. **Transaction Types**: Verify FTYPE values for accurate sales calculations
5. **Depot Identification**: SIDNO = 3700004 identifies depot locations

This guide should help understand the database structure and perform accurate calculations for inventory management metrics.
