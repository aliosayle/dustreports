# Report 7 Enhancement Summary

## Overview
Enhanced Report 7 (Kinshasa Bureau Client Report) by adding two new columns:
1. **Number of Invoices** - Count of distinct invoices per client
2. **Quantity in USD** - Total amount in USD using the same formula as Sales by Item Report

## Changes Made

### 1. API Route Updates (`webapp/routes/api_routes.py`)

#### Added Invoice Count Calculation
- Counts distinct `MID` (invoice ID) values per client (SID)
- Formula: `COUNT(DISTINCT MID) GROUP BY SID`

#### Added USD Amount Calculation
- Uses the same formula as Sales by Item Report: `SUM(CREDITUS - DEBITUS) + SUM(creditvatamount - debitvatamount)`
- Calculated per client (SID)
- Formula: `SUM(CREDITUS - DEBITUS) + SUM(creditvatamount - debitvatamount) GROUP BY SID`

#### Updated Response Data
- Added `NUM_INVOICES` field to each client record
- Added `QUANTITY_USD` field to each client record
- Updated metadata to include:
  - `total_invoices`: Total number of invoices across all clients
  - `total_quantity_usd`: Total USD amount across all clients

### 2. Template Updates (`webapp/templates/kinshasa_bureau_client.html`)

#### Added Table Columns
- Added "Number of Invoices" column header
- Added "Quantity in USD" column header
- Updated table to display 7 columns instead of 5

#### Added Formatting Function
- Added `formatCurrency()` function to format USD amounts with currency symbol

#### Updated DataTable Configuration
- Changed sort column from index 4 to index 5 (Total column moved)

### 3. Export Route Updates (`webapp/routes/export_routes.py`)

#### Updated Excel Export
- Added "Number of Invoices" column to headers
- Added "Quantity in USD" column to headers
- Updated merged cells range from `A1:E1` to `A1:G1`
- Added metadata rows for total invoices and total USD amount
- Updated totals row to include new columns

## Summary of Columns and Tables Used

### Tables Used

1. **sales_details** (ITEMS table)
   - Primary data source for sales transactions
   - Filtered by: `SID LIKE '4112%'` (Office Clients)
   - Filtered by: `FDATE BETWEEN from_date AND to_date`
   - Filtered by: `FTYPE IN (1, 2)` (Sales and Returns)

2. **accounts** (SUB table)
   - Used to get client names
   - Columns: `SID`, `SNAME`
   - Join: `sales_details.SID = accounts.SID`

### Columns Used from sales_details (ITEMS table)

| Column | Usage | Description |
|--------|-------|-------------|
| `SID` | Filter, Group By | Client identifier (filtered for SID starting with 4112) |
| `FDATE` | Filter | Transaction date (filtered by date range) |
| `FTYPE` | Filter, Group By | Transaction type (1=Sales, 2=Returns) |
| `QTY` | Calculate | Quantity for sales/returns calculations |
| `MID` | Calculate | Invoice ID (used for counting distinct invoices) |
| `CREDITUS` | Calculate | Credit amount in USD (for USD calculation) |
| `DEBITUS` | Calculate | Debit amount in USD (for USD calculation) |
| `creditvatamount` | Calculate | Credit VAT amount (for USD calculation) |
| `debitvatamount` | Calculate | Debit VAT amount (for USD calculation) |

### Columns Used from accounts (SUB table)

| Column | Usage | Description |
|--------|-------|-------------|
| `SID` | Join | Client identifier (matches sales_details.SID) |
| `SNAME` | Display | Client name for display |

### Output Columns

| Column | Source | Calculation |
|--------|--------|-------------|
| **Client Name** | `accounts.SNAME` | Client name from SUB table |
| **Client ID (SID)** | `sales_details.SID` | Client identifier |
| **Number of Invoices** | `sales_details.MID` | `COUNT(DISTINCT MID) GROUP BY SID` |
| **Sales Qty (FTYPE=1)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 1 GROUP BY SID` |
| **Returns Qty (FTYPE=2)** | `sales_details.QTY` | `SUM(QTY) WHERE FTYPE = 2 GROUP BY SID` |
| **Total (Net)** | Calculated | `SALES_QTY - RETURNS_QTY` |
| **Quantity in USD** | `sales_details.CREDITUS, DEBITUS, creditvatamount, debitvatamount` | `SUM(CREDITUS - DEBITUS) + SUM(creditvatamount - debitvatamount) GROUP BY SID` |

### Calculation Formulas

#### Number of Invoices
```sql
SELECT SID, COUNT(DISTINCT MID) as NUM_INVOICES
FROM sales_details
WHERE SID LIKE '4112%'
  AND FDATE BETWEEN :from_date AND :to_date
  AND FTYPE IN (1, 2)
GROUP BY SID
```

#### Quantity in USD
```sql
SELECT SID,
       SUM(CREDITUS - DEBITUS) + 
       SUM(creditvatamount - debitvatamount) as QUANTITY_USD
FROM sales_details
WHERE SID LIKE '4112%'
  AND FDATE BETWEEN :from_date AND :to_date
  AND FTYPE IN (1, 2)
GROUP BY SID
```

This formula matches the Sales by Item Report calculation method.

### Data Flow

1. **Filter sales_details** by:
   - `SID LIKE '4112%'` (Office Clients)
   - `FDATE BETWEEN from_date AND to_date`
   - `FTYPE IN (1, 2)`

2. **Group by SID** and calculate:
   - Sales quantity (FTYPE=1)
   - Returns quantity (FTYPE=2)
   - Number of distinct invoices (COUNT DISTINCT MID)
   - USD amount (CREDITUS-DEBITUS + VAT amounts)

3. **Join with accounts** table to get client names

4. **Sort by** Total (Net) quantity descending

5. **Return** JSON response with all columns

## Testing Notes

- Verify that `MID` column exists in sales_details table
- Verify that `CREDITUS`, `DEBITUS`, `creditvatamount`, `debitvatamount` columns exist
- Test with clients that have multiple invoices
- Test with clients that have both sales and returns
- Verify USD amounts match Sales by Item Report calculations for same clients

## Files Modified

1. `webapp/routes/api_routes.py` - API endpoint `/api/kinshasa-bureau-client-report`
2. `webapp/templates/kinshasa_bureau_client.html` - Frontend template
3. `webapp/routes/export_routes.py` - Excel export endpoint `/api/export-bureau-client-report`

