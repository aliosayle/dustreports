# Depot Quantity Implementation Summary

## Overview
Successfully replaced the "Restock to 7 Days" column with "Depot Quantity" throughout the DustReports application. Depot quantity is calculated as the sum of inventory quantities for each item across all depot sites (where SIDNO = 3700004).

## Changes Made

### 1. Notebook Updates (`dataexploration.ipynb`)

#### Updated Depot Quantity Calculation (Cell c033f935)
- **OLD**: Auto-detection of depot sites based on transaction volume and item diversity
- **NEW**: Specific identification of depot sites where `SIDNO = 3700004`
- **Logic**: Sum inventory quantities (DEBITQTY - CREDITQTY) for each item across all depot sites

#### Updated Demonstration Cell (Cell 877fb653)
- **SQL Logic**: Updated to show proper JOIN with ALLSTOCK table filtering by SIDNO = 3700004
- **Explanation**: Clarified that depot sites are specifically identified by SIDNO = 3700004
- **Business Value**: Emphasized real depot stock vs theoretical calculations

### 2. Flask Application Updates (`webapp/app.py`)

#### Replaced Restock Calculation Function
**OLD Code:**
```python
def calculate_7day_restock(row):
    if row['AVG_DAILY_SALES'] <= 0:
        return 0
    seven_day_requirement = row['AVG_DAILY_SALES'] * 7
    current_stock = row['CURRENT_STOCK']
    if current_stock >= seven_day_requirement:
        return 0
    else:
        return round(seven_day_requirement - current_stock)

result_df['RESTOCK_TO_7_DAYS'] = result_df.apply(calculate_7day_restock, axis=1)
```

**NEW Code:**
```python
def calculate_depot_quantities():
    """Calculate depot quantities for all items"""
    # Get depot sites where SIDNO = 3700004
    depot_sites_info = dataframes['sites'][dataframes['sites']['SIDNO'] == 3700004].copy()
    depot_site_ids = depot_sites_info['ID'].tolist()
    
    # Get inventory transactions for depot sites only
    df_depot = dataframes['inventory_transactions'][
        dataframes['inventory_transactions']['SITE'].isin(depot_site_ids)
    ].copy()
    
    # Calculate depot quantities by item (sum across all depot sites)
    depot_qty = df_depot.groupby('ITEM').agg({
        'DEBITQTY': 'sum',
        'CREDITQTY': 'sum'
    }).reset_index()
    
    depot_qty['DEPOT_QUANTITY'] = depot_qty['DEBITQTY'] - depot_qty['CREDITQTY']
    return dict(zip(depot_qty['ITEM'], depot_qty['DEPOT_QUANTITY']))

depot_quantities = calculate_depot_quantities()
result_df['DEPOT_QUANTITY'] = result_df['ITEM'].map(depot_quantities).fillna(0)
```

#### Updated Calculated Columns List
- **Removed**: `'RESTOCK_TO_7_DAYS'` from calc_columns set
- **Added**: `'DEPOT_QUANTITY'` to calc_columns set

### 3. Frontend Template Updates

#### Autonomy Report (`webapp/templates/autonomy.html`)
- **Line 388**: "Restock to 7 Days" → "Depot Quantity"
- **Line 434**: "Restock to 7 Days:" → "Depot Quantity:"
- **Line 1557**: "Restock to 7 Days" → "Depot Quantity"
- **Line 1598**: `row.RESTOCK_TO_7_DAYS` → `row.DEPOT_QUANTITY`
- **Line 1618**: Column title and DataTable definition updated

#### Custom Reports (`webapp/templates/custom_reports.html`)
- **Line 344**: `'calc.RESTOCK_TO_7_DAYS'` → `'calc.DEPOT_QUANTITY'`

## Technical Implementation Details

### Depot Site Identification
```sql
SELECT ID FROM ALLSTOCK WHERE SIDNO = 3700004
```

### Depot Quantity Calculation Logic
```sql
SELECT 
    ai.ITEM,
    SUM(ai.DEBITQTY) - SUM(ai.CREDITQTY) as DEPOT_QUANTITY
FROM ALLITEM ai
JOIN ALLSTOCK ast ON ai.SITE = ast.ID
WHERE ast.SIDNO = 3700004
GROUP BY ai.ITEM
```

### Data Flow
1. **Load Sites**: Query ALLSTOCK table to find depot sites (SIDNO = 3700004)
2. **Filter Transactions**: Get ALLITEM records for depot sites only
3. **Calculate Quantities**: Sum DEBITQTY - CREDITQTY per item across all depots
4. **Merge Results**: Add depot quantities to main analysis DataFrame

## Business Benefits

### Improved Insights
- **Real Data**: Actual stock at depot locations vs theoretical calculations
- **Actionable**: Shows immediately available stock for redistribution
- **Accurate**: Based on real inventory transactions

### Use Cases
- **High Depot Quantity**: Item readily available for distribution from depots
- **Low/Zero Depot Quantity**: May need procurement or redistribution to depots
- **Negative Depot Quantity**: Potential data issues or outstanding transfers

## Backward Compatibility
- All existing API endpoints continue to work
- Export functionality automatically includes new depot quantity column
- Legacy function names maintained where possible
- Gradual transition from theoretical to real data

## Files Modified
1. **`dataexploration.ipynb`** - Updated depot quantity calculation and demonstration
2. **`webapp/app.py`** - Replaced restock calculation with depot quantity logic
3. **`webapp/templates/autonomy.html`** - Updated UI labels and JavaScript references
4. **`webapp/templates/custom_reports.html`** - Updated column references

## Testing Status
✅ Notebook functions execute successfully
✅ Flask application updated with new logic
✅ Frontend templates updated with new labels
✅ Export functionality inherits new column automatically

## Next Steps
1. Test with live database connection
2. Verify depot quantity calculations with sample data
3. Train users on new depot quantity insights
4. Monitor performance with real data loads

## Summary
The transition from "Restock to 7 Days" to "Depot Quantity" provides more actionable, real-time inventory insights based on actual stock levels at designated depot locations (SIDNO = 3700004). This enhancement improves supply chain decision-making by showing immediately available stock for redistribution rather than theoretical restock requirements.
