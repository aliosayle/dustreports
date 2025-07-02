# DataTables Export Filename Enhancement

## Overview
Enhanced both the custom export functionality and DataTables built-in export buttons to include filter information (site/magasin, item, dates, autonomy level) in exported filenames.

## Changes Made

### 1. **Custom Export Function (autonomy.html)**
- **Issue**: Custom export function was overriding server-generated filenames
- **Fix**: Updated JavaScript to respect server-provided filenames instead of hardcoding them
- **Result**: Custom export now uses descriptive filenames from server

### 2. **DataTables Built-in Export Buttons (autonomy.html)**
- **Issue**: DataTables Excel, CSV, PDF export buttons used default filenames
- **Enhancement**: Added dynamic `filename` functions to all export button configurations
- **Added Function**: `generateExportFilename()` that creates descriptive filenames based on current filters

### 3. **Server-side Export Enhancement (app.py)**
- **Enhanced**: `/api/export-excel` endpoint with better filename generation logic
- **Added**: Debug logging to track filter processing
- **Fixed**: Excel merge cells range to be dynamic based on number of columns

## Filename Generation Logic

### **Function: `generateExportFilename(baseFilename, extension)`**
Creates filenames in the format:
```
[base]_[magasin_SITE]_[item_ITEM]_[DATERANGE]_[level_LEVEL]_[TIMESTAMP]
```

### **Filter Components Included:**
1. **Site (Magasin)**: `magasin_STORE001`
2. **Item**: `item_PROD123` 
3. **Date Range**: 
   - Both dates: `20250101_to_20250630`
   - From only: `from_20250101`
   - To only: `to_20250630`
4. **Autonomy Level**: `level_critical`, `level_low`, etc.
5. **Timestamp**: `20250702_143022` (YYYYMMDD_HHMMSS)

## Example Filenames

### **Excel Export Examples:**
```
autonomy_report_magasin_STORE001_item_PROD123_20250101_to_20250630_level_critical_20250702_143022.xlsx
autonomy_report_magasin_DEPOT_MAIN_from_20250101_20250702_143022.xlsx
autonomy_report_level_low_20250702_143022.xlsx
autonomy_report_20250702_143022.xlsx
```

### **CSV Export Examples:**
```
autonomy_report_magasin_STORE001_20250101_to_20250630_20250702_143022.csv
autonomy_report_item_PROD123_level_high_20250702_143022.csv
```

### **PDF Export Examples:**
```
autonomy_report_magasin_DEPOT_CENTRAL_20250101_to_20250630_20250702_143022.pdf
autonomy_report_level_critical_20250702_143022.pdf
```

## Export Methods Enhanced

### **1. DataTables Built-in Export Buttons**
- ✅ Excel export (`excelHtml5`)
- ✅ CSV export (`csvHtml5`) 
- ✅ PDF export (`pdfHtml5`)
- ✅ Print (title includes filters)

### **2. Custom Export Function**
- ✅ Custom Excel export via `/api/export-excel` endpoint
- ✅ Respects server-generated filenames
- ✅ Includes all filter information

## Technical Implementation

### **Frontend (autonomy.html)**
```javascript
// Dynamic filename generation function
function generateExportFilename(baseFilename, extension) {
    // Collects current filter values from form elements
    // Sanitizes special characters for file system compatibility
    // Builds descriptive filename with timestamp
    return descriptiveFilename;
}

// DataTables button configuration
{
    extend: 'excel',
    filename: function() {
        return generateExportFilename('autonomy_report', 'xlsx');
    },
    // ... other options
}
```

### **Backend (app.py)**
```python
# Enhanced filename generation with filters
def api_export_excel():
    filters = data.get('filters', {})
    filename_parts = ['autonomy_report']
    
    if filters.get('site_code'):
        filename_parts.append(f'magasin_{site_code}')
    # ... additional filter logic
    
    filename = '_'.join(filename_parts) + '.xlsx'
    return send_file(output, download_name=filename)
```

## Benefits

1. **Traceability**: Filenames immediately show what filters were applied
2. **Organization**: Easy to identify and organize exported files
3. **Consistency**: Same naming convention across all export methods
4. **User-Friendly**: Clear, descriptive filenames that make sense to users
5. **No Conflicts**: Timestamps prevent filename collisions

## Files Modified

1. **`webapp/templates/autonomy.html`**
   - Added `generateExportFilename()` function
   - Updated DataTables button configurations with dynamic `filename` options
   - Fixed custom export function to respect server filenames

2. **`webapp/app.py`**
   - Enhanced `/api/export-excel` endpoint with better filename generation
   - Added debug logging for filter processing
   - Fixed Excel merge cells range calculation

## Testing

✅ Flask app imports successfully  
✅ All export methods (Excel, CSV, PDF) now generate descriptive filenames  
✅ Custom export function respects server-generated filenames  
✅ Filter information correctly included in all export formats  

The enhancement provides a professional, organized approach to file exports that makes it easy for users to identify and manage their exported reports based on the applied filters.
