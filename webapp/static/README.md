# Static Files Directory

## Favicon Setup

To use your logo as a favicon:

1. **Convert your logo to ICO format:**
   - Take your logo image (PNG, JPG, etc.)
   - Convert it to ICO format using an online converter like:
     - https://convertio.co/png-ico/
     - https://favicon.io/favicon-converter/
   - Recommended sizes: 16x16, 32x32, 48x48, 64x64 pixels

2. **Save the favicon:**
   - Save the converted ICO file as `favicon.ico` in this directory
   - Path should be: `c:\projects\DustReports\webapp\static\favicon.ico`

3. **Alternative formats (optional):**
   - You can also add PNG versions for modern browsers:
     - `favicon-16x16.png` (16x16 pixels)
     - `favicon-32x32.png` (32x32 pixels)
     - `apple-touch-icon.png` (180x180 pixels for iOS)

## Current Setup

The application is configured to:
- Serve favicon from `/favicon.ico` route
- Reference it in HTML templates using `{{ url_for('favicon') }}`
- Fall back to 404 if favicon file doesn't exist

## Usage

Once you place your `favicon.ico` file in this directory, it will automatically be served by the Flask application and displayed in browser tabs, bookmarks, and browser history.
