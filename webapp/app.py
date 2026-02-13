"""
DustReports Flask Application
A modular inventory and sales analytics system for IBA.
"""

from flask import Flask
import os
from config.database import config
from routes.main_routes import main_bp
from routes.api_routes import api_bp
from routes.export_routes import export_bp
from services.database_service import start_scheduled_reload

def create_app(config_name='default'):
    """Application factory pattern"""
    
    app = Flask(__name__)
    
    # Load configuration
    app.config.from_object(config[config_name])
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(export_bp)
    
    return app

# Create application instance
app = create_app(os.getenv('FLASK_CONFIG', 'default'))

if __name__ == '__main__':
    import os
    print("🚀 Starting DustReports Flask Application...")
    print("📊 Modular architecture loaded successfully")
    print("🔗 Available endpoints:")
    print("   📋 Main pages: /, /autonomy, /stock-by-site, /custom-reports, /ciment-report, /sales-report, /sales-by-item")
    print("   🔌 API endpoints: /api/...")
    print("   📤 Export endpoints: /api/export-...")

    # Optional: Load database cache on startup (AUTO_LOAD_ON_STARTUP=1)
    # Runs in background thread so server starts immediately
    if os.getenv('AUTO_LOAD_ON_STARTUP', '0').strip().lower() in ('1', 'true', 'yes'):
        import threading
        from services.database_service import load_dataframes
        def _startup_load():
            try:
                print("📊 Auto-load on startup: loading database cache...")
                load_dataframes()
                print("✅ Startup load completed")
            except Exception as e:
                print(f"⚠️ Startup load failed: {e}")
        threading.Thread(target=_startup_load, daemon=True).start()

    # Start scheduled auto-reload (loads saved config or uses defaults)
    try:
        from services.database_service import load_scheduled_reload_config

        # Try to load saved config first
        config_loaded = load_scheduled_reload_config()

        if config_loaded:
            # Start with loaded config
            start_scheduled_reload()
            from services.database_service import get_scheduled_reload_times
            times = get_scheduled_reload_times()
            print(f"✅ Scheduled auto-reload enabled with saved config: {', '.join(times)}")
        else:
            # Use defaults
            start_scheduled_reload()
            print("✅ Scheduled auto-reload enabled with default times (6 AM, 12 PM, 6 PM daily)")
    except Exception as e:
        print(f"⚠️ Failed to start scheduled reload: {e}")

    # Production: use Waitress (faster, multi-threaded). Dev: use Flask built-in.
    use_waitress = os.getenv('USE_WAITRESS', '1').strip().lower() in ('1', 'true', 'yes')
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '5000'))

    if use_waitress:
        print("🟢 Running with Waitress (production server, multi-threaded)")
        from waitress import serve
        serve(app, host=host, port=port, threads=8)
    else:
        print("🔧 Running with Flask development server")
        app.run(debug=True, host=host, port=port)
