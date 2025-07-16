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
    print("🚀 Starting DustReports Flask Application...")
    print("📊 Modular architecture loaded successfully")
    print("🔗 Available endpoints:")
    print("   📋 Main pages: /, /autonomy, /stock-by-site, /custom-reports, /ciment-report")
    print("   🔌 API endpoints: /api/...")
    print("   📤 Export endpoints: /api/export-...")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
