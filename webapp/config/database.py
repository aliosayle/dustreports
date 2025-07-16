"""Database configuration module"""

# Database connection parameters
DATABASE_CONFIG = {
    'DATA_SOURCE': "100.200.2.1",
    'DATABASE_PATH': r"D:\dolly2008\fer2015.dol",
    'USERNAME': "ALIOSS",
    'PASSWORD': "Ali@123",
    'CLIENT_LIBRARY': r"C:\Users\User\Downloads\Compressed\ibclient64-14.1_x86-64\ibclient64-14.1.dll"
}

# ODBC connection string (fallback)
def get_connection_string():
    return (
        f"DRIVER=Devart ODBC Driver for InterBase;"
        f"Data Source={DATABASE_CONFIG['DATA_SOURCE']};"
        f"Database={DATABASE_CONFIG['DATABASE_PATH']};"
        f"User ID={DATABASE_CONFIG['USERNAME']};"
        f"Password={DATABASE_CONFIG['PASSWORD']};"
    )

# Flask configuration
class Config:
    SECRET_KEY = 'iba-dust-reports-2025'
    DEBUG = False

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
