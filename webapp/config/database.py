"""Database configuration module"""

import os

# Database connection parameters
DATABASE_CONFIG = {
    'DATA_SOURCE': os.getenv('IB_DATA_SOURCE', "100.200.2.1"),
    'DATABASE_PATH': os.getenv('IB_DATABASE_PATH', r"D:\dolly2008\fer2015.dol"),
    'USERNAME': os.getenv('IB_USERNAME', "ALIOSS"),
    'PASSWORD': os.getenv('IB_PASSWORD', "Ali@123"),
    'CLIENT_LIBRARY': os.getenv('IB_CLIENT_LIBRARY', r"C:\Users\User\Downloads\Compressed\ibclient64-14.1_x86-64\ibclient64-14.1.dll"),
}

# Prefer ODBC for much faster data loading (~3x). Set USE_ODBC=0 to use direct InterBase only.
USE_ODBC = os.getenv('USE_ODBC', '1').strip().lower() in ('1', 'true', 'yes')

# ODBC driver name: "InterBase ODBC Driver" (free Embarcadero) or "Devart ODBC Driver for InterBase" (paid)
ODBC_DRIVER = os.getenv('IB_ODBC_DRIVER', "InterBase ODBC Driver")

# ODBC DSN: if set, connect using this Data Source Name (e.g. "INTERBASE IBA") instead of driver + host/path
ODBC_DSN = os.getenv('IB_ODBC_DSN', "INTERBASE IBA").strip() or None


def get_connection_string():
    """Build ODBC connection string for pyodbc. Uses DSN if ODBC_DSN is set, else driver + params."""
    if ODBC_DSN:
        return (
            f"DSN={ODBC_DSN};"
            f"UID={DATABASE_CONFIG['USERNAME']};"
            f"PWD={DATABASE_CONFIG['PASSWORD']};"
        )
    return (
        f"DRIVER={{{ODBC_DRIVER}}};"
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
