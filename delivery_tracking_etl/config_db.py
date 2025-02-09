from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration for titanicsoft database
SRC_DB_CONNECTION_CONFIG = {
    'host': os.getenv('SRC_DB_HOST'),
    'user': os.getenv('SRC_DB_USER'),
    'password': os.getenv('SRC_DB_PASSWORD'),
    'database': os.getenv('SRC_DB_NAME'),
    'port': int(os.getenv('SRC_DB_PORT', 3306))
}

TRGT_DB_CONNECTION_CONFIG = {
    'host': os.getenv('TRGT_DB_HOST'),
    'user': os.getenv('TRGT_DB_USER'),
    'password': os.getenv('TRGT_DB_PASSWORD'),
    'database': os.getenv('TRGT_DB_NAME'),
    'port': int(os.getenv('TRGT_DB_PORT', 3306))
}
