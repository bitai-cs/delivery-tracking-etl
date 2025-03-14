from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# config_smtp.py
SMTP_CONFIG = {
    'host': os.getenv('SMTP_HOST'),
    'port': os.getenv('SMTP_PORT'),
    'username': os.getenv('SMTP_USER'),
    'password': os.getenv('SMTP_PASSWORD'),
    'use_tls': os.getenv('SMTP_USETLS')
}