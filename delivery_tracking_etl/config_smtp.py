from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# config_smtp.py
SMTP_CONFIG = {
    'SMTP_HOST': os.getenv('SMTP_HOST'),
    'SMTP_PORT': os.getenv('SMTP_PORT'),
    'SMTP_USER': os.getenv('SMTP_USER'),
    'SMTP_PASSWORD': os.getenv('SMTP_PASSWORD'),
    'SMTP_USETLS': os.getenv('SMTP_USETLS')
}