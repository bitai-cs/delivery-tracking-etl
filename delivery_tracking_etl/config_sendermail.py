from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# config_mailsender.py
NOTIFICATION_SENDERMAIL_CONFIG = {
    'NOTIF_SENDERNAME': os.getenv('NOTIF_SENDERNAME'),
    'NOTIF_SENDERMAIL': os.getenv('NOTIF_SENDERMAIL')
}