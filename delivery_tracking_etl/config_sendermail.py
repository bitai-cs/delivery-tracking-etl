from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# config_mailsender.py
NOTIFICATION_SENDERMAIL_CONFIG = {
    'sender_name': os.getenv('NOTIF_SENDERNAME'),
    'sender_email': os.getenv('NOTIF_SENDERMAIL')
}