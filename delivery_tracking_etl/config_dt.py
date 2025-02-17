from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration for titanicsoft database
DT_CONFIG = {
    'TIMEZONE': os.getenv('DT_TIMEZONE'),
    'DAYSOFMARGIN': int(os.getenv('DT_DAYSOFMARGIN', 5))
}
