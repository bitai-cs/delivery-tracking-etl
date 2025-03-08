from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration for titanicsoft database
DT_CONFIG = {
    'TIMEZONE': os.getenv('DT_TIMEZONE'),
    'DAYSOFMARGIN_IMPORT': int(os.getenv('DT_DAYSOFMARGIN_IMPORT', 15)),
    'DAYSOFMARGIN_ASSIGNDELIVERYDATE': int(os.getenv('DT_DAYSOFMARGIN_ASSIGNDELIVERYDATE', 15))
}
