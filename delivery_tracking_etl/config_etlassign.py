from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration for titanicsoft database
ETLASSIGN_CONFIG = {
    'LIMA_LOCATION': os.getenv('ETLASSIGN_LIMA_LOCATION')
}
