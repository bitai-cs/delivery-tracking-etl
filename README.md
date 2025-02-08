# Data Import Python Program

## Overview
This Python program facilitates the seamless transfer of data from a source database, **Titanicsoft**, to a target database, **DocumentTracking**. It is designed to handle data extraction, transformation (if needed), and loading (ETL) efficiently and reliably.

---

## Features
- **Database Connectivity**: Connects to both Titanicsoft (source) and DocumentTracking (target) databases.
- **Data Extraction**: Fetches data from the Titanicsoft database.
- **Data Transformation**: (Optional) Performs data cleansing and transformation as required.
- **Data Loading**: Dumps the processed data into the DocumentTracking database.
- **Error Handling**: Logs errors during the ETL process for debugging and monitoring.

---

## Requirements

### Software and Libraries
- Python 3.x
- Required libraries:
  - `pymysql` or `mysql-connector-python` (for MySQL connectivity)
  - `psycopg2` (for PostgreSQL connectivity, if applicable)
  - `pandas` (for data manipulation)
  - `sqlalchemy` (for ORM support, optional)
  - `logging` (for logging ETL activities)

### Environment Variables
Set the following environment variables to securely store database credentials:
- **SOURCE_DB_HOST**: Hostname for Titanicsoft database.
- **SOURCE_DB_USER**: Username for Titanicsoft database.
- **SOURCE_DB_PASS**: Password for Titanicsoft database.
- **TARGET_DB_HOST**: Hostname for DocumentTracking database.
- **TARGET_DB_USER**: Username for DocumentTracking database.
- **TARGET_DB_PASS**: Password for DocumentTracking database.

---

## How to Run

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Up Environment**
   Export the required environment variables:
   ```bash
   export SOURCE_DB_HOST=your_source_db_host
   export SOURCE_DB_USER=your_source_db_user
   export SOURCE_DB_PASS=your_source_db_password
   export TARGET_DB_HOST=your_target_db_host
   export TARGET_DB_USER=your_target_db_user
   export TARGET_DB_PASS=your_target_db_password
   ```

3. **Run the Program**
   Execute the script:
   ```bash
   python data_import.py
   ```

---

## Program Workflow

1. **Initialize Database Connections**
   - Establish connections to both Titanicsoft and DocumentTracking databases.

2. **Extract Data**
   - Execute SQL queries to fetch the necessary data from the Titanicsoft database.

3. **Transform Data** (Optional)
   - Perform any data transformations, such as cleaning, reformatting, or validation.

4. **Load Data**
   - Insert the transformed data into the DocumentTracking database.

5. **Logging and Error Handling**
   - Log the progress and errors during the ETL process.

---

## Configuration

### Database Configuration
Update the database connection details in the configuration file (`config.yaml`) or via environment variables.

### Logging
Logs are saved in the `logs/` directory. Update the `logging.conf` file to customize the logging format or level.

---

## Example Configuration (config.yaml)
```yaml
source_db:
  host: your_source_db_host
  user: your_source_db_user
  password: your_source_db_password

target_db:
  host: your_target_db_host
  user: your_target_db_user
  password: your_target_db_password
```

---

## Future Enhancements
- Add support for multiple source and target databases.
- Include advanced data transformation pipelines.
- Implement a scheduler for periodic data imports.
- Add unit tests for better code coverage.

---

## License
This project is licensed under the MIT License.
