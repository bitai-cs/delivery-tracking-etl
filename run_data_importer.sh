#!/bin/bash
cd /opt/delivery-tracking-etl
export $(grep -v '^#' .env | xargs)
LOG_FILE="cron_data_importer_$(date +'%Y%m%d').log"
echo "DT: $(date) ENV-VARS: DT_TIMEZONE=$DT_TIMEZONE, SRC_DB_HOST=$SRC_DB_HOST, TRGT_DB_HOST=$TRGT_DB_HOST" >> /opt/delivery-tracking-etl/$LOG_FILE
# Tomar en cuenta que en la instalación del proyecto ETLs se debió 
# correr el comando poetry install para crear el entorno virtual python 
# e instalar las dependencias del proyecto
poetry run data_importer