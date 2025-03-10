#!/bin/bash
cd /opt/delivery-tracking-etl
export $(grep -v '^#' .env | xargs)
LOG_FILE="cron_assign_estimated_delivery_date_$(date +'%Y%m%d').log"
echo "DT: $(date) ENV-VARS: DT_TIMEZONE=$DT_TIMEZONE, TRGT_DB_HOST=$TRGT_DB_HOST, DT_DAYSOFMARGIN_ASSIGNDELIVERYDATE=$DT_DAYSOFMARGIN_ASSIGNDELIVERYDATE" >> /opt/delivery-tracking-etl/$LOG_FILE
# Tomar en cuenta que en la instalación del proyecto ETLs se debió 
# correr el comando poetry install para crear el entorno virtual python 
# e instalar las dependencias del proyecto
poetry run assign_estimated_delivery_date