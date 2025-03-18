#!/bin/bash
cd /opt/delivery-tracking-etl
LOG_FILE="cron_etl_data_importer_$(date +'%Y%m%d').log"
echo "Executing .sh file -> DT: $(date)" >> /opt/delivery-tracking-etl/$LOG_FILE
# Tomar en cuenta que en la instalación del proyecto ETLs se debió
# correr el comando "poetry install" para crear el entorno virtual python
# e instalar las dependencias del proyecto
poetry run etl_data_importer
