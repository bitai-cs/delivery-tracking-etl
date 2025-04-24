#!/bin/bash
cd /opt/delivery-tracking-etl
# Calcular la fecha de ayer
YESTERDAY_LOG="cron_etl_assign_13_embarked_to_destination_$(date -d "yesterday" +'%Y%m%d').log"
# Eliminar el archivo log de ayer si existe
if [ -f "$YESTERDAY_LOG" ]; then
    rm "$YESTERDAY_LOG"
    echo "Deleted previous day log file: $YESTERDAY_LOG"
fi
# Crear el nuevo archivo log para hoy
LOG_FILE="cron_etl_assign_13_embarked_to_destination_$(date +'%Y%m%d').log"
echo "Executing .sh file -> DT: $(date)" >> /opt/delivery-tracking-etl/$LOG_FILE
# Tomar en cuenta que en la instalación del proyecto ETLs se debió
# correr el comando "poetry install" para crear el entorno virtual python
# e instalar las dependencias del proyecto
poetry run etl_assign_13_embarked_to_destination