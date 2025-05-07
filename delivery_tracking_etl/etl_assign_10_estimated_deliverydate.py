from delivery_tracking_etl.logger_config import setup_logger
from delivery_tracking_etl.config_db import TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from delivery_tracking_etl.util_dt import datetime_by_timezone
from delivery_tracking_etl.config_etlassign import ETLASSIGN_CONFIG
from datetime import timedelta
import pytz
import mysql.connector

# Crear el logger usando el nombre del módulo
logger = setup_logger(__name__)
print("Logger created successfully.")

def extract_data():
    # Converting the UTC datetime to local datetime
    datetimeFromTimeZone = datetime_by_timezone()
    logger.printInfo(f"Current datet & time by timezone: {datetimeFromTimeZone}")

    # Getting the date with margin
    date_with_margin = datetimeFromTimeZone - timedelta(days=DT_CONFIG['DAYSOFMARGIN_ASSIGNDELIVERYDATE'])

    # Query to extract data from source table
    query = """
    SELECT id, CAST(orden_fecha AS DATETIME) AS orden_fecha, orden_origen, orden_destino
    FROM seguimiento_documento
    WHERE orden_fecha >= %s
    AND (plazoasignado_fechlimite IS NULL or programado_fechllegada IS NULL or entransito_fechllegada IS NULL)
    AND orden_estado <> 0
    AND transportista_guia_estado <> 0;
    """

    # Connect to the source database
    logger.printInfo("Connecting to database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor(dictionary=True)
    logger.printInfo("Connection successful.")

    # Filter value
    fecha_filter_1 = date_with_margin.date().strftime('%Y-%m-%d')
    logger.printInfo(f"Filter1 value: {fecha_filter_1}")

    logger.printInfo("Extracting data from source table...")
    cursor.execute(query, (fecha_filter_1, ))

    # Fetch all rows
    logger.printInfo("Fetching all rows...")
    rows = cursor.fetchall()
    logger.printInfo("Rows fetched.")

    cursor.close()
    connection.close()
    logger.printInfo("Connection closed.")
    return rows

def update_data(data, recordCount):
    # Connect to the target database
    logger.printInfo("Connecting to database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor()
    logger.printInfo("Connection successful.")

    # Insert data into TABLE_B if it doesn't exist
    print("-------------------------------------------------------------------")
    logger.printInfo("Updating data into target table...")
    counter = 0
    for row in data:
        # Extract data from row
        id = row['id']
        orden_fecha = row['orden_fecha']
        orden_origen = row['orden_origen']
        orden_destino = row['orden_destino']

        # Increment the counter
        counter += 1
        porcentaje = (counter / recordCount) * 100
        print(f"Processing record: {id} | {orden_fecha} | {orden_origen} | {orden_destino} | #{counter} of {recordCount} | {porcentaje:.2f}%")

        # Consulta para obtener el tiempo de entrega desde la tabla B
        query_b = """
        SELECT tiempoentrega_dias FROM destino_tiempoentrega
        WHERE destino_nombre = %s LIMIT 1;
        """
        # Ejecutar la consulta y obtener el tiempo de entrega
        print(f"Obteniendo tiempoentrega_dias de {orden_origen} hacia {orden_destino}...")
        if (orden_destino == ETLASSIGN_CONFIG['LIMA_LOCATION']):
            cursor.execute(query_b, (orden_origen,))
        else:
            cursor.execute(query_b, (orden_destino,))
        tiempo_entrega = cursor.fetchone()

        if tiempo_entrega:
            tiempo_entrega_dias = tiempo_entrega[0]
            print(f"Se encontró tiempoentrega_dias para {orden_destino}: {tiempo_entrega_dias}")

            # Asignar la fecha límite de entrega en base a la fecha de orden y el tiempo de entrega
            plazoasignado_fechlimite = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=23) + timedelta(minutes=59) + timedelta(seconds=59)
            print(f"Valor para plazoasignado_fechlimite: {plazoasignado_fechlimite}")
            programado_fechllegada = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=23) + timedelta(minutes=59) + timedelta(seconds=59)
            print(f"Valor para programado_fechllegada: {programado_fechllegada}")
            entransito_fechllegada = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=23) + timedelta(minutes=59) + timedelta(seconds=59)
            print(f"Valor para entransito_fechllegada: {entransito_fechllegada}")

            # Actualizar campo plazoasignado_fechlimite en la tabla seguimiento_documento
            update_query = """
            UPDATE seguimiento_documento
            SET plazoasignado_fechlimite = CASE WHEN plazoasignado_fechlimite IS NULL THEN %s ELSE plazoasignado_fechlimite END,
                programado_fechllegada = CASE WHEN programado_fechllegada IS NULL THEN %s ELSE programado_fechllegada END,
                entransito_fechllegada = CASE WHEN entransito_fechllegada IS NULL THEN %s ELSE entransito_fechllegada END,
                estado_id = CASE WHEN estado_id = 0 THEN 10 ELSE estado_id END /* PLAZO DE ENTREGA ASIGNADO */
            WHERE id = %s;
            """
            print(f"Evaluar actualizacion plazoasignado_fechlimite de registro: {id}...")
            cursor.execute(update_query, (plazoasignado_fechlimite, plazoasignado_fechlimite, plazoasignado_fechlimite, id))
            # Verificar si se actualizó el campo
            if cursor.rowcount > 0:
                print("Commiting the transaction...")
                connection.commit()
                print("Transaction commited.")
            else:
                print("No se actualizó el campo plazoasignado_fechlimite.")
            ### END IF ###
        else:
            print(f"No se encontró tiempoentrega_dias para para {orden_destino}")
        ## END IF ###

        print("-------------------------------------------------------------------")
    ### END FOR LOOP ###

    # Close the cursor and connection
    cursor.close()
    # Close the connection
    connection.close()
    logger.printInfo("Connection closed.")

def main():
    logger.printInfo("ASSIGN_10_ESTIMATED_DELIVERY_DATE STARTING...")
    try:
        logger.printInfo("Review loaded TRGT_DB_CONNECTION_CONFIG:")
        for key, value in TRGT_DB_CONNECTION_CONFIG.items():
            if key == 'password':
                value = '*** hiden ***'
            logger.printInfo(f"{key}: {value}")

        # Step 1: Extract data from source table
        logger.printInfo("STEP 1 STARTING: GETTING DATA FROM seguimiento_documento TABLE...")
        data = extract_data()
        recordCount = len(data)
        logger.printInfo(f"STEP 1 COMPLETED: {recordCount} ROWS")

        # Step 2: Dump data into target table
        if data:
            logger.printInfo("STEP 2 STARTING: UPDATING DATA INTO seguimiento_documento TABLE...")
            update_data(data, recordCount)
            logger.printInfo("STEP 2 COMPLETED: DATA UPDATED")
        else:
            logger.printInfo("No data to update.")

        logger.printInfo("ASSIGN_10_ESTIMATED_DELIVERY_DATE FINISHED.")
    except mysql.connector.Error as e:
        logger.printError("Error en MySql!")
        logger.printException(e)
        logger.printInfo("ASSIGN_10_ESTIMATED_DELIVERY_DATE FINISHED W/ ERRORS.")
    except Exception as e:
        logger.printError("Error en la ejecución del ETL!")
        logger.printException(e)
        logger.printInfo("ASSIGN_10_ESTIMATED_DELIVERY_DATE FINISHED W/ ERRORS.")
    ### END TRY-EXCEPT ###
### END MAIN ###

if __name__ == "__main__":
    main()
