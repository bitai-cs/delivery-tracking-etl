from delivery_tracking_etl.logger_config import setup_logger
from delivery_tracking_etl.config_db import TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from delivery_tracking_etl.util_dt import datetime_by_timezone
from datetime import timedelta
import pytz
import mysql.connector

# Crear el logger usando el nombre del módulo
logger = setup_logger(__name__)
print("Logger created successfully.")

def extract_data():
    # Converting the UTC datetime to local datetime
    datetimeFromTimeZone = datetime_by_timezone()
    logger.printInfo(f"Current date & time by timezone: {datetimeFromTimeZone}")

    # Getting the date with margin
    date_with_margin = datetimeFromTimeZone - timedelta(days=DT_CONFIG['DAYSOFMARGIN_ASSIGNDELIVERED'])

    # Query to extract data from source table
    query = """
    SELECT  id, orden_id, CAST(orden_fecha AS DATETIME) AS orden_fecha, orden_servicio
    FROM seguimiento_documento
    WHERE   orden_fecha >= %s
            AND estado_id = 17 /* DESEMBARCADO EN DESTINO */
            AND orden_flagentregado = 1
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
        orden_id = row['orden_id']
        orden_fecha = row['orden_fecha']
        orden_servicio = row['orden_servicio']

        # Increment the counter
        counter += 1
        porcentaje = (counter / recordCount) * 100
        print(f"Processing record: {id} | {orden_id} | {orden_fecha} | {orden_servicio} | #{counter} of {recordCount} | {porcentaje:.2f}%")

        # Actualizar campo plazoasignado_fechlimite en la tabla seguimiento_documento
        update_query = """
        UPDATE seguimiento_documento
        SET estado_id = 40, /* ENTREGADO */
            completado_fecha = orden_fechaentregado,
            completado_cargospendientes = 1,
            completado_comentario = 'Estado actualizado desde el ETL',
            completado_usuestado = 'ETL',
            completado_fechestado = NOW()
        WHERE   estado_id = 17 /* DESEMBARCADO EN DESTINO */
                AND id = %s
                AND orden_flagentregado = 1;
        """
        print(f"Evaluar actualización ENTREGADO de registro: {id}...")
        cursor.execute(update_query, (id, ))
        # Verificar si se actualizó el campo
        if cursor.rowcount > 0:
            print("Commiting the transaction...")
            connection.commit()
            logger.printInfo(f"Updated: {id} | {orden_id} | {orden_fecha} | {orden_servicio}")
            print("Transaction commited.")
        else:
            logger.printInfo(f"No Updated: {id} | {orden_id} | {orden_fecha} | {orden_servicio}")
            print("No se actualizó a estado ENTREGADO.")
        ### END IF ###
        print("-------------------------------------------------------------------")
    ### END FOR LOOP ###

    # Close the cursor and connection
    cursor.close()
    # Close the connection
    connection.close()
    logger.printInfo("Connection closed.")

def main():
    logger.printInfo("ASSIGN_40_DELIVERED STARTING...")
    try:
        logger.printInfo("Review loaded TRGT_DB_CONNECTION_CONFIG:")
        for key, value in TRGT_DB_CONNECTION_CONFIG.items():
            if key == 'password':
                value = '*** hiden ***'
            logger.printInfo(f"{key}: {value}")

        logger.printInfo("Review loaded DT_CONFIG:")
        for key, value in DT_CONFIG.items():
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

        logger.printInfo("ASSIGN_40_DELIVERED FINISHED.")
    except mysql.connector.Error as e:
        logger.printError("Error en MySql!")
        logger.printException(e)
        logger.printInfo("ASSIGN_40_DELIVERED FINISHED W/ ERRORS.")
    except Exception as e:
        logger.printError("Error en la ejecución del ETL!")
        logger.printException(e)
        logger.printInfo("ASSIGN_40_DELIVERED FINISHED W/ ERRORS.")
    ### END TRY-EXCEPT ###
### END MAIN ###

if __name__ == "__main__":
    main()
