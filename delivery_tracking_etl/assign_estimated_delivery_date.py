from delivery_tracking_etl.config_db import TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from datetime import datetime, timezone, timedelta
import pytz
import mysql.connector

def extract_data():
    # Getting the current datetime in UTC
    utc_datetime = datetime.now(timezone.utc)
    print(f"Current UTC time: {utc_datetime}")
    
    # Getting the local timezone
    local_timezone = pytz.timezone(DT_CONFIG['TIMEZONE'])
    print(f"Local timezone: {local_timezone}")
    
    # Converting the UTC datetime to local datetime
    local_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(local_timezone)
    print(f"Current local time: {local_datetime}")
    
    # Getting the date with margin
    date_with_margin = local_datetime - timedelta(days=DT_CONFIG['DAYSOFMARGIN_ASSIGNDELIVERYDATE'])
    
    # Query to extract data from source table
    query = """
    SELECT id, CAST(orden_fecha AS DATETIME) AS orden_fecha, orden_destino 
    FROM seguimiento_documento 
    WHERE orden_fecha >= %s 
    AND (plazoasignado_fechlimite IS NULL or programado_fechllegada IS NULL or entransito_fechllegada IS NULL);
    """
    
    # Connect to the source database
    print("Connecting to database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor(dictionary=True)
    print("Connection successful.")
        
    # Filter value
    fecha_filter_1 = date_with_margin.date().strftime('%Y-%m-%d')
    print(f"Filter1 value: {fecha_filter_1}")
    
    print("Extracting data from source table...")
    cursor.execute(query, (fecha_filter_1, ))
    
    # Fetch all rows
    print("Fetching all rows...")
    rows = cursor.fetchall()
    print("Rows fetched.")
    
    cursor.close()
    connection.close()
    print("Connection closed.")
    return rows

def update_data(data, recordCount):
    # Connect to the target database
    print("Connecting to database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor()
    print("Connection successful.")
    
    # Insert data into TABLE_B if it doesn't exist
    print("-------------------------------------------------------------------")
    counter = 0
    for row in data:        
        # Extract data from row
        id = row['id']
        orden_fecha = row['orden_fecha']
        orden_destino = row['orden_destino']
                
        # Increment the counter
        counter += 1
        porcentaje = (counter / recordCount) * 100
        print(f"Processing record: {id} | {orden_fecha} | {orden_destino} | #{counter} of {recordCount} | {porcentaje:.2f}%")
                        
        # Consulta para obtener el tiempo de entrega desde la tabla B
        query_b = """
        SELECT tiempoentrega_dias FROM destino_tiempoentrega 
        WHERE destino_nombre = %s LIMIT 1;        
        """      
        # Ejecutar la consulta y obtener el tiempo de entrega
        print(f"Obteniendo tiempoentrega_dias para {orden_destino}...")
        cursor.execute(query_b, (orden_destino,))
        tiempo_entrega = cursor.fetchone()
           
        if tiempo_entrega:            
            tiempo_entrega_dias = tiempo_entrega[0]
            print(f"Se encontró tiempoentrega_dias para {orden_destino}: {tiempo_entrega_dias}")
            
            # Calcular las nuevas fechas
            plazoasignado_fechlimite = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=12)
            print(f"Valor para plazoasignado_fechlimite: {plazoasignado_fechlimite}")
            programado_fechllegada = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=12)
            print(f"Valor para programado_fechllegada: {programado_fechllegada}")
            entransito_fechllegada = orden_fecha + timedelta(days=tiempo_entrega_dias) + timedelta(hours=12)
            print(f"Valor para entransito_fechllegada: {entransito_fechllegada}")
            
            # Actualizar campo plazoasignado_fechlimite en la tabla seguimiento_documento
            update_query = """
            UPDATE seguimiento_documento 
            SET plazoasignado_fechlimite = %s
            WHERE id = %s AND plazoasignado_fechlimite IS NULL;
            """
            print(f"Evaluar actualizacion plazoasignado_fechlimite de registro: {id}...")
            cursor.execute(update_query, (plazoasignado_fechlimite, id))
            # Verificar si se actualizó el campo
            if cursor.rowcount > 0:
                print("Commiting the transaction...")
                connection.commit()
                print("Transaction commited.")
            else:
                print("No se actualizó el campo plazoasignado_fechlimite.")
            
            # Actualizar campos programado_fechllegada en la tabla A
            update_query = """
            UPDATE seguimiento_documento 
            SET programado_fechllegada = %s 
            WHERE id = %s AND programado_fechllegada IS NULL;
            """
            print(f"Evaluar actualizacion programado_fechllegada de registro: {id}...")
            cursor.execute(update_query, (programado_fechllegada, id))
            # Verificar si se actualizó el campo
            if cursor.rowcount > 0:
                print("Commiting the transaction...")
                connection.commit()
                print("Transaction commited.")
            else:
                print("No se actualizó el campo programado_fechllegada.")
            
            # Actualizar campo entransito_fechllegada en la tabla A
            update_query = """
            UPDATE seguimiento_documento 
            SET entransito_fechllegada = %s 
            WHERE id = %s AND entransito_fechllegada IS NULL;
            """
            print(f"Evaluar actualizacion entransito_fechllegada de registro: {id}...")
            cursor.execute(update_query, (entransito_fechllegada, id))
            # Verificar si se actualizó el campo
            if cursor.rowcount > 0:
                print("Commiting the transaction...")
                connection.commit()
                print("Transaction commited.")
            else:
                print("No se actualizó el campo entransito_fechllegada.")
        else:
            print(f"No se encontró tiempoentrega_dias para para {orden_destino}")
            
        print("-------------------------------------------------------------------")
    ### END FOR LOOP ###
        
    # Close the cursor and connection
    cursor.close()
    # Close the connection
    connection.close()
    print("Connection closed.")

def main():
    try:
        # Step 1: Extract data from source table
        print("STEP 1 STARTING: GETTING DATA FROM seguimiento_documento TABLE...")
        data = extract_data()
        recordCount = len(data)
        print(f"STEP 1 COMPLETED: {recordCount} ROWS")

        # Step 2: Dump data into target table
        if data:
            print("STEP 2 STARTING: UPDATING DATA INTO seguimiento_documento TABLE...")
            update_data(data, recordCount)
            print("STEP 2 COMPLETED: DATA UPDATED")
        else:
            print("No data to update.")

    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
