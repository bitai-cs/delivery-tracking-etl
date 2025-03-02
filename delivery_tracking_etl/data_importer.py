from delivery_tracking_etl.config_db import SRC_DB_CONNECTION_CONFIG, TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from datetime import datetime, timezone, timedelta
import pytz
import mysql.connector

def extract_data_from_source_table():
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
    date_with_margin = local_datetime - timedelta(days=DT_CONFIG['DAYSOFMARGIN'])
    
    # Query to extract data from source table
    query = """
    SELECT DISTINCT orden.id AS orden_id,
	orden.fecha AS orden_fecha ,
	ma.fecha AS embarque_fecha,
	CONCAT(orden.serie, "-", orden.numero) AS orden_servicio,
	orp.medio_pago AS ordenpago_forma,
	CONCAT(guit.serie, "-", guit.numero) AS transportista_guia,
	CONCAT(ma.serie, "-", ma.numero) AS manifiesto_nro,
	lo.nombre AS origen,
	cr.razon_social AS remitente,
	ld.nombre AS destino,
	cd.razon_social AS destinatario,
	te.nombre AS punto_entrega,
	orden.total_cantidad AS cantidad,
	orden.total_peso AS peso,
	orden.total_importe,
	CONCAT(per.nombre, "-", per.apellido) AS conductor,
	vi.placa AS vehiculo_placa,
	CONCAT(des.serie, "-", des.numero) AS desembarque,
	des.fecha AS fecha_desembarque,
	orden.fecha_entregado,
	0 estado_entrega,
	(
		CASE WHEN orden.fl_pagado = '1' THEN 'Pagado' ELSE 'No pagado' END
	) estado_pago,
	orp.cobrador,
	CONCAT(f.serie, "-", f.numero) AS factura_nro,
	f.total_importe AS factura_monto,
    %s AS etl_utcdt_update
    FROM
	orden orden
	INNER JOIN socio cr ON cr.id = orden.id_remitente
	LEFT JOIN socio cd ON cd.id = orden.id_destinatario
	-- LEFT JOIN socio ct ON ct.id = orden.id_tercero
	LEFT JOIN lugar lo ON lo.id = orden.id_lugar_origen
	LEFT JOIN lugar ld ON ld.id = orden.id_lugar_destino
	LEFT JOIN tipo_entrega te ON te.id = orden.id_tipo_entrega
	LEFT JOIN factura f ON f.id = orden.id_factura
 
	LEFT JOIN guia_transportista_orden guio ON guio.id_orden = orden.id
	LEFT JOIN guia_transportista guit ON guit.id = guio.id_guia_transportista
	LEFT JOIN manifiesto ma ON ma.id = guit.id_manifiesto
	LEFT JOIN personal per ON per.id = guit.id_conductor
	LEFT JOIN vehiculo vi ON vi.id = guit.id_vehiculo
	-- inner join desembarque_guia dgui on dgui.id_guia_transportista=guit.id
	LEFT JOIN desembarque des ON des.id = guit.id_desembarque
 
	LEFT JOIN orden_pago orp ON orp.id_orden = guio.id_orden
    WHERE orden.fecha >= %s AND orden.fecha <= %s
    AND NOT guit.id IS NULL;
    """
    
    # Connect to the source database
    print("Connecting to source database...")
    connection = mysql.connector.connect(**SRC_DB_CONNECTION_CONFIG)
    cursor = connection.cursor(dictionary=True)
    
    #Field value
    iso_format = local_datetime.isoformat()
    print(f"Field value: {iso_format}")
    
    # Filter value
    fecha_filter_1 = date_with_margin.date().strftime('%Y-%m-%d')
    print(f"Filter1 value: {fecha_filter_1}")
    
    # Filter value
    fecha_filter_2 = local_datetime.date().strftime('%Y-%m-%d')
    print(f"Filter2 value: {fecha_filter_2}")

    print("Extracting data from source table...")
    cursor.execute(query, (iso_format, fecha_filter_1, fecha_filter_2,))
    
    # Fetch all rows
    print("Fetching all rows...")
    rows = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return rows

def dump_data_into_target_table(data):
    # Connect to the target database
    print("Connecting to target database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor()
    
    # Insert data into TABLE_B if it doesn't exist
    for row in data:
        # UNIQUE KEY: orden_id, transportista_guia
        identifier1 = row['orden_id']  # Assuming 'id' is the unique identifier column
        identifier2 = row['transportista_guia']  # Assuming 'id' is the unique identifier column
        print(f"Inserting (or updateting if exists) record with KEY({identifier1} | {identifier2})...")                
        # Getting column names
        columns = row.keys()
        # Getting column values
        placeholders = ', '.join(['%s'] * len(columns))
        # Building the query template
        query = f"""
        INSERT INTO seguimiento_documento ({', '.join(columns)}) VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {', '.join([f"{column}=VALUES({column})" for column in columns])};
        """
        # Inserting or updating data into target table
        cursor.execute(query, tuple(row.values()))

    # Commit the transaction            
    connection.commit()
    # Close the cursor and connection
    cursor.close()
    # Close the connection
    connection.close()

def main():
    try:
        # Step 1: Extract data from source table
        print("Extracting data from source table...")
        data = extract_data_from_source_table()
        print(f"Extracted {len(data)} rows from source table.")

        # Step 2: Dump data into target table
        if data:
            print("Dumping data into target table...")
            dump_data_into_target_table(data)
            print("Data successfully dumped into target table.")
        else:
            print("No data to dump into target table.")

    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
