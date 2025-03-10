from delivery_tracking_etl.logger_config import setup_logger
from delivery_tracking_etl.config_db import SRC_DB_CONNECTION_CONFIG, TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from datetime import datetime, timezone, timedelta
import pytz
import mysql.connector

# Crear el logger usando el nombre del módulo
logger = setup_logger(__name__)
print("Logger created successfully.")

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
    date_with_margin = local_datetime - timedelta(days=DT_CONFIG['DAYSOFMARGIN_IMPORT'])
    
    # Query to extract data from source table
    query = """
    SELECT DISTINCT 
	orden.id AS orden_id,
	orden.fecha AS orden_fecha ,	
	CONCAT(orden.serie, "-", orden.numero) AS orden_servicio,
	
	CONCAT(GuiaTransp.serie, "-", GuiaTransp.numero) AS transportista_guia,
	
	CONCAT(Manif.serie, "-", Manif.numero) AS manifiesto_nro,	
	Manif.fecha AS manifiesto_fecha,
	
	LugarOri.nombre AS orden_origen,
	SocioRemit.razon_social AS remitente_razonsocial,
	SocioRemit.numero_documento AS remitente_nrodoc /* NEW */,
	CASE SocioRemit.id_documento WHEN 1 THEN 'DNI' WHEN 2 THEN 'RUC' ELSE '???' END AS remitente_tipodoc /* NEW */,
	orden.guia_remitente AS remitente_guia,
	
	LugarDest.nombre AS orden_destino,
	SocioDest.razon_social AS destinatario_razonsocial,
	SocioDest.numero_documento AS destinatario_nrodoc /* NEW */,
	CASE SocioDest.id_documento WHEN 1 THEN 'DNI' WHEN 2 THEN 'RUC' ELSE '???' END AS destinatario_tipodoc /* NEW */,
	
	TipoEntrega.nombre AS orden_puntoentrega,
	orden.direccion_entrega AS orden_direccionentrega,
	
	orden.total_cantidad AS orden_totalcantidad,
	orden.total_peso AS orden_totalpeso,
	orden.total_importe AS orden_totalimporte,
	
	CONCAT(Person.nombre, "-", Person.apellido) AS vehiculo_conductor,
	Vehiculo.placa AS vehiculo_placa,
	
	CONCAT(Desembarque.serie, "-", Desembarque.numero) AS desembarque,
	Desembarque.fecha AS desembarque_fecha,
		
	orden.fl_entregado AS orden_flagentregado,
	orden.fecha_entregado AS orden_fechaentregado,
		
	CASE WHEN orden.fl_pagado = '1' THEN 'Pagado' ELSE 'No pagado' END AS orden_estadopago,
	OrdenPago.medio_pago AS ordenpago_forma,
	OrdenPago.cobrador as ordenpago_cobrador,
	
	CONCAT(Fact.serie, "-", Fact.numero) AS factura_nro,
	Fact.total_importe AS factura_monto,
    
    %s AS etl_utcdt_update
    
    FROM orden orden
	INNER JOIN socio SocioRemit ON SocioRemit.id = orden.id_remitente
	LEFT JOIN socio SocioDest ON SocioDest.id = orden.id_destinatario
	-- LEFT JOIN socio ct ON ct.id = orden.id_tercero
	LEFT JOIN lugar LugarOri ON LugarOri.id = orden.id_lugar_origen
	LEFT JOIN lugar LugarDest ON LugarDest.id = orden.id_lugar_destino
	LEFT JOIN tipo_entrega TipoEntrega ON TipoEntrega.id = orden.id_tipo_entrega
	LEFT JOIN factura Fact ON Fact.id = orden.id_factura
 
	LEFT JOIN guia_transportista_orden GuiaTranspOrden ON GuiaTranspOrden.id_orden = orden.id
	LEFT JOIN guia_transportista GuiaTransp ON GuiaTransp.id = GuiaTranspOrden.id_guia_transportista
	LEFT JOIN manifiesto Manif ON Manif.id = GuiaTransp.id_manifiesto
	LEFT JOIN personal Person ON Person.id = GuiaTransp.id_conductor
	LEFT JOIN vehiculo Vehiculo ON Vehiculo.id = GuiaTransp.id_vehiculo
	LEFT JOIN desembarque Desembarque ON Desembarque.id = GuiaTransp.id_desembarque
 
	LEFT JOIN orden_pago OrdenPago ON OrdenPago.id_orden = GuiaTranspOrden.id_orden
    WHERE orden.fecha >= %s AND orden.fecha <= %s
    AND NOT GuiaTransp.id IS NULL;
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

def dump_data_into_target_table(data, recordCount):
    # Connect to the target database
    print("Connecting to target database...")
    connection = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
    cursor = connection.cursor()
    
    # Insert data into TABLE_B if it doesn't exist
    counter = 0
    for row in data:
        counter += 1        
        # UNIQUE KEY: orden_id, transportista_guia
        identifier1 = row['orden_id']  # Assuming 'id' is the unique identifier column
        identifier2 = row['transportista_guia']  # Assuming 'id' is the unique identifier column
        porcentaje = (counter / recordCount) * 100
        print(f"Inserting (or updateting if exists) record with KEY({identifier1} | {identifier2}) | #{counter} of {recordCount} | {porcentaje:.2f}%")
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
        print(f"Record with KEY({identifier1} | {identifier2}) inserted (or updated) successfully.")
        # Commit the transaction            
        connection.commit()
        print("Transaction committed.")
    ### END FOR LOOP ### 
    
    # Close the cursor and connection
    cursor.close()
    # Close the connection
    connection.close()

def main():
    logger.info("STARTING DATA_IMPORTER...")
    try:
        # Step 1: Extract data from source table
        msg = "Extracting data from source table..."
        logger.info(msg)
        print(msg)
        data = extract_data_from_source_table()
        recordCount = len(data)
        msg = f"Extracted {recordCount} rows from source table."
        logger.info(msg)
        print(msg)

        # Step 2: Dump data into target table
        if data:
            msg = "Dumping data into target table..."
            logger.info(msg)
            print(msg)
            dump_data_into_target_table(data, recordCount)
            msg = "Data successfully dumped into target table."
            logger.info(msg)
            print(msg)
        else:
            msg = "No data to dump into target table."
            logger.info(msg)
            print(msg)

    except mysql.connector.Error as e:
        msg = f"MySQL Error: {e}"
        logger.error(msg)
        print(msg)
    except Exception as e:
        msg = f"MySQL Error: {e}"
        logger.error(msg)
        print(msg)
    ### END TRY-EXCEPT ###
    msg = "DATA_IMPORTER FINISHED."
    logger.info(msg)
    print(msg)

if __name__ == "__main__":
    main()
