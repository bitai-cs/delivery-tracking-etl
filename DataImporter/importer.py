import mysql.connector
from datetime import datetime, timedelta

# Database configurations
TITANICSOFT_CONFIG = {
    'host': 'localhost',
    'user': 'titanicsoft_admin',
    'password': 'titanicsoft_admin',
    'database': 'titanicsoft'
}

# CTL_DOC_CONFIG = {
#     'host': 'localhost',
#     'user': 'ctldoc_admin',
#     'password': 'ctldoc_admin',
#     'database': 'ctldoc'
# }
CTL_DOC_CONFIG = {
    'host': '159.203.97.223',
    'user': 'ctldoc_admin',
    'password': 'ctldoc_admin',
    'database': 'ctldoc'
}

def extract_data():
    """Extract data from TABLE_A in titanicsoft database."""
    connection = mysql.connector.connect(**TITANICSOFT_CONFIG)
    cursor = connection.cursor(dictionary=True)
    
    # Calculate the filter time
    #current_time = datetime.now()
    # filter_time = current_time - timedelta(minutes=5)
    filter_time = "2024-01-03";
    
    # SQL query to fetch data
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
	f.total_importe AS factura_monto
    FROM
	orden orden
	INNER JOIN socio cr ON cr.id = orden.id_remitente
	LEFT JOIN socio cd ON cd.id = orden.id_destinatario
	LEFT JOIN socio ct ON ct.id = orden.id_tercero
	LEFT JOIN lugar lo ON lo.id = orden.id_lugar_origen
	LEFT JOIN lugar ld ON ld.id = orden.id_lugar_destino
	LEFT JOIN tipo_entrega te ON te.id = orden.id_tipo_entrega
	LEFT JOIN factura f ON f.id = orden.id_factura
	INNER JOIN guia_transportista_orden guio ON guio.id_orden = orden.id
	INNER JOIN guia_transportista guit ON guit.id = guio.id_guia_transportista
	INNER JOIN manifiesto ma ON ma.id = guit.id_manifiesto
	INNER JOIN personal per ON per.id = guit.id_conductor
	INNER JOIN vehiculo vi ON vi.id = guit.id_vehiculo
	/*inner join desembarque_guia dgui on dgui.id_guia_transportista=guit.id*/
	INNER JOIN desembarque des ON des.id = guit.id_desembarque
	LEFT JOIN orden_pago orp ON orp.id_orden = guio.id_orden
    WHERE ma.fecha = %s;
    """
    
    print("Try to extract data from source...")
    cursor.execute(query, (filter_time,))
    rows = cursor.fetchall()
    
    cursor.close()
    connection.close()
    return rows

def dump_data_to_table_b(data):
    """Insert data into TABLE_B in ctldoc database, avoiding duplicates."""
    connection = mysql.connector.connect(**CTL_DOC_CONFIG)
    cursor = connection.cursor()
    
    # Insert data into TABLE_B if it doesn't exist
    for row in data:
        identifier1 = row['orden_id']  # Assuming 'id' is the unique identifier column
        identifier2 = row['transportista_guia']  # Assuming 'id' is the unique identifier column
        
        # Check if the record already exists
        check_query = "SELECT COUNT(*) FROM seguimiento_documento WHERE orden_id = %s AND transportista_guia=%s;"
        cursor.execute(check_query, (identifier1,identifier2,))
        if cursor.fetchone()[0] == 0:
            # Insert the record if it doesn't exist
            print(f"Trying to insert record with identifier1={identifier1} and identifier2={identifier2} into seguimiento_documento...")
            columns = ', '.join(row.keys())
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO seguimiento_documento ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row.values()))
        else:
            print(f"Record with identifier1={identifier1} and identifier2={identifier2} already exists in seguimiento_documento.")
    
    connection.commit()
    cursor.close()
    connection.close()

def main():
    try:
        # Step 1: Extract data from TABLE_A
        print("Extracting data from source...")
        data = extract_data()
        print(f"Extracted {len(data)} rows from source.")

        # Step 2: Dump data into TABLE_B
        if data:
            print("Dumping data into seguimiento_documento...")
            dump_data_to_table_b(data)
            print("Data successfully dumped into seguimiento_documento.")
        else:
            print("No data to dump into seguimiento_documento.")

    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
