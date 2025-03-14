from delivery_tracking_etl.util_classes import OperationResult, DataOperationResult
from delivery_tracking_etl.util_dt import datetime_by_timezone
from delivery_tracking_etl.logger_config import setup_logger
from delivery_tracking_etl.config_db import TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_dt import DT_CONFIG
from delivery_tracking_etl.config_smtp import SMTP_CONFIG
from delivery_tracking_etl.config_sendermail import NOTIFICATION_SENDERMAIL_CONFIG
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import mysql.connector

# Crear el logger usando el nombre del módulo
logger = setup_logger(__name__)
print("Logger created successfully.")

# Función para enviar correo
def send_mail(destinatario, asunto, cuerpo):
    try:
        msg = MIMEMultipart()
        msg['From'] = f"{NOTIFICATION_SENDERMAIL_CONFIG['sender_name']} <{NOTIFICATION_SENDERMAIL_CONFIG['sender_email']}>"
        msg['To'] = destinatario
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'html'))

        with smtplib.SMTP(SMTP_CONFIG['host'], SMTP_CONFIG['port']) as server:
            if SMTP_CONFIG['use_tls']:
                server.starttls()
            server.login(SMTP_CONFIG['username'], SMTP_CONFIG['password'])
            server.send_message(msg)
            print(f"Correo enviado a {destinatario}")
        ### END WITH ###

        return OperationResult(
                success=True,
                status="OK",
                message=f"Correo enviado correctamente a {destinatario}"
            )
    except Exception as e:
        return OperationResult(
            success=False,
            status=e.__module__,
            message=f"Error al enviar correo a {destinatario}: {e}"
        )

# Función para obtener registros pendientes
def obtener_registros_pendientes():
    try:
        conn = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM NOTIF_VW_Notificacion_Cola WHERE notifestado_id = 0"
        cursor.execute(query)
        registros = cursor.fetchall()
        cursor.close()
        conn.close()

        return DataOperationResult(
            success=True,
            status="OK",
            message=f"Se obtuvieron {len(registros)} registros pendientes.",
            data=registros
        )
    except Exception as e:
        return DataOperationResult(
            success=False,
            status="ERROR",
            message=f"Error al obtener registros pendientes: {e}",
            data=None
        )

# Función para actualizar registro
def update_notification(registro_id):
    try:
        conn = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
        cursor = conn.cursor()
        query = """
            UPDATE notificacion_cola
            SET notifestado_id = 1,
                notifcola_fechproceso = %s,
                actusu = 'ETL',
                actfech = %s
            WHERE id = %s
        """
        fecha_actual = datetime.now()
        cursor.execute(query, (fecha_actual, fecha_actual, registro_id))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Registro {registro_id} actualizado")

        return OperationResult(
            success=True,
            status="OK",
            message=f"Registro {registro_id} actualizado correctamente."
        )
    except Exception as e:
        return OperationResult(
            success=False,
            status="ERROR",
            message=f"Error al actualizar registro {registro_id}: {e}"
        )

# Función principal
def main():
    logger.printInfo("NOTIFICACTION PROCCESS STArTING...")
    try:
        dataOperationResult = obtener_registros_pendientes()
        if (not dataOperationResult.OperationSuccessfull):
            logger.printError("Error al obtener registros pendientes!")
            logger.printError(f"Status: {dataOperationResult.status} Message: {dataOperationResult.message}")
            raise Exception("Debido al error en la operación previa el programa no puede continuar!")

        recordCount = len(dataOperationResult.Data)

        logger.printInfo(f"Se obtuvieron {recordCount} notificaciones pendientes.")
        logger.printInfo("Procesando notificaciones...")
        for notification in dataOperationResult.Data:
            notification_id = notification['id']
            receipt = notification['notifcola_email']
            subject = notification['notiftipo_titulo']
            body = notification['notiftipo_descripcion']

            logger.printInfo(f"Enviando correo a {receipt}...")
            operationResult = send_mail(receipt, subject, body)
            if (not operationResult.OperationSuccessfull):
                logger.printError("Error al enviar correo!")
                logger.printError(f"Status: {operationResult.status} Message: {operationResult.message}")
            else:
                logger.printInfo(f"Correo enviado a {receipt}")

            logger.printInfo(f"Actualizando registro {notification_id}...")
            operationResult = update_notification(notification_id)
            if (not operationResult.OperationSuccessfull):
                logger.printError("Error al actualizar notificación!")
                logger.printError(f"Status: {operationResult.status} Message: {operationResult.message}")
            else:
                logger.printInfo(f"Registro {notification_id} actualizado correctamente")
        ### END FOR LOOP ###

        logger.printInfo("NOTIFICACTION PROCCESS COMPLETED.")
    except Exception as e:
        logger.printError("Error en la ejecución del ETL!")
        logger.printException(e)
        logger.printInfo("NOTIFICACTION PROCCESS COMPLETED W/ ERRORS.")

if __name__ == "__main__":
    main()
