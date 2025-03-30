from delivery_tracking_etl.util_dt import datetime_by_timezone
from delivery_tracking_etl.util_classes import OperationResult, DataOperationResult
from delivery_tracking_etl.logger_config import setup_logger
from delivery_tracking_etl.config_db import TRGT_DB_CONNECTION_CONFIG
from delivery_tracking_etl.config_smtp import SMTP_CONFIG
from delivery_tracking_etl.config_sendermail import NOTIFICATION_SENDERMAIL_CONFIG
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import smtplib
import mysql.connector

# Crear el logger usando el nombre del módulo
logger = setup_logger(__name__)
print("Logger created successfully.")

# Función para enviar correo
def sendMail(mailRecipient, mailSubject, mailBody):
    try:
        msg = MIMEMultipart()
        msg['From'] = f"{NOTIFICATION_SENDERMAIL_CONFIG['NOTIF_SENDERNAME']} <{NOTIFICATION_SENDERMAIL_CONFIG['NOTIF_SENDERMAIL']}>"
        msg['To'] = mailRecipient
        msg['Subject'] = mailSubject
        msg.attach(MIMEText(mailBody, 'html'))

        with smtplib.SMTP(SMTP_CONFIG['SMTP_HOST'], SMTP_CONFIG['SMTP_PORT']) as server:
            if SMTP_CONFIG['SMTP_USETLS']:
                server.starttls()
            server.login(SMTP_CONFIG['SMTP_USER'], SMTP_CONFIG['SMTP_PASSWORD'])
            server.send_message(msg)
        ### END WITH ###

        return OperationResult(
                success=True,
                status="OK",
                message=f"Correo enviado correctamente a {mailRecipient}"
            )
    except Exception as e:
        return OperationResult(
            success=False,
            status=e.__module__,
            message=f"Error al enviar correo a {mailRecipient}: {e}"
        )

# Función para obtener registros pendientes
def getPendingNotifications():
    logger.printInfo("Obteniendo registros pendientes...")
    try:
        logger.printInfo("Conectando a la base de datos...")
        conn = mysql.connector.connect(**TRGT_DB_CONNECTION_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM NOTIF_VW_Notificacion_Cola WHERE notifestado_id = 0"
        logger.printInfo("Ejecutando consulta...")
        cursor.execute(query)
        registros = cursor.fetchall()
        logger.printInfo("Recuperando resultados de la consulta.")
        cursor.close()
        conn.close()
        logger.printInfo("Conexión a la base de datos cerrada.")

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

def loadMailTemplate(templateData):
    # Leer el contenido del archivo HTML
    templateContent = Path('delivery_tracking_etl/templates/notification_deliverycompleted_template.html').read_text(encoding='utf-8')

    # Reemplazar los placeholders con los datos reales
    for key, value in templateData.items():
        templateContent = templateContent.replace(f'{{{{{key}}}}}', value)

    return templateContent

# Función para actualizar registro
def updateNotificationRecord(notificationId):
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
        fecha_actual = datetime_by_timezone()
        cursor.execute(query, (fecha_actual, fecha_actual, notificationId))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Registro {notificationId} actualizado")

        return OperationResult(
            success=True,
            status="OK",
            message=f"Registro {notificationId} actualizado correctamente."
        )
    except Exception as e:
        return OperationResult(
            success=False,
            status="ERROR",
            message=f"Error al actualizar registro {notificationId}: {e}"
        )

# Función principal
def main():
    logger.printInfo("NOTIFICACTION PROCCESS STARTING...")
    try:
        #Review loaded environment variables
        logger.printInfo("Review loaded TRGT_DB_CONNECTION_CONFIG:")
        for key, value in TRGT_DB_CONNECTION_CONFIG.items():
            if key == 'password':
                value = '*** hiden ***'
            logger.printInfo(f"{key}: {value}")
        logger.printInfo("Review loaded SMTP_CONFIG:")
        for key, value in SMTP_CONFIG.items():
            if key == 'SMTP_PASSWORD':
                value = '*** hiden ***'
            logger.printInfo(f"{key}: {value}")
        logger.printInfo("Review loaded NOTIFICATION_SENDERMAIL_CONFIG:")
        for key, value in NOTIFICATION_SENDERMAIL_CONFIG.items():
            logger.printInfo(f"{key}: {value}")

        dataOperationResult = getPendingNotifications()
        if (not dataOperationResult.OperationSuccessfull):
            logger.printError("Error al obtener registros pendientes!")
            logger.printError(f"Status: {dataOperationResult.status} Message: {dataOperationResult.message}")
            raise Exception("Debido al error en la operación previa el programa no puede continuar!")

        recordCount = len(dataOperationResult.Data)

        logger.printInfo(f"Se obtuvieron {recordCount} notificaciones pendientes.")
        logger.printInfo("Procesando notificaciones...")
        for notification in dataOperationResult.Data:
            notification_id = notification['id']
            recipient = notification['notifcola_email']
            subject = notification['notiftipo_titulo']

            templateData = {
                'customer_name': notification['remitente_razonsocial'],
                'customer_so': notification['orden_servicio'],
                'company_address': 'Av. Jose Galvez 560, La Victoria.',
                'collection_date': notification['completado_fecha'].strftime('%d/%m/%Y'),
                'company_business_hours': '10:00 AM a 18:00 PM',
                'business_phones': '959 959 010 o al 959 959 032',
                'company_name': 'Walla Express',
                'so_link': f'http://159.203.97.223:8002/CustomerSupport/Details/{notification["seguimiento_id"]}'
            }
            mailBody = loadMailTemplate(templateData)

            logger.printInfo(f"Enviando correo a {recipient}...")
            operationResult = sendMail(recipient, subject, mailBody)
            if (not operationResult.OperationSuccessfull):
                logger.printError("Error al enviar correo!")
                logger.printError(f"Status: {operationResult.status} Message: {operationResult.message}")
            else:
                logger.printInfo(f"Correo enviado a {recipient}")

            logger.printInfo(f"Actualizando registro {notification_id}...")
            operationResult = updateNotificationRecord(notification_id)
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
