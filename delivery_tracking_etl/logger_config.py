from delivery_tracking_etl.util_dt import datetime_by_timezone, yesterday_from
import logging
import os
class ETLLogger(logging.Logger):
    def printVerbose(self, param):
        self.verbose(param)
        print(param)
    def printDebug(self, param):
        self.debug(param)
        print(param)
    def printInfo(self, param):
        self.info(param)
        print(param)
    def printWarning(self, param):
        self.warning(param)
        print(param)
    def printError(self, param):
        self.error(param)
        print(param)
    def printCritical(self, param):
        self.critical(param)
        print(param)
    def printException(self, param):
        self.exception(param)
        print(param)

def setup_logger(module_name):
    # Obtener la fecha y hora actual según la zona horaria configurada
    now = datetime_by_timezone()
    # Obtener la fecha actual
    current_date = now.strftime('%Y-%m-%d')
    # Nombre del archivo de log
    log_filename = f'logs/etl_{module_name}_{current_date}.log'

    # Crear el directorio de logs si no existe
    os.makedirs('logs', exist_ok=True)

    # Configurar el logger
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Configurar la clase del logger
    logging.setLoggerClass(ETLLogger)

    # Eliminar el archivo de log del día anterior
    delete_old_log(module_name, now)

    return logging.getLogger(module_name)

def delete_old_log(module_name, current_date):
    # Obtener la fecha de ayer
    yesterday = (yesterday_from(current_date)).strftime('%Y-%m-%d')

    # Nombre del archivo de log de ayer
    old_log_filename = f'logs/etl_{module_name}_{yesterday}.log'

    # Eliminar el archivo de log de ayer si existe
    if os.path.exists(old_log_filename):
        os.remove(old_log_filename)
