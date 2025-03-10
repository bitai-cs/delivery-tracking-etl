import logging
import os
from datetime import datetime, timedelta

def setup_logger(module_name):
    now = datetime.now()
    
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
    
    # Eliminar el archivo de log del día anterior
    delete_old_log(module_name, now)
    
    return logging.getLogger()

def delete_old_log(module_name, current_date):
    # Obtener la fecha de ayer
    yesterday = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Nombre del archivo de log de ayer
    old_log_filename = f'logs/etl_{module_name}_{yesterday}.log'
    
    # Eliminar el archivo de log de ayer si existe
    if os.path.exists(old_log_filename):
        os.remove(old_log_filename)