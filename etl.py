import os
import logging
import time
from datetime import datetime
import pandas as pd
from io import StringIO
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv

# Настройка для Windows
if os.name == 'nt':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    CONFIG_DIR = os.path.join(BASE_DIR, 'config')
    
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(CONFIG_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'etl.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(os.path.join(CONFIG_DIR, '.env'))

# Параметры подключения к БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'bank_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
    'port': os.getenv('DB_PORT', '5432')
}

# Сопоставление таблиц и файлов
TABLE_FILE_MAPPING = {
    'ds.ft_balance_f': os.path.join(DATA_DIR, 'ft_balance_f.csv'),
    'ds.ft_posting_f': os.path.join(DATA_DIR, 'ft_posting_f.csv'),
    'ds.md_account_d': os.path.join(DATA_DIR, 'md_account_d.csv'),
    'ds.md_currency_d': os.path.join(DATA_DIR, 'md_currency_d.csv'),
    'ds.md_exchange_rate_d': os.path.join(DATA_DIR, 'md_exchange_rate_d.csv'),
    'ds.md_ledger_account_s': os.path.join(DATA_DIR, 'md_ledger_account_s.csv')
}

# Первичные ключи для каждой таблицы
PRIMARY_KEYS = {
    'ds.ft_balance_f': ['on_date', 'account_rk'],
    'ds.ft_posting_f': None,
    'ds.md_account_d': ['data_actual_date', 'account_rk'],
    'ds.md_currency_d': ['currency_rk', 'data_actual_date'],
    'ds.md_exchange_rate_d': ['data_actual_date', 'currency_rk'],
    'ds.md_ledger_account_s': ['ledger_account', 'start_date']
}

def create_connection():
    """Создание подключения к PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        logger.info("Успешное подключение к базе данных")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

def execute_sql(conn, query, params=None):
    """Выполнение SQL-запроса"""
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, params or ())
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка выполнения запроса: {e}\nQuery: {query}")
            raise

def log_etl_start(conn, table_name):
    """Логирование начала ETL-процесса"""
    query = """
    INSERT INTO logs.etl_logs 
    (table_name, start_time, status, records_processed) 
    VALUES (%s, %s, %s, %s)
    RETURNING log_id;
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(query, (table_name, datetime.now(), 'started', 0))
            log_id = cursor.fetchone()[0]
            conn.commit()
            return log_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при логировании начала процесса: {e}")
            raise

def log_etl_end(conn, log_id, table_name, status, records_processed, error_msg=None):
    """Логирование завершения ETL-процесса"""
    query = """
    UPDATE logs.etl_logs 
    SET end_time = %s, status = %s, records_processed = %s, error_message = %s
    WHERE log_id = %s;
    """
    execute_sql(conn, query, (datetime.now(), status, records_processed, error_msg, log_id))

def read_csv_with_encoding_tries(file_path):
    """Чтение CSV с попытками разных кодировок (финальная версия)"""
    encodings = ['utf-8-sig', 'cp1251', 'latin1', 'utf-16-le']
    
    for encoding in encodings:
        try:
            if encoding == 'utf-16-le':
                with open(file_path, 'rb') as f:
                    content = f.read()
                    if content.startswith(b'\xff\xfe'):
                        content = content[2:]
                    decoded = content.decode('utf-16-le')
                    df = pd.read_csv(StringIO(decoded), sep=';')
            else:
                df = pd.read_csv(file_path, sep=';', encoding=encoding)
            
            logger.info(f"Файл {file_path} прочитан с кодировкой {encoding}")
            return df
        except Exception as e:
            logger.warning(f"Не удалось прочитать с кодировкой {encoding}: {str(e)}")
            continue
    
    raise ValueError(f"Не удалось прочитать файл {file_path} ни с одной из кодировок: {encodings}")

def load_data_from_csv(conn, table_name, file_path):
    """Загрузка данных из CSV в таблицу"""
    logger.info(f"Начало загрузки данных в таблицу {table_name} из файла {file_path}")
    
    log_id = log_etl_start(conn, table_name)
    error_msg = None
    records_processed = 0
    
    try:
        # Чтение CSV-файла
        df = read_csv_with_encoding_tries(file_path)
        
        # Нормализация названий столбцов
        df.columns = df.columns.str.strip().str.lower()
        
        # Обработка для таблицы балансов
        if table_name == 'ds.ft_balance_f':
            df['on_date'] = pd.to_datetime(df['on_date'], format='%d.%m.%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Специальная обработка для таблицы валют
        if table_name == 'ds.md_currency_d':
            str_cols = ['currency_code', 'code_iso_char']
            for col in str_cols:
                if col in df.columns:
                    # Преобразуем все значения в строки и заменяем пропуски на None
                    df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>'], None)
                    # Обрезаем только строковые значения (не None)
                    df[col] = df[col].apply(lambda x: x[:3] if x is not None and isinstance(x, str) else x)
        
        # Для таблицы проводок очищаем данные перед загрузкой
        if table_name == 'ds.ft_posting_f':
            truncate_query = "TRUNCATE TABLE ds.ft_posting_f;"
            execute_sql(conn, truncate_query)
            logger.info(f"Таблица {table_name} очищена перед загрузкой новых данных")
        
        # Преобразование DataFrame в список кортежей, заменяя NaN на None
        data_tuples = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
        
        # Получение списка колонок
        columns = df.columns.tolist()
        columns_str = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Формирование SQL-запроса для вставки
        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        """
        
        # Для таблиц с первичными ключами добавляем ON CONFLICT
        if PRIMARY_KEYS.get(table_name):
            conflict_columns = ', '.join([f'"{col}"' for col in PRIMARY_KEYS[table_name]])
            update_columns = ', '.join(
                [f'"{col}" = EXCLUDED."{col}"' 
                 for col in columns if col not in PRIMARY_KEYS[table_name]]
            )
            insert_query += f"""
            ON CONFLICT ({conflict_columns}) 
            DO UPDATE SET {update_columns}
            """
        
        # Выполнение массовой вставки
        with conn.cursor() as cursor:
            cursor.execute("BEGIN;")
            
            try:
                extras.execute_batch(
                    cursor,
                    insert_query,
                    data_tuples,
                    page_size=1000
                )
                
                records_processed = len(data_tuples)
                conn.commit()
                logger.info(f"Успешно загружено {records_processed} записей в таблицу {table_name}")
            
            except Exception as e:
                conn.rollback()
                error_msg = str(e)
                logger.error(f"Ошибка при вставке данных: {error_msg}")
                # Дополнительная диагностика
                if table_name == 'ds.md_currency_d':
                    logger.info("Примеры проблемных данных:")
                    sample = df[['currency_code', 'code_iso_char']].head(5)
                    logger.info(sample.to_string())
                raise
    
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ошибка при загрузке данных в таблицу {table_name}: {error_msg}")
        raise
    
    finally:
        status = 'completed' if error_msg is None else 'failed'
        log_etl_end(conn, log_id, table_name, status, records_processed, error_msg)
    
    return records_processed

def check_files_exist():
    """Проверка наличия всех необходимых файлов"""
    missing_files = []
    for table_name, file_path in TABLE_FILE_MAPPING.items():
        if not os.path.exists(file_path):
            missing_files.append(file_path)
            logger.warning(f"Файл не найден: {file_path}")
    
    if missing_files:
        logger.error(f"Отсутствуют следующие файлы: {', '.join(missing_files)}")
        return False
    return True

def main():
    """Основная функция ETL-процесса"""
    conn = None
    try:
        logger.info("Начало ETL-процесса")
        
        # Проверка наличия файлов
        if not check_files_exist():
            return
        
        # Подключение к БД
        conn = create_connection()   
      
        # Искусственная задержка для демонстрации
        logger.info("Искусственная задержка 5 секунд...")
        time.sleep(5)
        
        # Загрузка данных для каждой таблицы
        for table_name, file_path in TABLE_FILE_MAPPING.items():
            try:
                load_data_from_csv(conn, table_name, file_path)
            except Exception as e:
                logger.error(f"Прерывание загрузки таблицы {table_name} из-за ошибки: {e}")
                continue  # Продолжаем с другими таблицами
        
        logger.info("ETL-процесс успешно завершен")
    
    except Exception as e:
        logger.error(f"Критическая ошибка в ETL-процессе: {e}")
    finally:
        if conn is not None:
            conn.close()
            logger.info("Соединение с базой данных закрыто")

if __name__ == "__main__":
    # Для Windows: установка корректной кодировки консоли
    if os.name == 'nt':
        import sys
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    main()