# Разработка ETL-процесс для загрузки «банковских» данных из csv-файлов в соответствующие таблицы СУБД PostgreSQL. 
## Структура папок и файлов проекта:
bank_etl/
|
|—— config/
|   |__ .env              # Файл с настройками подключения к БД
|
|—— data/                 # Исходные CSV-файлы
|   |—— ft_balance_f.csv
|   |—— ft_posting_f.csv
|   |—— md_account_d.csv
|   |—— md_currency_d.csv
|   |—— md_exchange_rate_d.csv
|   |__ md_ledger_account_s.csv
|
|—— logs/                 # Логи выполнения (генерируются автоматически)
|
|—— sql/
|   |—— create_schemas.sql    # SQL для создания схем
|   |__ create_tables.sql     # SQL для создания таблиц
|
|—— etl.py               # Основной скрипт ETL-процесса
|—— link.txt             # Файл со ссылкой на видео    
|__ requirements.txt     # Зависимости