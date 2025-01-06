from datetime import datetime
import os
import base64
from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd
import contextlib
from config import USERNAME,USERPASS

engine = create_engine(f'postgresql+psycopg2://{USERNAME}:{base64.b64decode(USERPASS).decode('utf-8')}@localhost:5433/airflow', echo=True)

with contextlib.suppress(FileNotFoundError):
    os.remove('dm_f101_round_f.csv')

with engine.connect() as connection:
    connection.execute(text("""INSERT INTO logs.log_details (run_id, job)
	    VALUES (:run_id, :job);"""),{"run_id" : 'export_data', "job" : 'Exporting CSV'})
    connection.commit()

df = pd.read_sql_query(r'''SELECT
    from_date
    , to_date
    , chapter
    , ledger_account
    , characteristic
    , balance_in_rub
    , balance_in_val
    , balance_in_total
    , turn_deb_rub
    , turn_deb_val
    , turn_deb_total
    , turn_cre_rub
    , turn_cre_val
    , turn_cre_total
    , balance_out_rub
    , balance_out_val
    , balance_out_total
FROM
    dm.dm_f101_round_f;''', engine)

df.to_csv('dm_f101_round_f.csv', index=False)

# Заменяем два произвольных значения в csv
with contextlib.suppress(FileNotFoundError):
    os.remove('dm_f101_round_f_v2.csv')

df = pd.read_csv('dm_f101_round_f.csv')
df._set_value(6, "balance_in_rub", 20000000.22)
df._set_value(12, "turn_cre_total", 5555555.0)
df.to_csv("dm_f101_round_f_v2.csv", index=False)

# загружаем в postgres
with engine.connect() as connection:
    connection.execute(text("""INSERT INTO logs.log_details (run_id, job)
	    VALUES (:run_id, :job);"""),{"run_id" : 'export_data', "job" : 'Importing CSV_v2'})
    connection.commit()

df.to_sql('dm_f101_round_f_v2', engine, schema="dm", if_exists="replace", index=False)

connection.close() 
engine.dispose()