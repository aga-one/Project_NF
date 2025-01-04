from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

import pandas
from datetime import datetime
import time

PATH = Variable.get("my_path")

conf.set("core", "template_searchpath", PATH)

def insert_data(table_name, encoding = None):
    df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter = ";", encoding=encoding)
    postgres_hook = PostgresHook("postgres-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)

def wait_me(sec):
    time.sleep(sec)

default_args ={
"owner":"agushchin",
"start_date": datetime(2024,12,23),
"retries": 2
}

with DAG(
"insert_data",
default_args = default_args,
description = "Загрузка данных в stage",
catchup=False,
template_searchpath =[PATH],
schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start"
    )

    sql_start_to_log = SQLExecuteQueryOperator(
        task_id="sql_start_to_log",
        conn_id="postgres-db",
        sql=r"""INSERT INTO logs.log_details ("time", run_id, job) 
		VALUES (CURRENT_TIMESTAMP, '{{run_id}}','Starting job');"""
    )
	
    wait_me = PythonOperator(
    	task_id='wait_me',
	python_callable=wait_me,
	op_args = [5]
    )

    split_start = DummyOperator(
        task_id="split_start"
    )

    call_clear_all_data = SQLExecuteQueryOperator(
        task_id="call_clear_all_data",
        conn_id="postgres-db",
        sql="CALL stage.clear_all_data()"
    )
 
    split = DummyOperator(
        task_id="split"
    )
 
    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_balance_f"}
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_posting_f"}
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_account_d"}
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_currency_d","encoding" : "cp1252"}
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_exchange_rate_d"}
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_ledger_account_s"}
    )

    split1 = DummyOperator(
        task_id="split1"
    )

    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f",
        conn_id="postgres-db",
        sql="sql/ft_posting_f.sql"
    )

    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f",
        conn_id="postgres-db",
        sql="sql/ft_balance_f.sql"
    )
    sql_md_account_d = SQLExecuteQueryOperator(
        task_id="sql_md_account_d",
        conn_id="postgres-db",
        sql="sql/md_account_d.sql"
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d",
        conn_id="postgres-db",
        sql="sql/md_currency_d.sql"
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d",
        conn_id="postgres-db",
        sql="sql/md_exchange_rate_d.sql"
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s",
        conn_id="postgres-db",
        sql="sql/md_ledger_account_s.sql"
    )  

    split_data_marts = DummyOperator(
        task_id="split_data_marts"
    )

    call_dm_fill_data_marts_f = SQLExecuteQueryOperator(
        task_id="call_dm_fill_data_marts_f",
        conn_id="postgres-db",
        sql=r"CALL dm.dm_fill_data_marts_f('{{run_id}}')"
    )

    split_end = DummyOperator(
        task_id="split_end"
    )

    sql_end_to_log = SQLExecuteQueryOperator(
        task_id="sql_end_to_log",
        conn_id="postgres-db",
        sql=r"""INSERT INTO logs.log_details ("time", run_id, job) 
		VALUES (CURRENT_TIMESTAMP, '{{run_id}}','Job finished');"""
    )
	
    end = DummyOperator(
        task_id="end"
    )

    (
        start
	>> sql_start_to_log
	>> wait_me
	>> split_start
	>> call_clear_all_data
	>> split
        >> [ft_balance_f,ft_posting_f,md_account_d,md_currency_d,md_exchange_rate_d,md_ledger_account_s]
	>> split1
        >> [sql_ft_posting_f,sql_ft_balance_f,sql_md_account_d,sql_md_currency_d,sql_md_exchange_rate_d,sql_md_ledger_account_s]
        >> split_data_marts
	>> call_dm_fill_data_marts_f
	>> split_end
	>> sql_end_to_log
        >> end
    )
