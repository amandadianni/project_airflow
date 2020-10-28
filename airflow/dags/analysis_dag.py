from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from data_analysis import read_csv


dag = DAG('python_dag',
          description='Python DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2018, 11, 1),
          catchup=False)

read_csv_task = PythonOperator(
    task_id='read_csv_task',
    python_callable=read_csv,
    # op_args = ['one', 'two', 'three'],
    dag=dag
)

# task_instance = kwargs['task_instance']
# task_instance.xcom_pull(task_ids='Task1')
# def Task1(**kwargs):
#     file_name = kwargs['dag_run'].conf.get[file]
#     task_instance = kwargs['task_instance']
#     task_instance.xcom_push(key='file', value=file_name)
#     return file_name
# 1. OK separar o tratamento em outro arquivo
# 2. OK PATH relativo do arquivo csv
# 3. mais análises
# 4. tentar extrair trechos em tasks separadas
# 5. exportar csv com os dados tratados
