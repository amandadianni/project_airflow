from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import data_analysis as da


dag = DAG('data_analysis_dag',
          description='Data Analysis DAG',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2020, 11, 1),
          catchup=False)

read_csv_task = PythonOperator(
    task_id='read_csv_task',
    python_callable=da.read_csv,
    dag=dag
)

export_cleared_csv_task = PythonOperator(
    task_id='export_cleared_csv_task',
    python_callable=da.export_cleared_csv,
    provide_context=True,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data_task',
    python_callable=da.clean_data,
    provide_context=True,
    dag=dag
)

count_per_type_task = PythonOperator(
    task_id='count_per_type_task',
    python_callable=da.count_per_type,
    provide_context=True,
    dag=dag
)

installs_per_category_task = PythonOperator(
    task_id='installs_per_category_task',
    python_callable=da.installs_per_category,
    provide_context=True,
    dag=dag
)

apps_per_android_version_task = PythonOperator(
    task_id='apps_per_android_version_task',
    python_callable=da.apps_per_android_version,
    provide_context=True,
    dag=dag
)

review_5_count_per_category_task = PythonOperator(
    task_id='review_5_count_per_category_task',
    python_callable=da.review_5_count_per_category,
    provide_context=True,
    dag=dag
)

review_1_count_per_category_task = PythonOperator(
    task_id='review_1_count_per_category_task',
    python_callable=da.review_1_count_per_category,
    provide_context=True,
    dag=dag
)

read_csv_task >> clean_data_task >> [
    count_per_type_task,
    installs_per_category_task,
    apps_per_android_version_task,
    review_5_count_per_category_task,
    review_1_count_per_category_task
] >> export_cleared_csv_task
