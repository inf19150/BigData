# -*- coding: utf-8 -*-

"""
Title: OCID Daily-Dag
Author: Nick Dressler (6870655)
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator

args = {
    'owner': 'airflow'
}

daily_dag = DAG(
    'Cell_Coverage_daily',
    default_args=args,
    description='Dag to be executed every day on 08:15, fetching diff',
    schedule_interval='15 08 * * *',
    start_date=datetime(2021, 11, 25),
    catchup=False,
    max_active_runs=1
)

# Download diff database from ocid to local file on airflow fs
download_diff_dataset = HttpDownloadOperator(
    task_id='download_diff',
    #download_uri='https://opencellid.org/ocid/downloads?token={}&type=diff&file=OCID-diff-cell-export-{{ ds }}-T000000.csv.gz'.format(API_KEY),
    download_uri='http://193.196.53.117/ocid/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
    save_to='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv.gz',
    dag=daily_dag,
)


# Unzip diff database tgz-file to csv file on airflow fs
unzip_diff_dataset = UnzipFileOperator(
    task_id='unzip_diff',
    zip_file='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv.gz',
    extract_to='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv',
    dag=daily_dag,
)

# Clear local files within raw-directory on airflow fs
clear_local_raw_import_dir = ClearDirectoryOperator(
    task_id='clear_local_raw_directory',
    directory='/home/airflow/ocid/raw',
    pattern='*',
    dag=daily_dag,
)

# Move diff database to remote hdfs
hdfs_put_ocid_diff = HdfsPutFileOperator(
    task_id='upload_ocid_diff_hdfs',
    local_file='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv',
    remote_file='/user/hadoop/ocid/work/ocid_diff_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=daily_dag,
)

pyspark_ocid_diff_to_final = SparkSubmitOperator(
    task_id='pyspark_filter_reduce_diff_write_to_final_parquet',
    conn_id='spark',
    application='/home/airflow/airflow/python/ocid_diff_to_final_db.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_raw_to_final_diff',
    verbose=True,
    application_args=[
        '--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
        '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
        '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
        '--hdfs_source_dir', '/user/hadoop/ocid/work/',
        '--hdfs_target_dir', '/user/hadoop/ocid/final/',
    ],
    dag=daily_dag
)

# Daily-Dag flow
clear_local_raw_import_dir
clear_local_raw_import_dir >> download_diff_dataset >> unzip_diff_dataset
unzip_diff_dataset >> hdfs_put_ocid_diff >> pyspark_ocid_diff_to_final
