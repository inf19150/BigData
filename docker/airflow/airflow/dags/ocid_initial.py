# -*- coding: utf-8 -*-

"""
Title: OCID Initial-Dag
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

initial_dag = DAG(
    'Cell_Coverage_initial',
    default_args=args,
    description='Initial DAG of OpenCellID (fetching initial dataset)',
    schedule_interval='@once',
	start_date=datetime(2021, 11, 15),
    catchup=False,
    max_active_runs=1
)


# Tasks tied to this dag.

# Create a local direoctory within airflow, where the raw-data gets downloaded to
create_local_import_dir = BashOperator(
    task_id='create_import_dir',
    bash_command='mkdir -p /home/airflow/ocid/raw',
    dag=initial_dag,
)


# Create a corresponding directory, where the extracted raw-data (csv-file) is later being uploaded to
create_remote_hdfs_dir_raw= HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_ocid_raw_dir',
    directory='/user/hadoop/ocid/work/',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)


# Create a corresponding directory, where the reduced final-data (as table) is later being stored to
create_remote_hdfs_dir_final= HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_ocid_final_dir',
    directory='/user/hadoop/ocid/final/',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)


# Download full database from ocid to local file on airflow fs
download_initial_dataset = HttpDownloadOperator(
    task_id='download_initial',
    #download_uri='https://opencellid.org/ocid/downloads?token={}&type=full&file=cell_towers.csv.gz'.format(API_KEY),
	download_uri='http://193.196.53.117/ocid/cell_towers.csv.gz',
    save_to='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv.gz',
    dag=initial_dag,
)


# Unzip full database tgz-file to csv file on airflow fs
unzip_initial_dataset = UnzipFileOperator(
    task_id='unzip_initial',
    zip_file='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv.gz',
    extract_to='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv',
    dag=initial_dag,
)


# Move extracted full database to remote hdfs
hdfs_put_ocid_initial = HdfsPutFileOperator(
    task_id='upload_ocid_full_hdfs',
    local_file='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv',
    remote_file='/user/hadoop/ocid/work/ocid_full_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)


# Move diff database to remote hdfs
hdfs_put_ocid_diff= HdfsPutFileOperator(
    task_id='upload_ocid_diff_hdfs',
    local_file='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv',
    remote_file='/user/hadoop/ocid/work/ocid_diff_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)

pyspark_ocid_full_to_final = SparkSubmitOperator(
    task_id='pyspark_filter_reduce_full_write_to_final_parquet',
    conn_id='spark',
    application='/home/airflow/airflow/python/ocid_full_to_final_db.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_raw_to_final_full',
    verbose=True,
    application_args=[
					'--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}',
					'--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}',
					'--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
					'--hdfs_source_dir', '/user/hadoop/ocid/work/',
					'--hdfs_target_dir', '/user/hadoop/ocid/final/',
					],
    dag=initial_dag
)


# Initial-Dag flow
create_local_import_dir 

create_local_import_dir >> create_remote_hdfs_dir_raw >> create_remote_hdfs_dir_final
create_local_import_dir >> download_initial_dataset >> unzip_initial_dataset 

unzip_initial_dataset >> hdfs_put_ocid_initial >> pyspark_ocid_full_to_final