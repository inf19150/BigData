# -*- coding: utf-8 -*-

"""
Title: Simple Example Dag 
Author: Marcel Mittelstaedt
Description: 
Just for educational purposes, not to be used in any productive mannor.
Downloads IMDb data, puts them into HDFS and creates HiveTable.
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
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

daily_dag = DAG(
    'Cell_Coverage_daily',
    default_args=args,
    description='Dag to be executed every day on 08:15, fetching diff',
    schedule_interval='15 08 * * *',
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


# Clear local files within raw-directory on airflow fs
clear_local_raw_import_dir = ClearDirectoryOperator(
    task_id='clear_local_raw_directory',
    directory='/home/airflow/ocid/raw',
    pattern='*',
    dag=daily_dag,
)


"""dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)"""

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


# Initial-Dag flow
create_local_import_dir >> create_remote_hdfs_dir_raw >> create_remote_hdfs_dir_final >> download_initial_dataset >> unzip_initial_dataset >> hdfs_put_ocid_initial >> pyspark_ocid_full_to_final

# Daily-Dag flow
clear_local_raw_import_dir >> download_diff_dataset >> unzip_diff_dataset >> hdfs_put_ocid_diff >> pyspark_ocid_diff_to_final