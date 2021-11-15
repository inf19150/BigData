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
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'airflow'
}

API_KEY = 'pk.1d747aefca776719299e26f04d7d331c'


hiveSQL_create_table_ocid='''
CREATE EXTERNAL TABLE IF NOT EXISTS ocid(
	radio STRING,
	mcc STRING,
	net STRING,
	area STRING,
	cell DECIMAL(1,0),
	unit DECIMAL(4,0),
	lon STRING,
	lat INT,
	range STRING,
	samples INT,
	changeable STRING,
	created INT,
	updated INT,
	averageSignal INT
) COMMENT 'OCID_full_data' PARTITIONED BY (radio STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/ocid/work'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_add_partition_title_ratings='''
ALTER TABLE title_ratings
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/imdb/title_ratings/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/';
'''

hiveSQL_create_top_tvseries_external_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS top_tvseries (
    original_title STRING, 
    start_year DECIMAL(4,0), 
    end_year STRING,  
    average_rating DECIMAL(2,1), 
    num_votes BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb_final/top_tvseries';
'''

hiveQL_create_top_movies_external_table='''
CREATE TABLE IF NOT EXISTS top_movies (
    original_title STRING, 
    start_year DECIMAL(4,0), 
    average_rating DECIMAL(2,1), 
    num_votes BIGINT
) STORED AS ORCFILE LOCATION '/user/hadoop/imdb_final/top_movies';
'''

hiveSQL_insertoverwrite_top_movies_table='''
INSERT OVERWRITE TABLE top_movies
SELECT
    m.original_title,
    m.start_year,
    r.average_rating,
    r.num_votes
FROM
    title_basics m
    JOIN title_ratings r ON (m.tconst = r.tconst)
WHERE
    m.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and m.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and m.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and r.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and r.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.num_votes > 200000 AND r.average_rating > 8.6
    AND m.title_type = 'movie' AND m.start_year > 2000
'''

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
    description='Dag to be executed every day after 00:00, fetching diff',
    schedule_interval='15 08 * * *',
	start_date=datetime(2021, 11, 15),
    catchup=False,
    max_active_runs=1
)



create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow/',
    directory='ocid',
    dag=initial_dag,
)

create_local_raw_import_dir = CreateDirectoryOperator(
    task_id='create_raw_import_dir',
    path='/home/airflow/ocid',
    directory='raw',
    dag=initial_dag,
)

clear_local_raw_import_dir = ClearDirectoryOperator(
    task_id='clear_raw_import_dir',
    directory='/home/airflow/ocid/raw',
    pattern='*',
    dag=daily_dag,
)

download_initial_dataset = HttpDownloadOperator(
    task_id='download_initial',
    #download_uri='https://opencellid.org/ocid/downloads?token={}&type=full&file=cell_towers.csv.gz'.format(API_KEY),
	download_uri='http://193.196.53.117/ocid/cell_towers.csv.gz'.format(API_KEY),
    save_to='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv.gz',
    dag=initial_dag,
)

download_current_diff = HttpDownloadOperator(
    task_id='download_current_diff',
    #download_uri='https://opencellid.org/ocid/downloads?token={}&type=diff&file=OCID-diff-cell-export-{{ ds }}-T000000.csv.gz'.format(API_KEY),
    download_uri='http://193.196.53.117/ocid/OCID-diff-cell-export-{{ ds }}-T000000.csv.gz',
	save_to='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv.gz',
    dag=daily_dag,
)

unzip_initial_dataset = UnzipFileOperator(
    task_id='unzip_initial',
    zip_file='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv.gz',
    extract_to='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv',
    dag=initial_dag,
)

unzip_current_diff = UnzipFileOperator(
    task_id='unzip_current_diff',
    zip_file='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv.gz',
    extract_to='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv',
    dag=daily_dag,
)

create_hdfs_ocid_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_ocid_dir',
    directory='/user/hadoop/ocid/work/',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)

hdfs_put_ocid_initial = HdfsPutFileOperator(
    task_id='upload_ocid_diff_hdfs',
    local_file='/home/airflow/ocid/raw/ocid_full_{{ ds }}.csv',
    remote_file='/user/hadoop/ocid/work/ocid_full_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=initial_dag,
)

hdfs_put_ocid_diff= HdfsPutFileOperator(
    task_id='upload_ocid_diff_hdfs',
    local_file='/home/airflow/ocid/raw/ocid_diff_{{ ds }}.csv',
    remote_file='/user/hadoop/ocid/work/ocid_diff_{{ ds }}.csv',
    hdfs_conn_id='hdfs',
    dag=daily_dag,
)

create_HiveTable_title_ratings = HiveOperator(
    task_id='create_ocid_table',
    hql=hiveSQL_create_table_ocid,
    hive_cli_conn_id='beeline',
    dag=initial_dag)

"""
addPartition_HiveTable_title_ratings = HiveOperator(
    task_id='add_partition_title_ratings_table',
    hql=hiveSQL_add_partition_title_ratings,
    hive_cli_conn_id='beeline',
    dag=dag)

dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

pyspark_top_tvseries = SparkSubmitOperator(
    task_id='pyspark_write_top_tvseries_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_top_tvseries.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_calculate_top_tvseries',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/imdb', '--hdfs_target_dir', '/user/hadoop/imdb_final/top_tvseries', '--hdfs_target_format', 'csv'],
    dag = dag
)

create_table_for_top_tvseries = HiveOperator(
    task_id='create_top_tvseries_external_table',
    hql=hiveSQL_create_top_tvseries_external_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_HiveTable_top_movies = HiveOperator(
    task_id='create_top_movies_external_table',
    hql=hiveQL_create_top_movies_external_table,
    hive_cli_conn_id='beeline',
    dag=dag)

hive_insert_overwrite_top_movies = HiveOperator(
    task_id='hive_write_top_movies_table',
    hql=hiveSQL_insertoverwrite_top_movies_table,
    hive_cli_conn_id='beeline',
    dag=dag)

create_local_import_dir >> clear_local_import_dir 
clear_local_import_dir >> download_title_ratings >> unzip_title_ratings >> create_hdfs_title_ratings_partition_dir >> hdfs_put_title_ratings >> create_HiveTable_title_ratings >> addPartition_HiveTable_title_ratings >> dummy_op
clear_local_import_dir >> download_title_basics >> unzip_title_basics >> create_hdfs_title_basics_partition_dir >> hdfs_put_title_basics >> create_HiveTable_title_basics >> addPartition_HiveTable_title_basics >> dummy_op
dummy_op >> pyspark_top_tvseries >> create_table_for_top_tvseries
dummy_op >> create_HiveTable_top_movies >> hive_insert_overwrite_top_movies
"""

create_local_import_dir >> create_local_raw_import_dir >> download_initial_dataset >> unzip_initial_dataset >> create_hdfs_ocid_partition_dir >> hdfs_put_ocid_initial >> create_HiveTable_title_ratings

clear_local_raw_import_dir >> download_current_diff >> unzip_current_diff >> hdfs_put_ocid_diff