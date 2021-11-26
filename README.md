# BigData

<img src="https://user-images.githubusercontent.com/72318314/123131995-8642e280-d44e-11eb-8959-a0ed110a537b.png">

## Requirements / Prerequisites
- [x] [Docker + Docker-Compose](https://www.docker.com/)
- [x] Git

## Run Application
- [x] Clone this repo: `git clone https://github.com/inf19150/BigData && cd BigData/docker`
- [x] Run `docker-compose up -d` for production and `docker-compose up -d -f docker-compose.dev.yml` for dev
- [x] Start hadoop, therefore run `docker-compose up -d`
* wait until `docker logs hadoop` says **Container Startup finished.**
- [x] Run `docker exec -it bash hadoop`, within bash of container execute
* `su hadoop && cd`
* `start-all.sh`
* `hiveserver2`

### Visit UI of components
|Service|URL|
|-|-|
|NodeRed-Editor|[your-host-ip:1880](http://your-host-ip:1880)|
|NodeRed-UI (Frontend)|[your-host-ip:1880/ui](http://your-host-ip:1880/ui)|
|AirFlow|[your-host-ip:8080](http://your-host-ip:8080)|
|HDFS|[your-host-ip:9870](http://your-host-ip:9870)|

### Various credentials
|Service|User|PW|
|-|-|-|
|NodeRed|admin|bigdata2021|
|MySQL|root|bigdata2021|
|MySQL|sqluser|password|

## DAGs & Tasks
### CellCoverage initial
<img src="/doc/screenshots/dag_full.png">
<img src="/doc/screenshots/dag_time_full.png">

### CellCoverage daily
<img src="/doc/screenshots/dag_diff.png">
<img src="/doc/screenshots/dag_time_diff.png">

### Tasks: See also [docker/airflow/airflow/dags](docker/airflow/airflow/dags)

<details>
 
 <summary><b>Tasks of initial DAG</b></summary>
 
    # Create a local direoctory within airflow, where the raw-data gets downloaded to
    create_local_import_dir = BashOperator(
        task_id='create_import_dir',
        bash_command='mkdir -p /home/airflow/ocid/raw',
        dag=initial_dag,
    )


    # Create a corresponding directory, where the extracted raw-data (csv-file) is later being uploaded to
    create_remote_hdfs_dir_raw = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_ocid_raw_dir',
        directory='/user/hadoop/ocid/work/',
        hdfs_conn_id='hdfs',
        dag=initial_dag,
    )


    # Create a corresponding directory, where the reduced final-data (as table) is later being stored to
    create_remote_hdfs_dir_final = HdfsMkdirFileOperator(
        task_id='mkdir_hdfs_ocid_final_dir',
        directory='/user/hadoop/ocid/final/',
        hdfs_conn_id='hdfs',
        dag=initial_dag,
    )


    # Download full database from ocid to local file on airflow fs
    download_initial_dataset = HttpDownloadOperator(
        task_id='download_initial',
        # download_uri='https://opencellid.org/ocid/downloads?token={}&type=full&file=cell_towers.csv.gz'.format(API_KEY),
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

 </details>

<details>
 
 <summary><b>Tasks of daily DAG</b></summary>
 
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

 </details>
 
### Explanation

## Software-Design
### Docker-Stack

<img src="/doc/screenshots/docker_arch.png">

### Frontend

<img src="/doc/screenshots/ui_nodered.png">
<img src="/doc/screenshots/frontend_nodered.png">

<b>Code/Flow of Frontend business-logic, see [docker/nodered/](docker/nodered)</b>

### OpenCellID Crawler-Script
Due to limited api-requests a simple bash-script has been written and is executed every night by a cronjob.

    DAY_STR=$(date +"%Y-%m-%d")
    echo $DAY_STR

    $(rm -r "/home/ubuntu/httpdata/ocid/*")
    $(wget -O "/home/ubuntu/httpdata/ocid/OCID-diff-cell-export-"$DAY_STR"-T000000.csv.gz" "https://opencellid.org/ocid/downloads?token=pk.1d747aefca776719299e26f04d7d331c&type=diff&file=OCID-diff-cell-export-"$DAY_STR"-T000000.csv.gz")
    $(wget -O "/home/ubuntu/httpdata/ocid/cell_towers.csv.gz" "https://opencellid.org/ocid/downloads?token=pk.1d747aefca776719299e26f04d7d331c&type=full&file=cell_towers.csv.gz")

    echo "Done!"

A simple nginx-webserver is used to create a own http-endpoint which can be used by the corresponding DAG-Task.
## Appendix

## OCID-API Structure

<img src="/doc/screenshots/api_structure.png">

## Screenshots

<p>Partitioned and reduced table stored as parquet on HDFS</p>
<img src="/doc/screenshots/hdfs_partition.png">
<p>Working dir on HDFS, where extracted raw-data is stored to and being processed further</p>
<img src="/doc/screenshots/hdfs_work.png">
<p>Count of reduced datasets on final mysql-database, after first initial dag</p>
<img src="/doc/screenshots/end_db_count.png">
<p>Docker-Compose Stack</p>
<img src="/doc/screenshots/docker_stack.png">

## [Prebuild Images](https://hub.docker.com/r/dressni/big_data/tags)

<img src="/doc/screenshots/docker_hub.png">
