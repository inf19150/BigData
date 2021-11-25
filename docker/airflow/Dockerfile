FROM marcelmittelstaedt/airflow:latest

COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

#CMD rm -r /home/airflow/BigData
#CMD rm /home/airflow/airflow/python
#CMD rm /home/airflow/airflow/dags
#CMD rm /home/airflow/airflow/plugins

# Copy DAGs, Python scripts & plugins
COPY --chown=airflow ./airflow/dags/ /home/airflow/airflow/dags
COPY --chown=airflow ./airflow/plugins/ /home/airflow/airflow/plugins
COPY --chown=airflow ./airflow/python/ /home/airflow/airflow/python
