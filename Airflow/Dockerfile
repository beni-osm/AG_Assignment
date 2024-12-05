FROM apache/airflow:2.10.3
ADD requirements.txt .
ENV AIRFLOW_HOME=/opt/airflow
COPY dags/ $AIRFLOW_HOME/dags
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt