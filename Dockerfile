FROM apache/airflow:2.0.1
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user nano
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN python3 -m pip install apache-airflow[postgres,s3,aws,azure,mssql,sendgrid]