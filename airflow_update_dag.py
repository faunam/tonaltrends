# pull new data from gdelt
# do transformations, compare to earlier data in postgres to see if need alert (is spark the right place to do that?)
# add new data to postgres
# send alerts with record ids/other info if necessary (maybe return some kind of task ID to airflow 0 or 1? and
#     if 1 it will trigger a sns task) https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html

from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta

import process_new_gdelt_csv

default_args = {
    'owner': 'ubuntu',
    'start_date': datetime(2020, 2, 5),
    'schedule_interval': '*/15 * * * *',  # every 15 min
    'retry_delay': timedelta(seconds=5)
}


# Using the context manager allows you not to duplicate the dag parameter in each operator
with DAG('new_GDELT_csvs', default_args=default_args) as dag:

    # download_csv_task = PythonOperator(
    #     task_id = 'download_csv',
    #     python_callable = download_gdelt_csv.download_unzip_new_csv,
    #     op_kwargs = {'index_url': 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt',
    #                's3_mount_dir': '/home/ubuntu/s3_gdelt_link/'}
    # )

    # to pass variables between airflow tasks: xcom https://airflow.apache.org/docs/stable/concepts.html#xcoms

    process_and_upload_csv_task = PythonOperator(
        task_id='down_up_csv',
        python_callable=process_new_gdelt_csv.down_up,
        op_kwargs={"psql_table": "full_sample"}
    )

    # Use arrows to set dependencies between tasks
    # download_csv_task >> process_and_upload_csv_task

# airflow backfill <dag-name> -s 2020-04-05 <optional end date arg>

# try with interval = @once and see if it works. also test the python file by itself so u can see if it works.
