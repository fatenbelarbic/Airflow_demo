# Now that we’ve generated the queries, it’s time to connect the last piece of the puzzle: Save the data in Postgres


# """
# Documentation of pageview format: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
# """

from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="version_wiki_05",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp", #Path to search for sql file
    max_active_runs=1,
)


def _get_data(year, month, day, hour, output_path, **context):

    import os, ssl
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)
    context["task_instance"].xcom_push(key="url", value=url)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz", bash_command="gunzip --force /tmp/wikipageviews.gz", dag=dag
)


def _fetch_pageviews(pagenames, execution_date, **context):

    xcom_url = context["task_instance"].xcom_pull(
        task_ids="get_data", key="url"
    )
    print(f"Deploying model {xcom_url}")

    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres", # Identifier to credentials to use for connection (stored encrypted in the metastore)
    # BUT we can add the my_postgres connection in Airflow with the help of the CLI.
    sql="postgres_query.sql", # SQL query or path to file containing SQL queries
    dag=dag,
)
# Intricate operations such as setting up a connection to the database and closing it after completion are handled under the hood.

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres


# The argument SQL can be templated and thus a path to a file holding a SQL query can also be provided. 
# Any filepath ending in .sql will be read, templates in the file will be rendered, and the queries in the file will be executed by the PostgresOperator
