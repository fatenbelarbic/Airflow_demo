#  After extracting the pageviews, write the pageview counts to a SQL database

# CREATE TABLE pageview_counts (
#    pagename VARCHAR(50) NOT NULL,
#    pageviewcount INT NOT NULL,
#    datetime TIMESTAMP NOT NULL
# );

# Airflow tasks run independently of each other, possibly on different physical machines depending on your setup.
# Therefore they cannot share objects in memory.
# There are two ways of passing data between tasks:
# 1- By using the Airflow metastore to write and read results between tasks. 
# ## This is called XCom which allows storing and later reading any picklable object in the Airflow metastore.
# ## Examples of non-picklable objects are database connections and file handlers. 
# ## Using XComs for storing pickled objects is only suitable for smaller objects. 
# ## Since Airflow’s metastore (typically a MySQL or Postgres database) is finite in size and pickled objects are stored in blobs in the metastore, 
# ## it’s typically advised to apply XComs only for transferring small pieces of data such as a handful of strings (e.g., a list of names).
# 2- By writing results to and from a persistent location (e.g., disk or database) between tasks.
# ## The number of ways to store data are limitless, but typically a file on disk is created.



from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="version_wiki_04",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
)


def _get_data(year, month, day, hour, output_path, **_):

    import os, ssl
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


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

# Running this task will produce a file (/tmp/postgres_query.sql) for the given interval, 
# containing all the SQL queries to be run by the PostgresOperator.
def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0) # Initialize result for all pageviews with zero
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts # Store pageview count

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items(): # For each result, write SQL query
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

get_data >> extract_gz >> fetch_pageviews