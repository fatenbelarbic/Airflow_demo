import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
 
dag = DAG(
  dag_id="version_wiki_01",
  start_date=airflow.utils.dates.days_ago(3),
  schedule_interval="@hourly",
)
 
# Rendering variables at runtime with templating
# The double curly braces denote a Jinja-templated string. Jinja is a templating engine, which replaces variables and/or expressions in a templated string at runtime.
# A number of variables are available at runtime from the task context. One of these variables is execution_date.
get_data = BashOperator(
  task_id="get_data",
  bash_command=(
    "curl -k -o /tmp/wikipageviews.gz "
    "https://dumps.wikimedia.org/other/pageviews/"
    "{{ execution_date.year }}/"                         
    "{{ execution_date.year }}-"
    "{{ '{:02}'.format(execution_date.month) }}/"
    "pageviews-{{ execution_date.year }}"
    "{{ '{:02}'.format(execution_date.month) }}"
    "{{ '{:02}'.format(execution_date.day) }}-"
    "{{ '{:02}'.format(execution_date.hour) }}0000.gz"   
  ),
  dag=dag,
)

# With the BashOperator (and all other operators in Airflow), 
# you provide a string to the bash_command argument (or whatever the argument is named in other operators), which is automatically templated at runtime. 

# PythonOperator is an exception to this standard, because it doesn’t take arguments that can be templated with the runtime context, 
# but instead a python_callable argument in which the runtime context can be applied.