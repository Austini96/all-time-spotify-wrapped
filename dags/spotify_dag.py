"""
Spotify Analytics DAG
Runs hourly to extract, load, and transform Spotify listening data
With OpenLineage integration for data lineage tracking
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import TaskInstance
from airflow.lineage.entities import File
from datetime import datetime, timedelta
import sys
import os

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

from spotify_extractor import extract_spotify_data
from duckdb_loader import load_to_duckdb
from utils import validate_env_vars
from load_extended_history import load_extended_streaming_history

def alert_slack_channel(context):
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook_url:
        return
    last_task = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
                    for ti in task_instances 
                    if ti.state == 'failed']
    title = f":red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)} tasks failed)"
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
        'Error': error_message
    }
    msg = '\n'.join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()


def alert_slack_retry(context):
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook_url:
        return
    last_task = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    try_number = context.get('task_instance').try_number
    max_tries = context.get('task_instance').max_tries
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    
    title = f":warning: Task: *{dag_name}.{task_name}* is retrying (attempt {try_number}/{max_tries})"
    msg_parts = {
        'Execution date': execution_date,
        'Task': task_name,
        'Attempt': f"{try_number} of {max_tries}",
        'Error': error_message
    }
    msg = '\n'.join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
    
    # Send to Slack using webhook URL directly
    import requests
    try:
        response = requests.post(
            slack_webhook_url,
            json={"text": msg},
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            print(f"✅ Slack retry alert sent successfully!")
        else:
            print(f"⚠️  Slack retry alert failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending Slack retry alert: {e}")


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': alert_slack_channel,  # Fires after all retries exhausted
    'on_retry_callback': alert_slack_retry,      # Fires on each retry attempt
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'spotify_analytics_pipeline',
    default_args=default_args,
    description='Extract Spotify data, load to DuckDB, transform with dbt',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    tags=['spotify', 'analytics', 'duckdb', 'dbt'],
)

# Task 1: Validate environment
validate_env = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_env_vars,
    dag=dag,
)

# Task 2: Extract from Spotify API
extract_data = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    dag=dag,
)

# Task 3: Load to DuckDB
load_data = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    dag=dag,
)

# Task 4: Load extended streaming history (optional, runs once)
load_extended = PythonOperator(
    task_id='load_extended_history',
    python_callable=load_extended_streaming_history,
    dag=dag,
)

# Task 5: Install dbt dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /opt/airflow/dbt_project && dbt deps --profiles-dir .',
    dag=dag,
)

# Task 5: Run dbt models with OpenLineage tracking
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_project && dbt-ol run --profiles-dir .',
    dag=dag,
)

# Task 6: Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    dag=dag,
)

# Task 8: Generate dbt docs
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow/dbt_project && dbt docs generate --profiles-dir .',
    dag=dag,
)

# Define task dependencies
validate_env >> extract_data

# Both loaders run in parallel
extract_data >> [load_data, load_extended]

# Both loaders must complete before dbt
[load_data, load_extended] >> dbt_deps >> dbt_run >> [dbt_test, dbt_docs]