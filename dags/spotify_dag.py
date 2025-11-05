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

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts')

import os
from spotify_extractor import extract_spotify_data
from duckdb_loader import load_to_duckdb
from utils import validate_env_vars
from load_extended_history import load_extended_streaming_history


def send_slack_alert(message):
    import requests
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        return
    
    try:
        response = requests.post(webhook_url, json={"text": message}, headers={"Content-Type": "application/json"})
        if response.status_code == 200:
            print(f"✅ Slack alert sent successfully")
        else:
            print(f"⚠️  Slack alert failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error sending Slack alert: {e}")


def alert_slack_channel(context):
    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    
    failed_tasks = [f"<{task_ti.log_url}|{task_ti.task_id}>" for task_ti in dag_run.get_task_instances() if task_ti.state == 'failed']
    error = context.get('exception') or context.get('reason')
    
    msg = f":red_circle: DAG *{ti.dag_id}* failed ({len(failed_tasks)} tasks)\n"
    msg += f"*Execution*: {context.get('execution_date')}\n"
    msg += f"*Failed Tasks*: {', '.join(failed_tasks)}\n"
    msg += f"*Error*: {error}"
    
    send_slack_alert(msg)


def alert_slack_retry(context):
    ti = context.get('task_instance')
    
    msg = f":warning: Task *{ti.dag_id}.{ti.task_id}* retrying (attempt {ti.try_number}/{ti.max_tries})\n"
    msg += f"*Execution*: {context.get('execution_date')}\n"
    msg += f"*Error*: {context.get('exception') or context.get('reason')}"
    
    send_slack_alert(msg)


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
    description='Extract Spotify data, load to DuckDB, transform with dbt. Runs hourly 9 AM-3 PM CST and once at 9 PM CST.',
    schedule_interval='0 15,16,17,18,19,20,21,3 * * *',  # 9 AM-3 PM CST hourly (15-21 UTC) and 9 PM CST (3 UTC)
    catchup=False,
    max_active_runs=1,  # Prevent concurrent DAG runs to avoid DuckDB lock conflicts
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

# Run loaders sequentially to avoid DuckDB locking issues
extract_data >> load_data >> load_extended

# Both loaders must complete before dbt
load_extended >> dbt_deps >> dbt_run >> [dbt_test, dbt_docs]