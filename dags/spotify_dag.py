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
from emit_lineage import emit_dbt_lineage
from sync_to_postgres import sync_duckdb_to_postgres
from load_extended_history import load_extended_streaming_history

# OpenLineage will automatically track lineage via Airflow's integration
# We'll use inlets/outlets to explicitly declare dataset lineage


def alert_slack_channel(context):
    """Alert to slack channel on failure (after all retries exhausted)"""
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
    """Alert to slack channel on retry"""
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
    # Outlets: datasets this task produces
    outlets=[
        File('/opt/airflow/data/raw/spotify_tracks.csv'),
        File('/opt/airflow/data/raw/spotify_audio_features.csv'),
        File('/opt/airflow/data/raw/spotify_artists.csv'),
    ],
    dag=dag,
)

# Task 3: Load to DuckDB
load_data = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    # Inlets: datasets this task reads from
    inlets=[
        File('/opt/airflow/data/raw/spotify_tracks.csv'),
        File('/opt/airflow/data/raw/spotify_audio_features.csv'),
        File('/opt/airflow/data/raw/spotify_artists.csv'),
    ],
    # Outlets: datasets this task produces (DuckDB tables)
    outlets=[
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_tracks'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_audio_features'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_artists'},
    ],
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

# Task 5: Run dbt models
# Note: dbt-ol doesn't support DuckDB yet, so we use regular dbt
# OpenLineage events will be captured via Airflow's inlet/outlet mechanism
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    # Inlets: raw DuckDB tables
    inlets=[
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_tracks'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_audio_features'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'raw_spotify_artists'},
    ],
    # Outlets: transformed dbt models
    outlets=[
        # Staging models
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'staging.stg_spotify_tracks'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'staging.stg_spotify_audio_features'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'staging.stg_spotify_artists'},
        # Marts models
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'marts.dim_tracks'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'marts.dim_artists'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'marts.dim_albums'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'marts.fct_listening_history'},
        # Analytics models
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'analytics.top_tracks_daily'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'analytics.top_artists_daily'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'analytics.listening_patterns_hourly'},
        {'namespace': 'duckdb://spotify.duckdb', 'name': 'analytics.audio_features_analysis'},
    ],
    dag=dag,
)

# Task 6: Emit lineage events to Marquez
emit_lineage = PythonOperator(
    task_id='emit_lineage',
    python_callable=emit_dbt_lineage,
    dag=dag,
)

# Task 7: Sync data to PostgreSQL for Metabase
sync_postgres = PythonOperator(
    task_id='sync_to_postgres',
    python_callable=sync_duckdb_to_postgres,
    dag=dag,
)

# Task 8: Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    dag=dag,
)

# Task 9: Generate dbt docs
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow/dbt_project && dbt docs generate --profiles-dir .',
    dag=dag,
)

# Define task dependencies
# Parallelization:
# 1. load_data and load_extended run in parallel after extract_data
# 2. dbt_test and dbt_docs run in parallel after sync_postgres

validate_env >> extract_data

# Both loaders run in parallel
extract_data >> [load_data, load_extended]

# Both loaders must complete before dbt
[load_data, load_extended] >> dbt_deps >> dbt_run >> emit_lineage >> sync_postgres

# Tests and docs generation run in parallel
sync_postgres >> [dbt_test, dbt_docs]