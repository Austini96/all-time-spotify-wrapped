"""
Test DAG to verify Slack alerts are working
This DAG intentionally fails to trigger the Slack notification
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import TaskInstance
from datetime import datetime, timedelta
import os


def alert_slack_channel(context):
    """Alert to slack channel on failure"""
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook_url:
        print("⚠️  SLACK_WEBHOOK_URL not set - cannot send alert")
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
    
    # Send to Slack using webhook URL directly
    import requests
    try:
        response = requests.post(
            slack_webhook_url,
            json={"text": msg},
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            print(f"✅ Slack alert sent successfully!")
        else:
            print(f"⚠️  Slack alert failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending Slack alert: {e}")


def task_that_succeeds():
    """A task that always succeeds"""
    print("✅ This task succeeds!")
    return "Success!"


def task_that_fails():
    """A task that intentionally fails to test Slack alerts"""
    print("💥 About to fail intentionally to test Slack alert...")
    raise Exception("🧪 TEST FAILURE - This is intentional to test Slack notifications!")


def task_with_division_by_zero():
    """A task that fails with a realistic error"""
    print("Attempting a calculation...")
    result = 10 / 0  # This will raise ZeroDivisionError
    return result


# Default arguments with Slack alert on failure
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': alert_slack_channel,  # This triggers Slack on failure
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Don't retry so we see the failure immediately
    'retry_delay': timedelta(minutes=1),
}

# Create test DAG
dag = DAG(
    'test_slack_alert',
    default_args=default_args,
    description='Test DAG to verify Slack failure alerts are working',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'slack', 'monitoring'],
)

# Task 1: Succeeds to show the flow is working
task_success = PythonOperator(
    task_id='task_that_succeeds',
    python_callable=task_that_succeeds,
    dag=dag,
)

# Task 2: Intentionally fails to trigger Slack alert
task_fail = PythonOperator(
    task_id='task_that_fails',
    python_callable=task_that_fails,
    dag=dag,
)

# Set up dependency
task_success >> task_fail

