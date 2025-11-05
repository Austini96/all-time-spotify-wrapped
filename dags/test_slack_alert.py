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


def _send_slack_message(message):
    """Helper function to send message to Slack"""
    import requests
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("⚠️  SLACK_WEBHOOK_URL not set - cannot send alert")
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
    """Send Slack notification when DAG fails after all retries"""
    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    
    failed_tasks = [f"<{task_ti.log_url}|{task_ti.task_id}>" for task_ti in dag_run.get_task_instances() if task_ti.state == 'failed']
    error = context.get('exception') or context.get('reason')
    
    msg = f":red_circle: DAG *{ti.dag_id}* failed ({len(failed_tasks)} tasks)\n"
    msg += f"*Execution*: {context.get('execution_date')}\n"
    msg += f"*Failed Tasks*: {', '.join(failed_tasks)}\n"
    msg += f"*Error*: {error}"
    
    _send_slack_message(msg)


def alert_slack_retry(context):
    """Send Slack notification when task is retrying"""
    ti = context.get('task_instance')
    
    msg = f":warning: Task *{ti.dag_id}.{ti.task_id}* retrying (attempt {ti.try_number}/{ti.max_tries})\n"
    msg += f"*Execution*: {context.get('execution_date')}\n"
    msg += f"*Error*: {context.get('exception') or context.get('reason')}"
    
    _send_slack_message(msg)


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


# Default arguments with Slack alert on failure and retry
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': alert_slack_channel,  # Fires after all retries exhausted
    'on_retry_callback': alert_slack_retry,      # Fires on each retry attempt
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Retry twice so you can test retry notifications
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

