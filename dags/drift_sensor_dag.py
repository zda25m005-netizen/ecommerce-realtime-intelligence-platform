"""
DAG 2: Drift Sensor - Layer 4
Schedule: Every 30 minutes
Monitors CTR. If recommendations stop performing (CTR drops below threshold),
triggers emergency retraining of the ALS model.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import requests
import json

SPARK_MASTER = "spark://spark-master:7077"
CTR_THRESHOLD = 0.02  # 2% minimum CTR
PROMETHEUS_URL = "http://prometheus:9090"

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.0.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)

default_args = {
    'owner': 'gaurav_jha',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2026, 3, 1),
}


def check_ctr_drift(**context):
    """
    Query Prometheus for current CTR metric.
    If CTR < threshold, trigger retraining.
    """
    try:
        query = 'avg(recommendation_ctr_rate) over (last 1h)'
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={'query': 'recommendation_ctr_rate'},
            timeout=10,
        )
        data = response.json()

        if data['status'] == 'success' and data['data']['result']:
            current_ctr = float(data['data']['result'][0]['value'][1])
            print(f"Current CTR: {current_ctr:.4f} (threshold: {CTR_THRESHOLD})")
            context['task_instance'].xcom_push(key='current_ctr', value=current_ctr)

            if current_ctr < CTR_THRESHOLD:
                print(f"CTR DRIFT DETECTED! {current_ctr:.4f} < {CTR_THRESHOLD}")
                return 'trigger_emergency_retrain'
            else:
                print("CTR is healthy, no action needed.")
                return 'skip_retrain'
        else:
            print("No CTR data available yet, skipping check.")
            return 'skip_retrain'

    except Exception as e:
        print(f"Error querying Prometheus: {e}")
        return 'skip_retrain'


with DAG(
    dag_id='drift_sensor_auto_retrain',
    default_args=default_args,
    description='Monitor CTR drift, trigger emergency retrain if needed',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'ml', 'drift'],
) as dag:

    # ── Check CTR drift ──────────────────────────────────────────────────
    check_drift = BranchPythonOperator(
        task_id='check_ctr_drift',
        python_callable=check_ctr_drift,
    )

    # ── Branch: Skip retrain (CTR is healthy) ────────────────────────────
    skip_retrain = EmptyOperator(
        task_id='skip_retrain',
    )

    # ── Branch: Emergency retrain ────────────────────────────────────────
    emergency_retrain = BashOperator(
        task_id='trigger_emergency_retrain',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'--packages {SPARK_PACKAGES} '
            f'/opt/ml/recommender/als_model.py'
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Deploy retrained model ───────────────────────────────────────────
    deploy_retrained = BashOperator(
        task_id='deploy_retrained_model',
        bash_command=(
            'curl -X POST http://bentoml-server:3001/reload_models '
            '|| echo "Deploy triggered"'
        ),
    )

    # ── Log alert ────────────────────────────────────────────────────────
    log_alert = BashOperator(
        task_id='log_drift_alert',
        bash_command='echo "ALERT: CTR drift detected and emergency retrain triggered at $(date)"',
    )

    # DAG Dependencies
    check_drift >> [skip_retrain, emergency_retrain]
    emergency_retrain >> deploy_retrained >> log_alert
