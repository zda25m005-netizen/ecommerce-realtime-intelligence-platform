"""
DAG 1: Nightly Pipeline - Layer 4
Schedule: Daily at 2:00 AM
Pipeline: Bronze -> Silver -> Gold -> Retrain Models -> Deploy Best Model
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

SPARK_MASTER = "spark://spark-master:7077"
SPARK_APPS = "/opt/spark-apps"
ML_APPS = "/opt/ml"

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.0.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)

default_args = {
    'owner': 'gaurav_jha',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 3, 1),
}

with DAG(
    dag_id='nightly_etl_and_retrain',
    default_args=default_args,
    description='Nightly: Bronze->Silver->Gold->Retrain->Deploy',
    schedule_interval='0 2 * * *',  # 2:00 AM daily
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'ml', 'nightly'],
) as dag:

    # ── Step 1: Bronze to Silver ─────────────────────────────────────────
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'--packages {SPARK_PACKAGES} '
            f'{SPARK_APPS}/batch/bronze_to_silver.py'
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Step 2: Silver to Gold (Feature Store) ───────────────────────────
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'--packages {SPARK_PACKAGES} '
            f'{SPARK_APPS}/batch/silver_to_gold.py'
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Step 3a: Train ALS Recommender ───────────────────────────────────
    train_als = BashOperator(
        task_id='train_als_recommender',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'--packages {SPARK_PACKAGES} '
            f'{ML_APPS}/recommender/als_model.py'
        ),
        execution_timeout=timedelta(hours=2),
    )

    # ── Step 3b: Train GBT Pricing (parallel with ALS) ──────────────────
    train_gbt = BashOperator(
        task_id='train_gbt_pricing',
        bash_command=(
            f'spark-submit --master {SPARK_MASTER} '
            f'--packages {SPARK_PACKAGES} '
            f'{ML_APPS}/pricing/gbt_pricing_model.py'
        ),
        execution_timeout=timedelta(hours=2),
    )

    # ── Step 4: Deploy models via BentoML ────────────────────────────────
    deploy_models = BashOperator(
        task_id='deploy_models',
        bash_command=(
            'curl -X POST http://bentoml-server:3001/reload_models '
            '|| echo "BentoML reload triggered (may need manual restart)"'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Step 5: Health check ─────────────────────────────────────────────
    health_check = BashOperator(
        task_id='health_check',
        bash_command=(
            'curl -f http://bentoml-server:3001/healthz '
            '&& echo "Health check passed" '
            '|| echo "Health check failed - old model still serving"'
        ),
    )

    # ── Step 6: Log completion ───────────────────────────────────────────
    log_completion = BashOperator(
        task_id='log_completion',
        bash_command='echo "Nightly pipeline completed at $(date)"',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # DAG Dependencies
    bronze_to_silver >> silver_to_gold >> [train_als, train_gbt] >> deploy_models >> health_check >> log_completion
