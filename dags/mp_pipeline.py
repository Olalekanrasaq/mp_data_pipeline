from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime
from datetime import datetime, timedelta
from scripts.get_reports import get_reports
from scripts.download_files import download_files
from scripts.merge_reports import merge_backfill_reports

year = datetime.now().year
month = datetime.now().strftime("%B")
# day = datetime.now().day - 1

def is_backfill(**context):
    dag_run = context["dag_run"]
    return "merge_backfill_reports" if dag_run.run_type == "backfill" else "skip_merge"

# define default args for the DAG
default_args={
              "owner": "Olalekan Rasaq",
              "email": ["olalekanrasaq1331@gmail.com"],
              "email_on_failure": True,
              "email_on_retry": False,
              "retries": 0, 
              "retry_delay": timedelta(minutes=1)
              }

with DAG(
    dag_id="mp_pipeline",
    default_args=default_args,
    description="A data pipeline to extract and load mp data into a database",
    schedule="@daily",
    start_date=datetime(2026, 1, 12),
    catchup=False,
    tags=["Moniepoint", "data-pipeline"],
):

    download_report = PythonOperator(
        task_id="download_report",
        python_callable=download_files,
        op_kwargs={
            "day": '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%d") }}',
            "report_date": '{{ macros.ds_add(ds, -1) }}'
        }
    )

    extract_report = PythonOperator(
        task_id="extract_report",
        python_callable=get_reports,
        op_kwargs={
            "day": '{{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%d") }}',
            "month": month,
            "year": year,
            "report_date": '{{ macros.ds_add(ds, -1) }}',
            "yesterday_date": '{{ macros.ds_add(ds, -2) }}'
        }
    )

    branch = BranchPythonOperator(
        task_id="check_backfill",
        python_callable=is_backfill
    )

    merge = PythonOperator(
        task_id="merge_backfill_reports",
        python_callable=merge_backfill_reports,
        op_kwargs={
            "report_date": '{{ ds }}'
        }
    )

    skip = EmptyOperator(task_id="skip_merge")


    download_report >> extract_report >> branch >> [merge, skip]