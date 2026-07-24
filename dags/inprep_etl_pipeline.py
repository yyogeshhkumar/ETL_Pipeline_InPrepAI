import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/Users/yogi/Documents/DEP"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["inprep-etl"],
)
def inprep_etl_pipeline():

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT_DIR} && python extract.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && python load.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT_DIR} && python transform.py",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"cd {PROJECT_DIR} && python validate.py",
    )

    extract >> load >> transform >> validate

inprep_etl_pipeline()