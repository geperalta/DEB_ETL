from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

def ingest_data():
    hook = PostgresHook(postgres_conn_id = "ml_conn")
    hook.insert_rows(
        table = "user_purchase",
        rows = [
            [
                "a123456789",
                "stockcode1",
                "this is a detail text",
                12,
                "12/01/2010  8:26:00 AM",
                2.55,
                17850,
                "Meshico"
            ]
        ]
    )

with DAG("testo_dago", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="ml_conn",
        sql="""
            CREATE SCHEMA POSTUSER;
            CREATE TABLE POSTUSER.user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
                );
        """
    )
    load = PythonOperator(task_id="load", python_callable=ingest_data)
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow