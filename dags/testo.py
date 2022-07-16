from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.postgres.hook.postgres import PostgresHook
from airflow.utils.dates import days_ago

with DAG("testo_dago", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="ml_conn",
        sql="""
            CREATE SCHEMA IF NOT EXIST POSTUSER;
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
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_workflow")

    start_workflow >> validate >> prepare >> load >> end_workflow