from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.sql import BranchSQLOperator
from airflow.utils.trigger_rule import TriggerRule

def ingest_data():
    hook = PostgresHook(postgres_conn_id = "ml_conn")
    hook.insert_rows(
        table = "WIZESCHEMA.user_purchase",
        rows = [
            [
                "GERAa56789",
                "testocode1alv",
                "Tengo mucho sueño",
                12,
                "07/18/2022  3:13:00 AM",
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
            CREATE SCHEMA IF NOT EXISTS WIZESCHEMA;
            CREATE TABLE IF NOT EXISTS WIZESCHEMA.user_purchase (
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
    clear = PostgresOperator( #dag to clear the table if has data, for BRANCH example
        task_id="clear",
        postgres_conn_id="ml_conn",
        sql="""DELETE FROM WIZESCHEMA.user_purchase""",
    )
    continue_workflow = DummyOperator(task_id="continue_workflow")
    branch = BranchSQLOperator (
        task_id='is_empty',
        conn_id='ml_conn',
        sql="SELECT COUNT(*) AS rows FROM WIZESCHEMA.user_purchase", 
        #if the result count is 1 the statement its TRUE and rune if_true branch
        follow_task_ids_if_true=[clear.task_id],
        follow_task_ids_if_false=[continue_workflow.task_id],
    )
    load = PythonOperator(
        task_id="load", 
        python_callable=ingest_data,
        trigger_rule=TriggerRule.ONE_SUCCESS,)
    end_workflow = DummyOperator(task_id="end_workflow")

    #DAGs order to execute. Downstring
    start_workflow >> validate >> prepare >> branch
    branch >> [clear, continue_workflow] >> load >> end_workflow