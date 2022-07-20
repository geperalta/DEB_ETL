from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.operators.sql import BranchSQLOperator
from airflow.utils.trigger_rule import TriggerRule

def ingest_data():
    s3_hook = S3Hook(aws_conn_id="aws_default",)
    psql_hook = PostgresHook(postgres_conn_id = "ml_conn")
    file = s3_hook.download_file(
        bucket_name="s3-gera-data-bootcamp-198920220629062530475400000001",
        key="CSVs/user_purchase.csv"
    )
    #psql_hook.bulk_load(table="WIZESCHEMA.user_purchase", tmp_file=file) #<-this is the one JM changed for .copyexpert
    ##UPGRADE!!Instead to Download and then ADD the data bulk to table
    #  we can use a LAMBDA to read S3 loop it and add column by column to RDS

    ##Edited to ingest data from the S3 File
    # psql_hook.insert_rows(
    #     table = "WIZESCHEMA.user_purchase",

    #manual entries for test
    #     rows = [
    #         [
    #             "GERAa12345",
    #             "testdellamada",
    #             "Tengo mucho sueño",
    #             12,
    #             "07/18/2022  3:13:00 AM",
    #             2.55,
    #             17850,
    #             "Meshico"
    #         ]
    #     ]
    # )
    psql_hook.copy_expert(sql = """COPY WIZESCHEMA.user_purchase(
                invoice_number,
                stock_code,
                detail,
                quantity,
                invoice_date,
                unit_price,
                customer_id,
                country) 
                FROM STDIN
                DELIMITER ',' CSV HEADER;""", filename = file)

with DAG("testo_dago", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = S3KeySensor(
        task_id="validate",
        aws_conn_id="aws_default",
        bucket_name="s3-gera-data-bootcamp-198920220629062530475400000001",
        bucket_key="CSVs/user_purchase.csv",
    )
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

    #DAGs order to execute. downstream
    start_workflow >> validate >> prepare >> branch
    branch >> [clear, continue_workflow] >> load >> end_workflow