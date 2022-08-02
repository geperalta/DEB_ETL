from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator

# Configurations
BUCKET_NAME = "<your-bucket-name>"
local_data = "./dags/data/movie_review.csv"
s3_data = "data/movie_review.csv"
local_script = "./dags/scripts/spark/random_text_classification.py"
s3_script = "scripts/random_text_classification.py"

# helper function
def _local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

data_to_s3 = PythonOperator(
    dag=dag,
    task_id="data_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_data, "key": s3_data,},
)
script_to_s3 = PythonOperator(
    dag=dag,
    task_id="script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": local_script, "key": s3_script,},
)