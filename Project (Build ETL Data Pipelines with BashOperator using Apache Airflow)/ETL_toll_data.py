from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Dag Arguments
default_args = {
    'owner': 'saad',
    'start_date': datetime.now(),
    'email': ['dummy@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    catchup=False
) as dag:

    # Tasks
    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/'
    )

    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command='''
        cut -d"," -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv \
        > /home/project/airflow/dags/finalassignment/csv_data.csv
        '''
    )

    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command='''
        cut -f5,6,7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv \
        > /home/project/airflow/dags/finalassignment/tsv_data.csv
        '''
    )

    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command='''
        cut -c 1-4,5-8 /home/project/airflow/dags/finalassignment/payment-data.txt \
        > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
        '''
    )

    consolidate_data = BashOperator(
        task_id='consolidate_data',
        bash_command='''
        paste /home/project/airflow/dags/finalassignment/csv_data.csv \
              /home/project/airflow/dags/finalassignment/tsv_data.csv \
              /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
        > /home/project/airflow/dags/finalassignment/extracted_data.csv
        '''
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='''
        cut -d"," -f1,2,3,4,5,6,7,8,9 /home/project/airflow/dags/finalassignment/extracted_data.csv \
        | tr '[:lower:]' '[:upper:]' \
        > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
        '''
    )

    # Pipeline
    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
