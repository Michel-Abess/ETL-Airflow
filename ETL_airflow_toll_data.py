xx
# import the libraries
from datetime import timedelta
# The DAG object : instantiate a DAG
from airflow import DAG
# Operators: write tasks!
from airflow.operators.bash_operator import BashOperator
# help makes scheduling
from airflow.utils.dates import days_ago
#defining DAG arguments
default_args = {
    'owner': 'Annie Dum Myname',
    'start_date': days_ago(0),
    'email': ['anniesmart@kinky.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks
# define the first task : unziped data
extract = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -xzvf tolldata.tgz',
    dag=dag,
)

# define the second task : extract data from csv file
extract = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f 1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the third task : extract data from tsv file
extract = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f 5-7 tollplaza-data.tsv | tr '\t' ',' > tsv_data.csv ",
    dag=dag,
)



# define the fourth task : extract data from fixed width file
extract = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="awk '{print $(NF-1), $NF}' payment-data.txt | tr ' ' ',' > fixed_width_data.csv",
    dag=dag,
)


# define the fifth task : consolidate extracted data from different files
extract = BashOperator(
    task_id='consolidate_data',
    bash_command= "paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv",
    dag=dag,a
)


# define the sixth task : transform_data
extract = BashOperator(
    task_id='transform_data',
    bash_command="cut -d',' -f4 extracted_data.csv | tr '[:lower:]' '[:upper:]' > /staging/transformed_data.csv",
    dag=dag,
)


# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data




