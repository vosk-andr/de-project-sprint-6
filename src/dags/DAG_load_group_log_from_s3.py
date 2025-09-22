from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum
import vertica_python
from airflow.hooks.base import BaseHook
from airflow.models import Variable

def fetch_s3_file(bucket: str, key: str) -> str:
    """Загружает файл из S3 в локальную файловую систему"""
    AWS_ACCESS_KEY_ID = Variable.get("aws_access_key_id")
    AWS_SECRET_ACCESS_KEY = Variable.get("aws_secret_access_key")
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    local_path = f'/data/{key}'
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=local_path
    )
    return local_path

def load_to_vertica(table_name: str, file_path: str):

    # Получаем подключение из Airflow Connection
    conn = BaseHook.get_connection('vertica_prod')
    vertica_conn = {
        'host': conn.host,
        'port': conn.port,
        'user': conn.login,
        'password': conn.extra_dejson['password'],
        'database': conn.schema,
        'autocommit': conn.extra_dejson.get('autocommit', True),
        'tlsmode': conn.extra_dejson.get('tlsmode', 'disable')
    }
    
    # Подключаемся к Vertica
    v_conn = vertica_python.connect(**vertica_conn)
    cursor = v_conn.cursor()
    
    try:
        copy_command = f"""
        COPY {table_name} FROM LOCAL '{file_path}'
        DELIMITER ','
        ENCLOSED BY '"'
        SKIP 1
        """
        cursor.execute(copy_command)
        print(f"Успешно загружено {file_path} в {table_name}")
    except Exception as e:
        print(f"Ошибка при загрузке в Vertica: {str(e)}")
        raise
    finally:
        cursor.close()
        v_conn.close()

@dag(
    schedule_interval=None,
    start_date=pendulum.parse('2022-07-13'),
    catchup=False,
    tags=['sprint6', 'vertica']
)
def project6():

    files_to_tables = {
        'group_log.csv': 'STV2025041935__STAGING.group_log'
    }
    
    begin = DummyOperator(task_id="begin")
    
    for file_name, table_name in files_to_tables.items():
        fetch_task = PythonOperator(
            task_id=f'fetch_{file_name.replace(".", "_")}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': file_name},
        )
        
        load_task = PythonOperator(
            task_id=f'load_{file_name.replace(".", "_")}_to_vertica',
            python_callable=load_to_vertica,
            op_kwargs={
                'table_name': table_name,
                'file_path': f'/data/{file_name}'
            },
        )
        
        begin >> fetch_task >> load_task


sprint6_dag = project6()
