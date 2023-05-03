from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow import DAG
from airflow.models import Variable
from etl import extract_load, process

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    get_orders = PythonOperator(
        task_id='extract_data',
        python_callable=extract_load,
        op_kwargs={'table': 'Order', 
                   'file_path': 'output_orders.csv'},
    )

    get_orders.doc_md = dedent(
    """\
    #### Task Documentation

    This task extract the data from the sqlite database and and load it into 'output_orders.csv'.
    The function to run the task (extract_load) receive two arguments the table to be extratecd and the path
    to save thequery result.
    """
    )

    calculate = PythonOperator(
        task_id='calculate_vendas_brasil',
        python_callable=process,
        op_kwargs={'join_table': 'OrderDetail', 
                   'left_key': 'Id', 
                   'right_key': 'OrderId',
                   'how': 'inner', 
                   'file_path': 'count.txt'}
    )
    calculate.doc_md = dedent(
    """\
    #### Task Documentation

    This task loads another table and the execute a join with the Order data, get previously at 
    the first task. To do the join, is needed to pass the table to be extract in the sqlite database, the keys
    related to the tables and the path to  store the result. It is also importante to note the this task calls
    the function used in the first task, but just to extract the the data.
    """
    )

    get_orders >> calculate >> export_final_output
