import airflow
from airflow import DAG
import pandas as pd
import yfinance as yf
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, date, datetime
import glob 


default_args = {
    "start_date" : datetime(2021,10,15),
    "retries" : 2,
    "retry_delay" : timedelta(seconds=5)
}

stock_dag = DAG(
    dag_id = "marketvol",
    default_args = default_args,
    description = "A simple dag",
    schedule_interval = "0 18 * * 1-5"
)

templated_command = """
    mkdir -p ~/airflow/dags/data/{{ds}}
"""

t0 = BashOperator(
    task_id = "create_folder",
    bash_command = templated_command,
    dag = stock_dag
)


def download_stock(symbol):
    try: 
        path = "~/airflow/dags/data/" + str(date.today()) + "/"
        start_date = date.today()
        end_date = start_date + timedelta(days=1)
        if symbol == "TSLA":
            tsla_df = yf.download("TSLA", start=start_date, end = end_date, interval='1m')
            tsla_df.to_csv(path + "TSLA.csv", header=True)

        elif symbol == "AAPL":
            aapl_df = yf.download("AAPL", start=start_date, end = end_date, interval='1m')
            aapl_df.to_csv(path + "AAPL.csv", header=True)
            
    except Exception as e:
        error = print(str(e))
        return error 

def query_files():
    tesla_data = pd.read_csv("/opt/airflow/dags/full_stock_data/TSLA.csv")
    apple_data = pd.read_csv("/opt/airflow/dags/full_stock_data/AAPL.csv")
    tesla_data["spread"] = tesla_data['High'] - tesla_data['Low']
    apple_data["spread"] = apple_data['High'] - apple_data['Low']
    tesla_data.to_csv("/opt/airflow/dags/full_stock_data/Final_data_tesla.csv")
    apple_data.to_csv("/opt/airflow/dags/full_stock_data/Final_data_apple.csv")


t1 = PythonOperator(
    task_id = "TSLA_download",
    python_callable = download_stock, 
    provide_context = True,
    op_kwargs = {"symbol": "TSLA"},
    dag = stock_dag
)

t2 = PythonOperator(
    task_id = "AAPL_download",
    python_callable = download_stock,
    provide_context =True,
    op_kwargs = {"symbol": "AAPL"},
    dag = stock_dag
)

templated_command2 = """
    cp -a ~/airflow/dags/data/{{ds}}/AAPL.csv /opt/airflow/dags/full_stock_data/
"""

templated_command3 = """
    cp -a ~/airflow/dags/data/{{ds}}/TSLA.csv /opt/airflow/dags/full_stock_data/
"""


t3 = BashOperator(
    task_id = "move_TSLA",
    bash_command = templated_command2,
    dag = stock_dag
)

t4 = BashOperator(
    task_id = "move_AAPL",
    bash_command = templated_command3,
    dag = stock_dag
)

t5 = PythonOperator(
    task_id = "query_files",
    python_callable = query_files,
    provide_context = True,
    dag = stock_dag
)

t0 >> t1 >> t3
t0 >> t2 >> t4 
t3 >> t5
t4 >> t5

