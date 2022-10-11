from datetime import datetime, timedelta
from dateutil.parser import parse
from io import BytesIO
import pandas as pd

from sqlalchemy.types import DateTime, Float, Text
from airflow import DAG
from providers.airlaunch.opcua.transfers.opcua_to_postgres import OPCUAToPostgresOperator

from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

floationColumns = [
    {"airflowNodeId": "1008", "levelNodeId": "1015", "colNo": 1},
    {"airflowNodeId": "1009", "levelNodeId": "1016", "colNo": 2},
    {"airflowNodeId": "1010", "levelNodeId": "1017", "colNo": 3},
    {"airflowNodeId": "1011", "levelNodeId": "1018", "colNo": 4},
    {"airflowNodeId": "1012", "levelNodeId": "1019", "colNo": 5},
    {"airflowNodeId": "1013", "levelNodeId": "1020", "colNo": 6},
    {"airflowNodeId": "1014", "levelNodeId": "1021", "colNo": 7},
]

def extract_xlsx_from_ftp(postgres_conn_id: str, postgres_table: str, ftp_conn_id: str, remote_full_path: str, sheet_name: str, column_names: list, dtype: dict, start_time: str, end_time: str):
    ftp_hook = FTPHook(
        ftp_conn_id
    )

    buff = BytesIO()
    ftp_hook.retrieve_file(
            remote_full_path=remote_full_path, 
            local_full_path_or_buffer=buff
        )
    
    excel_object = pd.ExcelFile(buff, engine='openpyxl')
    df = excel_object.parse(sheet_name = sheet_name, index_col = 0)

    df = df[(df.index >= start_time) & (df.index < end_time)]
    df.reset_index(drop=False, inplace=True)
    df.columns = column_names

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    df.to_sql(postgres_table, pg_hook.get_sqlalchemy_engine(), if_exists='append', index=False, dtype=dtype)


def extract_postgres_table(source_postgres_conn_id: str, source_table: str, dest_postgres_conn_id: str, dest_table: str, start_time: str, end_time: str):
    source_hook = PostgresHook(source_postgres_conn_id)
    destination_hook = PostgresHook(dest_postgres_conn_id)

    records = source_hook.get_pandas_df("""SELECT * FROM "{}" WHERE datetime >= '{}' AND datetime < '{}'""".format(source_table, start_time, end_time))
    records.to_sql(dest_table, destination_hook.get_sqlalchemy_engine(), if_exists='append', index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hello@airlaunch.ch'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
}
with DAG(
    'manufacturing_data_integration',
    default_args=default_args,
    description='Extract Production Data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2021, 11, 21, 1, 0, 0),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=10,
) as dag:

    fetchInputQuality = PythonOperator(
       task_id='extract_input_quality',
       python_callable=extract_xlsx_from_ftp,
       queue='local',
       op_kwargs={
           "postgres_table": "input_quality",
           "column_names": ['datetime', 'percent_iron_feed', 'percent_silica_feed'],
           "dtype": {"datetime": DateTime, "percent_iron_feed": Float, "percent_silica_feed": Float},
           "ftp_conn_id": "ftp_lab",
           "remote_full_path": "/data/input_quality_shifted.xlsx",
           "sheet_name": 'Sheet1',
           "start_time": "{{ data_interval_start.to_datetime_string() }}",
           "end_time": "{{ data_interval_end.to_datetime_string() }}",
           "postgres_conn_id": "postgres_warehouse"
       }
    )

    fetchIncidentReports = PythonOperator(
       task_id='fetch_incident_reports',
       python_callable=extract_xlsx_from_ftp,
       queue='local',
       op_kwargs={
           "postgres_table": "incident_reports",
           "column_names": ['datetime', 'device', 'reason', 'category', 'severity'],
           "dtype": {"datetime": DateTime, "reason": Text, "category": Text, "device": Text, 'severity': Text},
           "ftp_conn_id": "ftp_lab",
           "remote_full_path": "/data/incident_reports_shifted.xlsx",
           "sheet_name": 'Sheet1',
           "start_time": "{{ data_interval_start.to_datetime_string() }}",
           "end_time": "{{ data_interval_end.to_datetime_string() }}",
           "postgres_conn_id": "postgres_warehouse"
       }
    )

    for column in floationColumns:
        extractColumnAirflow = OPCUAToPostgresOperator(
            task_id='extract_opc_column_airflow_{}'.format(column["colNo"]),
            queue='local',
            opcua_node="ns=0;i={}".format(column["airflowNodeId"]),
            opcua_conn_id="opc",
            if_exists='append', 
            opcua_startdate="{{ data_interval_start.to_datetime_string() }}",
            opcua_enddate="{{ data_interval_end.to_datetime_string() }}",
            postgres_table='column_airflow',
            static_columns={
                'column': column["colNo"]
            },
            postgres_conn_id="postgres_warehouse",
        )

        extractColumnLevel = OPCUAToPostgresOperator(
            task_id='extract_opc_column_level_{}'.format(column["colNo"]),
            queue='local',
            opcua_node="ns=0;i={}".format(column["levelNodeId"]),
            opcua_conn_id="opc",
            if_exists='append', 
            opcua_startdate="{{ data_interval_start.to_datetime_string() }}",
            opcua_enddate="{{ data_interval_end.to_datetime_string() }}",
            postgres_table='column_level',
            static_columns={
                'column': column["colNo"]
            },
            postgres_conn_id="postgres_warehouse",
        )

    fetchOutputQuality = PythonOperator(
        task_id='extract_output_quality',
        python_callable=extract_xlsx_from_ftp,
        queue='local',
        op_kwargs={
            "postgres_table": "output_quality",
            "column_names": ['datetime', 'percent_iron_concentrate', 'percent_silica_concentrate'],
            "dtype": {"datetime": DateTime, "percent_iron_concentrate": Float, "percent_silica_concentrate": Float},
            "ftp_conn_id": "ftp_lab",
            "remote_full_path": "/data/output_quality_shifted.xlsx",
            "sheet_name": 'Sheet1',
            "start_time": "{{ data_interval_start.to_datetime_string() }}",
            "end_time": "{{ data_interval_end.to_datetime_string() }}",
            "postgres_conn_id": "postgres_warehouse"
        }
    )

    fetchInputProperties = PythonOperator(
        task_id='load_input_properties',
        python_callable=extract_postgres_table,
        queue='local',
        op_kwargs={
            "source_postgres_conn_id": 'postgres_quality',
            "source_table": "input_quality_shifted",
            "dest_postgres_conn_id": 'postgres_warehouse',
            "dest_table": "input_properties",
            "start_time": "{{ data_interval_start.to_datetime_string() }}",
            "end_time": "{{ data_interval_end.to_datetime_string() }}",
        }
    )

if __name__ == "__main__":
    dag.cli()