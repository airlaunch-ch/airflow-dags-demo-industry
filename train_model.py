from datetime import datetime, timedelta
from typing import Dict, List
from dateutil.parser import parse
from io import BytesIO
import pandas as pd

from string import Template

from sqlalchemy.types import DateTime, Float, Text
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.automl import AutoMLImportDataOperator, AutoMLCreateDatasetOperator, AutoMLTablesListTableSpecsOperator, AutoMLTablesListColumnSpecsOperator, CloudAutoMLHook
from airflow.providers.google.cloud.operators.vertex_ai.dataset import CreateDatasetOperator, ImportDataOperator, ExportDataOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import CreateAutoMLImageTrainingJobOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from uuid import uuid4
from google.protobuf.struct_pb2 import Value

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

PROJECT_ID = 'fleet-tensor-340919'
GCP_BUCKET = 'demo.airlaunch.ch'
GCP_CONN = 'google_cloud_default'
REGION = 'us-central1'
TARGET = "Deposit"

DISPLAY_NAME = str(uuid4())

TABULAR_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml",
    "metadata": Value(string_value="test-tabular-dataset"),

}

IMAGE_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": Value(string_value="test-image-dataset"),

}

IMAGE_IMPORT_CONFIG = [
    {
        "import_schema_uri": (
            "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_classification_single_label_io_format_1.0.0.yaml"
        ),
        "gcs_source": {
            "uris": ["gs://demo.airlaunch.ch/manufacturing.csv"]

        },
    },
]


IRON_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml",
    "metadata": Value(string_value="iron-ore-quality"),
}

IRON_IMPORT_CONFIG = [
    {
        "import_schema_uri": "gs://demo.airlaunch.ch/import-format_1.0.0.yaml",
        "gcs_source": {
            "uris": ["gs://demo.airlaunch.ch/manufacturing.csv"]
        },
    },
]

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
    'manufacturing_model_training',
    default_args=default_args,
    description='Extract Production Data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 16, 1, 0, 0),
    catchup=True,
) as dag:

    create_image_dataset_job = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=REGION,
        project_id=PROJECT_ID,

    )

    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id=create_image_dataset_job.output['dataset_id'],
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=IMAGE_IMPORT_CONFIG,
    )

    DATASET_ID = "3681501380352147456" 
    create_auto_ml_image_training_job = CreateAutoMLImageTrainingJobOperator(
       task_id="auto_ml_image_task",
       display_name=f"auto-ml-image-{DISPLAY_NAME}",
       dataset_id=DATASET_ID,
       prediction_type="classification",
       multi_label=False,
       model_type="CLOUD",
       training_fraction_split=0.6,
       validation_fraction_split=0.2,
       test_fraction_split=0.2,
       budget_milli_node_hours=8000,
       model_display_name=f"auto-ml-image-model-{DISPLAY_NAME}",
       disable_early_stopping=False,
       region=REGION,
       project_id=PROJECT_ID,
    )

    
    
    #create_image_dataset_job >> import_data_job
    create_image_dataset_job >> import_data_job >> create_auto_ml_image_training_job 

query = Template("""
SELECT col_airflow.timestamp as timestamp, col_7_airflow_value, col_7_level_value, starch_flow, amina_flow, ore_pulp_flow, ore_pulp_ph, ore_pulp_density, percent_iron_feed, percent_silica_feed, percent_iron_concentrate
FROM 
    (
		SELECT timestamp, "value" AS col_7_airflow_value FROM column_airflow 
	 		WHERE "column"=7 AND timestamp BETWEEN '$start_time'::timestamp AND '$end_time'::timestamp
	) col_airflow
INNER JOIN
    (
		SELECT timestamp, "value" AS col_7_level_value FROM column_level
			WHERE "column"=7
	) col_level
ON (col_airflow.timestamp = col_level.timestamp)

INNER JOIN
	(
		SELECT datetime, starch_flow, amina_flow, ore_pulp_flow, ore_pulp_ph, ore_pulp_density FROM input_properties
	) input_properties
ON (col_airflow.timestamp = input_properties.datetime)

INNER JOIN
	(
		SELECT datetime, percent_iron_feed, percent_silica_feed FROM input_quality 
	) input_quality
ON ((col_airflow.timestamp = input_quality.datetime))

INNER JOIN
(
	SELECT datetime, percent_iron_concentrate FROM output_quality 
) output_quality
ON (col_airflow.timestamp = output_quality.datetime)
""")



with DAG(
    'extract_for_model',
    default_args=default_args,
    description='Training a quality predictor',
    schedule_interval=timedelta(days=180),
    start_date=datetime(2021, 11, 1, 0, 0, 0),
    catchup=True,
) as dag:

    start_time = "{{ data_interval_start.format('YYYY-MM-DD HH:mm:ss') }}"
    end_time = "{{ data_interval_end.format('YYYY-MM-DD HH:mm:ss') }}"
    extract_postgres_data = PostgresToGCSOperator(
        task_id='extract_quality_data',
        postgres_conn_id='postgres_warehouse',
        export_format='csv',
        bucket=GCP_BUCKET,
        filename="manufacturing.csv",
        sql=query.substitute(start_time=start_time, end_time=end_time)
    )


    create_image_dataset_job = CreateDatasetOperator(
        task_id="iron_ore_dataset",
        dataset=IRON_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )


    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id=create_image_dataset_job.output['dataset_id'],
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=IRON_IMPORT_CONFIG,
    )

if __name__ == "__main__":
    dag.cli()