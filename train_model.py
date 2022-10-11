from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.google.cloud.operators.vertex_ai.dataset import CreateDatasetOperator, ImportDataOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import CreateAutoMLImageTrainingJobOperator
from uuid import uuid4
from google.protobuf.struct_pb2 import Value


PROJECT_ID = 'fleet-tensor-340919'
GCP_CONN = 'google_cloud_default'
REGION = 'us-central1'

DISPLAY_NAME = str(uuid4())


#
# Real Dataset
#
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
            "uris": ["gs://cloud-samples-data/ai-platform/flowers/flowers.csv"]
        },
    },
]

#
# Dummy Dataset
#

TEST_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": Value(string_value="test-image-dataset"),
}

TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": (
            "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_bounding_box_io_format_1.0.0.yaml"
        ),
        "gcs_source": {
            "uris": ["gs://ucaip-test-us-central1/dataset/salads_oid_ml_use_public_unassigned.jsonl"]

        },
    },
]

#
# Workflow Definiton
#

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
    'flower_model_training',
    default_args=default_args,
    description='Train a Flower Classifier',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 16, 1, 0, 0),
    catchup=False,
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
        import_configs=TEST_IMPORT_CONFIG,
        timeout=10,
    )

    # Normally, you dynamically get the dataset created in the dag. 
    # For the demo, this would take too long, so we use an existing Dataset to intiate training
    dataset_id = create_image_dataset_job.output['dataset_id']
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
       sync=False,
    )

    #create_image_dataset_job >> import_data_job
    create_image_dataset_job >> import_data_job >> create_auto_ml_image_training_job 
