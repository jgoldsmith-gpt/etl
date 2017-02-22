import pytest
from datetime import datetime
from airflow import DAG, settings
from airflow.models import TaskInstance
from oeem_etl.airflow.sensors import *
from oeem_etl.airflow.hooks import GCSHook
from airflow.exceptions import AirflowSensorTimeout

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
TEST_DAG_ID = 'unit_test_dag'


@pytest.fixture
def test_dag():
    args = {
        'owner': 'airflow',
        'start_date': DEFAULT_DATE,
    }
    dag = DAG(TEST_DAG_ID, default_args=args)
    return dag

def test_gcs_file_sensor(test_dag):
    task = GCSFileSensor(
        task_id='test_gcs_file_sensor',
        bucket='bucket',
        object='object',
        poke_interval=5,
        timeout=5,
        dag=test_dag)

    with pytest.raises(AirflowSensorTimeout):
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

def test_check_task_states():
    session = settings.Session()
    sensor_task = session.query(TaskInstance).filter(TaskInstance.task_id == 'test_gcs_file_sensor')
    assert sensor_task[0].state == 'failed', "Expected failed state for test_gcs_file_sensor"
    session.close()

