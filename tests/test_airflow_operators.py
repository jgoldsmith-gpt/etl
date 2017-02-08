import pytest
import os
from datetime import datetime
from airflow import DAG
import json
import csv
import mock
from oeem_etl.airflow.operators import *
from oeem_etl.requester import Requester

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
TEST_DAG_ID = 'unit_test_dag'


class MockResponse(object):
    pass


@pytest.fixture(autouse=True)
def pass_datastore_requests(monkeypatch):
    response = MockResponse()
    response.status_code = 200
    monkeypatch.setattr(Requester, 'upload_chunk', lambda x, y, z: response)

@pytest.fixture
def test_dag():
    args = {
        'owner': 'airflow',
        'start_date': DEFAULT_DATE,
    }
    dag = DAG(TEST_DAG_ID, default_args=args)
    return dag

@pytest.fixture(scope='session')
def proj_trace_data(tmpdir_factory):
    j = {
        'unit_test': {
            'P1': ['T1','T2'],
            'P2': ['T3'],
        }
    }

    file = tmpdir_factory.mktemp('oeem_etl_operator_test').join('create_proj_trace_map_from_json_in.json')
    file.write(json.dumps(j))
    return str(file)

@pytest.fixture(scope='session')
def temp_out_file(tmpdir_factory):
    file = tmpdir_factory.mktemp('oeem_etl_operator_test').join('temp_out.txt')
    return str(file)

@pytest.fixture(scope='session')
def sample_csv_data(tmpdir_factory):
    data = (
        "fargle,blargle,gargle\n"
        "Bonds,Barry,762\n"
        "Aaron,Hank,755\n"
        "Ruth,Babe,714\n"
        )

    file = tmpdir_factory.mktemp('oeem_etl_operator_test').join('tranlsate_csv.csv')
    file.write(data)
    return str(file)

@pytest.fixture(scope='session')
def sample_project_csv_data(tmpdir_factory):
    data = (
        "project_id,zipcode,baseline_period_end,reporting_period_start\n"
        "1,19806,2015-01-01,2015-01-02\n"
        "2,19801,1978-08-06,1978-08-07\n"
    )

    file = tmpdir_factory.mktemp('oeem_etl_operator_test').join('project_csv.csv')
    file.write(data)
    return str(file)

@pytest.fixture(scope='session')
def sample_metadata_csv_data(tmpdir_factory):
    data = (
        "project_id,some_field,some_other_field\n"
        "1,1,one\n"
        "2,2,two\n"
        )

    file = tmpdir_factory.mktemp('oeem_etl_operator_test').join('metadata_csv.csv')
    file.write(data)
    return str(file)

def test_create_proj_trace_map_from_json_operator(test_dag, proj_trace_data, temp_out_file):
    task = CreateProjTraceMapFromJsonOperator(
        task_id='test_create_proj_trace_map_from_json',
        in_file=proj_trace_data,
        out_file=temp_out_file,
        map_name='unit_test',
        dag=test_dag)

    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    p1_has_t1 = False
    p1_has_t2 = False
    p2_has_t3 = False
    unexpected = False

    with open(temp_out_file, 'r') as f_out:
        reader = csv.DictReader(f_out)
        for row in reader:
            proj_id = row['project_id']
            trace_id = row['trace_id']
            if proj_id == 'P1' and trace_id == 'T1':
                p1_has_t1 = True
            elif proj_id == 'P1' and trace_id == 'T2':
                p1_has_t2 = True
            elif proj_id == 'P2' and trace_id == 'T3':
                p2_has_t3 = True
            else:
                unexpected = True

    assert p1_has_t1, "P1 did not have T1"
    assert p1_has_t2, "P1 did not have T2"
    assert p2_has_t3, "P2 did not have T3"
    assert not unexpected, "Unexpected map found"

def test_translate_csv(test_dag, sample_csv_data, temp_out_file):
    task = TranslateCSVOperator(
        task_id='test_translate_csv',
        in_file=sample_csv_data,
        out_file=temp_out_file,
        field_map={
            'fargle': ['last_name'],
            'bargle': ['first_name'],
            'gargle': ['career_hrs'],
        },
        dag=test_dag)

    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    bonds = False
    aaron = False
    ruth = False
    unexpected = False
    with open(temp_out_file, 'r') as f_out:
        reader = csv.DictReader(f_out)
        for row in reader:
            name = row['last_name']
            hrs = int(row['career_hrs'])
            if name == 'Bonds' and hrs == 762:
                bonds = True
            elif name == 'Aaron' and hrs == 755:
                aaron = True
            elif name == 'Ruth' and hrs == 714:
                ruth = True
            else:
                unexpected = True

    assert bonds, "Bonds should have 762 HRs"
    assert aaron, "Aaron should have 755 HRs"
    assert ruth, "Ruth should have 714 HRs"
    assert not unexpected, "Unexpected values found"

def test_load_project_csv_operator(test_dag, sample_project_csv_data):
    task = LoadProjectCSVOperator(
        task_id='test_load_project_csv',
        filename=sample_project_csv_data,
        datastore_url='http://datastore',
        access_token='token',
        dag=test_dag)

    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

def test_load_project_metadata_csv_operator(test_dag, sample_metadata_csv_data):
    task = LoadProjectMetadataCSVOperator(
        task_id='test_load_metadata_csv',
        filename=sample_metadata_csv_data,
        datastore_url='http://datastore',
        access_token='token',
        dag=test_dag)

    task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


