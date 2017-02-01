import pytest
import os
from datetime import datetime
from airflow import DAG
import json
import csv
from oeem_etl.airflow.operators import *

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



    
