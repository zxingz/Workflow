import pytest
import yaml
import os

config = None
dag_ids = set()
project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def test_init():
    assert 'building' == 'building'


def test_load_airflow_config():
    global config
    try:
        project_yaml = os.path.join(os.path.join(project_dir,'config'), 'project.yaml')
        with open(project_yaml, 'r') as config_file:
            config = yaml.load(config_file)
    except Exception as e:
        raise pytest.fail("{0}".format(e))


def test_dags_in_config():
    global dag_ids
    try:
        for dag in config['DAGS'].keys():
            dag_ids.add(dag)
    except Exception as e:
        raise pytest.fail("{0}".format(e))


def test_dags_point_to_init_code():
    for dag_id in dag_ids:
        dag_file = os.path.join(os.path.join(project_dir, 'dags'), dag_id+'.py')
        if os.path.isfile(dag_file):
            print(dag_id)
            assert os.readlink(dag_file) in ['DagClass/__init__.py', './DagClass/__init__.py']


def test_dag_has_required_keys():
    req_keys = set(['owner', 'schedule_interval', 'days_ago', 'dag_timeout', 'operators'])
    for dag_id in dag_ids:
        dag_keys = set(config['DAGS'][dag_id].keys())
        print(dag_id)
        for req_key in req_keys:
            assert req_key in dag_keys


def test_no_duplicate_operator_ids():
    for dag_id in dag_ids:
        operators = [x['id'] for x in config['DAGS'][dag_id]['operators']]
        assert len(operators) == len(set(operators))




