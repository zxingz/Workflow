# Declaring all the imports.

## Native imports
import os
import sys
from datetime import timedelta
import yaml
import ssl
try:
    from urllib2 import urlopen
except:
    from urllib.request import urlopen
import json

## Airflow imports
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.configuration import conf as AirflowConf
from airflow.models import Connection
from airflow import settings

# Global Variables
# config_url = 'https://gitlab.decisionsciences.bns/api/v4/projects/1080/repository/files/config%2Fproject.yaml/raw?private_token=jxbcskFCfsU2nZxm7jSW&ref=master'


def showWorkerNames():
    # initialize
    workers = Variable.get('Workers', default_var=dict(), deserialize_json=True)
    if 'data' not in workers:
        Variable.set('Workers', workers, serialize_json=True)
    try:
        def convertToFormat(data):
            ret = {'data': []}
            for worker in data['data']:
                ret['data'].append({'status': worker['status'],
                                    'pid': worker['pid'],
                                    'queue': worker['hostname']})
            return ret
        hostname = AirflowConf.get('celery', 'flower_host')
        port = AirflowConf.get('celery', 'flower_port')
        resp = urlopen('http://'+hostname+':'+port+'/dashboard?json=1')
        data = convertToFormat(json.loads(resp.read()))
        if data != workers:
            Variable.set('Workers', data, serialize_json=True)
    except:
        pass


def showQueueName(dag_id, task_id):
    try:
        queue = Variable.get('.'.join(['Queue', dag_id, task_id]), default_var=None)
    except:
        queue = None
    if queue is None:
        queue = AirflowConf.get('celery', 'default_queue')
        Variable.set('.'.join(['Queue', dag_id, task_id]), queue)
    return queue


def sendNotification(**kwargs):
    task_instance = kwargs.get('task_instance')
    dag_id = task_instance.dag_id

    # email title.
    title = dag_id + ' Completed Successfully !'

    # email contents
    body = """
            Hi User(s), <br>
            <br>
            This is to let you know that the """ + dag_id + """ DAG completed successfully. <br>
            <br>
            Sincerely,<br>
            Airflow-BOT <br>
            """
    send_email(user_emails + support_emails, title, body)


def conditionally_trigger(context, dag_run_obj):
    return dag_run_obj


dag_id = __file__.split('/')[-1].split('.')[0]

if dag_id != '__init__':

    # display the name of the workers
    showWorkerNames()

    # Importing all environment variables
    airflow_home = os.environ['AIRFLOW_HOME']
    if 'user' in os.environ:
        user = os.environ['user']
    elif 'USER' in os.environ:
        user = os.environ['USER']
    else:
        user = None

    # Loading DAG config
    project_yaml = os.path.join(os.path.join(airflow_home, 'config'), 'project.yaml')
    with open(project_yaml, 'r') as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    ## if (not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None)):
    ##     ssl._create_default_https_context = ssl._create_unverified_context
    ## project_yaml = urlopen(config_url).read()
    ## config = yaml.load(project_yaml)

    # Loading Environment Variables
    for k, v in config['ENV'].items():
        os.environ[k] = os.path.expandvars(v)

    # Loading Airflow Variables
    for k, v in config['VAR'].items():
        os.environ['AIRFLOW_VAR_' + k] = os.path.expandvars(Variable.get(k, default_var=v))

    # Loading Connection Variables
    session = settings.Session()
    for connection in session.query(Connection).all():
        os.environ['CONN_VAR_' + connection.conn_id+'_LOGIN'] = str(connection.login)
        os.environ['CONN_VAR_' + connection.conn_id + '_PASSWORD'] = str(connection.password)
        os.environ['CONN_VAR_' + connection.conn_id + '_CONN_TYPE'] = str(connection.conn_type)
        os.environ['CONN_VAR_' + connection.conn_id + '_HOST'] = str(connection.host)
        os.environ['CONN_VAR_' + connection.conn_id + '_PORT'] = str(connection.port)
        os.environ['CONN_VAR_' + connection.conn_id + '_EXTRA'] = str(connection.extra)

    # Creating configs
    globals()[dag_id] = DAG(dag_id=dag_id,
                            default_args={'owner': config['DAGS'][dag_id]['owner'],
                                          'start_date': airflow.utils.dates.days_ago(
                                              config['DAGS'][dag_id]['days_ago'])},
                            dagrun_timeout=timedelta(hours=config['DAGS'][dag_id]['dag_timeout']),
                            schedule_interval=config['DAGS'][dag_id]['schedule_interval']
                            if config['DAGS'][dag_id]['schedule_interval'] != 'None' else None)

    # Getting user Emails
    user_emails, support_emails = [], []
    if 'support_emails' in config['DAGS'][dag_id]:
        support_emails = config['DAGS'][dag_id]['support_emails']
    if 'user_emails' in config['DAGS'][dag_id]:
        user_emails = config['DAGS'][dag_id]['user_emails']

    # Creating all the operators
    operators = dict()
    for operator in config['DAGS'][dag_id]['operators']:
        flags = {
            'faliure_email': True,
            'kerberose': True
        }
        for key in flags:
            if key in operator['config']:
                flags[key] = True if operator['config'][key] == 'True' else False

        # check if queue is mentioned in the config
        if 'queue' in operator:
            queue = operator['queue']
        else:
            queue = showQueueName(dag_id, operator['id'])

        if operator['config']['type'] == 'bash':
            cmd = operator['config']['cmd']
            if flags['kerberose']:
                cmd = 'echo $USER && kinit $USER -k -t  $KEYTABS_DIR/$USER.keytab && ' + cmd

            operators[operator['id']] = BashOperator(
                task_id=operator['id'],
                bash_command=os.path.expandvars(cmd),
                dag=globals()[dag_id],
                email_on_failure=flags['faliure_email'],
                email=support_emails,
                queue=queue
            )

        elif operator['config']['type'] == 'trigger':
            operators[operator['id']] = TriggerDagRunOperator(
                task_id=operator['id'],
                trigger_dag_id=operator['config']['target_dag_id'],
                python_callable=conditionally_trigger,
                params={},
                dag=globals()[dag_id],
                queue=queue
            )

    # Link operator edges
    if 'edges' in config['DAGS'][dag_id]:
        for edge in config['DAGS'][dag_id]['edges']:
            parent = list(edge)[-1]
            child = edge[parent]
            operators[parent].set_downstream(operators[child])

    # Adding send notification
    if 'end_notification' in config['DAGS'][dag_id] and config['DAGS'][dag_id]['end_notification'] == 'False':
        pass
    else:
        end_notification = PythonOperator(task_id='Send_Notification',
                                          python_callable=sendNotification,
                                          email_on_failure=True,
                                          email=user_emails,
                                          provide_context=True,
                                          dag=globals()[dag_id])

        for operator in operators:
            if 'edges' in config['DAGS'][dag_id] \
                    and operator in [list(x)[-1] for x in config['DAGS'][dag_id]['edges']]:
                pass
            else:
                operators[operator].set_downstream(end_notification)
