from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'zmachynspider',
    'start_date': datetime(2017, 3, 1),
    'email': ['dasharya@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    }

dag = DAG('jobs_pipeline', default_args=default_args, schedule_interval=timedelta(1))
t1 = BashOperator(task_id='get_sf',
                  bash_command='python /home/ubuntu/airflow/dags/scripts/get_jobs.py data_science Bay_Area',
                  dag=dag)

t2 = BashOperator(task_id='get_ny',
                  bash_command='python /home/ubuntu/airflow/dags/scripts/get_jobs.py data_science New_York,_NY',
                  dag=dag)

t3 = BashOperator(task_id='get_sl',
                  bash_command='python /home/ubuntu/airflow/dags/scripts/get_jobs.py data_science Seattle,_WA',
                  dag=dag)

t4 = BashOperator(task_id='get_pd',
                  bash_command='python /home/ubuntu/airflow/dags/scripts/get_jobs.py data_science Portland,_OR',
                  dag=dag)

t5 = BashOperator(task_id='update_website',
                  bash_command='''aws emr create-cluster \
--name "reload-dataframes-job" \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.2xlarge InstanceGroupType=CORE,InstanceCount=3,InstanceType=m1.xlarge \
--log-uri s3://deproj/emr/transformation-logs/ \
--release-label emr-5.3.1 \
--use-default-roles \
--applications Name=Spark \
--auto-terminate \
--ec2-attributes KeyName=DATAENGINEERINGACCESS \
--bootstrap-actions file:///home/ubuntu/airflow/dags/scripts/bootstrap.json \
--steps file:///home/ubuntu/airflow/dags/scripts/step.json''',
                  dag=dag)

t5.set_upstream([t1, t2, t3, t4])
