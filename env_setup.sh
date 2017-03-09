#install
sudo apt-get update
sudo apt-get install python-dev python-pip
pip install boto boto3 beautifulsoup4 requests awscli -U

#set up airflow
sudo apt-get install unzip
sudo apt-get install build-essential
sudo apt-get install python-dev
sudo apt-get install libsasl2-dev
sudo apt-get install python-pandas

#pip install airflow
pip install airflow

mkdir ~/airflow/dags
mkdir ~/airflow/logs
mkdir ~/airflow/plugins
mkdir ~/airflow/dags/scripts

export AIRFLOW_HOME=~/airflow
airflow initdb
