from datetime import timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

hdfs_path = '/opt/gobblin/output'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='dag',
    default_args=args,
    catchup = False,
    schedule_interval=timedelta(minutes=15),
    dagrun_timeout=timedelta(minutes=15),
)

def put_data_to_ch():
    from subprocess import Popen, PIPE
    import json
    proc_ls = Popen("hdfs dfs -ls -C /opt/gobblin/output/yury.zenin", shell=True, stdout=PIPE, stderr=PIPE)
    proc_ls.wait()
    stdout_ls, stderr_ls = proc_ls.communicate()
    if proc_ls.returncode:
       print(stderr_ls)
    for f in  stdout_ls.split(b'\n'):
       if f=b'':
           continue

       print(f)
       proc_cat = Popen("hdfs dfs -cat {}".format(f.decode("utf-8")), shell=True, stdout=PIPE, stderr=PIPE)
       proc_cat.wait()
       stdout_cat, stderr_cat = proc_cat.communicate()
       if proc_cat.returncode:
           print(stderr_cat)

       try:
           json_obj=json.loads(stdout_cat.decode("utf-8")))
           for field in json_obj:
                if !type(json_obj[field]) is str: 
                      json_obj[field] =  json_obj[field].str
           print(json_obj)
       except:
           print('Это не json')
           continue

       proc_ch = Popen("hdfs dfs -cat {} | clickhouse-client --query='INSERT INTO yury_zenin (timestamp ,referer ,location ,remoteHost ,partyId ,sessionId ,pageViewId ,eventType ,item_id ,item_price ,item_url ,basket_price ,detectedDuplicate ,detectedCorruption ,firstInSession ,userAgentName) FORMAT JSONEachRow' && hdfs dfs -rm {}".format(f.decode("utf-8")), shell=True, stdout=PIPE, stderr=PIPE)
       proc_ch.wait()
       stdout_ch, stderr_ch = proc_ch.communicate()
       if proc_ch.returncode:
           print(stderr_ch)
       print(stdout_ch)

hdfs2ch = PythonOperator(
    task_id='hdfs2ch',
    python_callable=put_data_to_ch,
    dag=dag,
)
