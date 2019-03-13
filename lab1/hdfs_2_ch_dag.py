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
    from os.path import basename
    import json

    proc_stat = Popen("hdfs dfs -ls -C /opt/gobblin/output/part.task_GobblinKafkaQuickStart_*_0_0.txt", shell=True, stdout=PIPE, stderr=PIPE)
    proc_stat.wait()
    stdout_stat, stderr_stat = proc_stat.communicate()
    if proc_stat.returncode:
       print(stderr_stat)
    print('��������� �������� ����:{}'.format(basename(stdout_stat)))

    proc_ls = Popen("hdfs dfs -ls -C /opt/gobblin/output/yury.zenin", shell=True, stdout=PIPE, stderr=PIPE)
    proc_ls.wait()
    stdout_ls, stderr_ls = proc_ls.communicate()
    if proc_ls.returncode:
       print(stderr_ls)
       return 1

    for f in  stdout_ls.split(b'\n'):
       if f==b'' or basename(stdout_stat)>=basename(f):
           continue

       print(f)

       proc_cat = Popen("hdfs dfs -cat {} >  /tmp/file.json".format(f.decode("utf-8")), shell=True, stdout=PIPE, stderr=PIPE)
       proc_cat.wait()
       stdout_cat, stderr_cat = proc_cat.communicate()
       if proc_cat.returncode:
           print(stderr_cat)
           return 1

       print("hdfs_cat_done")
       local_file=open("/tmp/file.json","r")
       r =  local_file.read().replace("'",'"')
       local_file.close()

       local_file=open("/tmp/file.json","w")
       local_file.truncate(0)
       local_file.write(r)
       local_file.close()


       cmd = """cat  /tmp/file.json | clickhouse-client --query='INSERT INTO yury_zenin (item_url, basket_price, item_price, detectedDuplicate,
                        timestamp, remoteHost, pageViewId, sessionId, detectedCorruption, partyId, location, eventType, item_id, referer,
                        userAgentName, firstInSession) FORMAT JSONEachRow'"""
       print('�� CH:{}'.format(cmd))
       proc_ch = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
       proc_ch.wait()
       stdout_ch, stderr_ch = proc_ch.communicate()
       if proc_ch.returncode:
           print(stderr_ch)
           return 1

       print(stdout_ch)

       proc_stat = Popen("hdfs dfs -rm /opt/gobblin/output/part.task_GobblinKafkaQuickStart_*_0_0.txt || hdfs dfs -touchz {}".format(f.decode("utf-8").replace('/yury.zenin','')), shell=True, stdout=PIPE, stderr=PIPE)
       proc_stat.wait()
       stdout_stat, stderr_stat = proc_stat.communicate()
       if proc_stat.returncode:
           print(stderr_stat)
           return 1

hdfs2ch = PythonOperator(
    task_id='hdfs2ch',
    python_callable=put_data_to_ch,
    dag=dag,
)


