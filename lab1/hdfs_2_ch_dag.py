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
    print('Последний успешный файл:{}'.format(basename(stdout_stat)))

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
       proc_cat = Popen("hdfs dfs -cat {}".format(f.decode("utf-8")), shell=True, stdout=PIPE, stderr=PIPE)
       proc_cat.wait()
       stdout_cat, stderr_cat = proc_cat.communicate()
       if proc_cat.returncode:
           print(stderr_cat)
           return 1

       try:
           json_obj=json.loads(stdout_cat.decode("utf-8"))
       except:
           print('Это не json')
           continue
       print('На входе:{}'.format(json_obj))

       for field in json_obj:
           if type(json_obj[field]) == type(True):
                json_obj[field] = 1 if json_obj[field]==True else 0
           elif field == "timestamp":
                pass
           elif type(json_obj[field]) is not str:
                json_obj[field] =  str(json_obj[field])
       print('После приведения типов:{}'.format(json_obj))


       cmd = """echo {s} | clickhouse-client --query='INSERT INTO yury_zenin ({fields})
                                         FORMAT JSONEachRow'""".format(s=str(json_obj).replace("'",'\\"'), fields=', '.join(json_obj.keys()))
       print('На CH:{}'.format(cmd))
       proc_ch = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
       proc_ch.wait()
       stdout_ch, stderr_ch = proc_ch.communicate()
       if proc_ch.returncode:
           print(stderr_ch)
           return 1

       print(stdout_ch)

       proc_stat = Popen("hdfs dfs -rm /opt/gobblin/output/part.task_GobblinKafkaQuickStart_*_0_0.txt || hdfs dfs -touchz {}".format(f.decode("utf-8").replace('/yury.zenin','')), shell=True, stdout=PIPE, s$
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


