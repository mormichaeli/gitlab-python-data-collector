#!/usr/bin/env python3
import sys
from typing import Counter
import gitlab
from datetime import datetime, timedelta
import pandas as pd
from psycopg2 import connect
from sqlalchemy import create_engine
#----------------------------------------------------------------
# use python version 3.8.9 to support gitlab python module
#----------------------------------------------------------------

treshold_h=10
orion_project_id=3

runners_dataset = pd.DataFrame(columns=['id','description','ip_address','active','is_shared','runner_type','name','online','status'])
runners_jobs_dataset = pd.DataFrame(columns=['job_id','runner_id'])
conn_string = "host='ip' dbname='db' user='user' password='pass'"
conn = connect(conn_string)
#engine = create_engine('postgresql://user:password@ipaddress:5432/dbname')
print(conn)
#runners_jobs_dataset.to_sql('gitlab_job_runner_test', engine)

# private token or personal token authentication (self-hosted GitLab instance)
gl=gitlab.Gitlab(url='https://www.gitlabxxx.com',private_token='token')


#tables:
#1. job_id - Runner_id
#2. job_id - Runner_info
#3. runner_attributes

#1. job_id - Runner_id
now = datetime.now()
print(datetime.now())
gl_runners=gl.runners.all()
print(gl_runners)
for runner in gl_runners:
    jobs = gl.runners.get(runner.id).jobs.list() 
    if (len(jobs) > 0):
        #print(jobs)
        for job in jobs:
            runner_s = pd.Series({'job_id':job.id,'runner_id':runner.id},name=0)
            runners_jobs_dataset = runners_jobs_dataset.append(runner_s)
print(runners_jobs_dataset)
runners_jobs_dataset=runners_jobs_dataset.astype({'job_id':'str','runner_id':'str'})
print(datetime.now())
#3. runner_attributes
runners_dataset = pd.DataFrame(columns=['id','description','ip_address','active','is_shared','runner_type','name','online','status'])

for runner in gl_runners:
    runner_s = pd.Series({'id': runner.id, 'description': runner.description, 'ip_address': runner.ip_address, 'active': runner.active, 'is_shared': runner.is_shared, 'runner_type': runner.runner_type, 'name': runner.name, 'online': runner.online, 'status': runner.status},name=0)
    runners_dataset = runners_dataset.append(runner_s)
print(runners_dataset)

runners_jobs_dataset=runners_jobs_dataset.astype({'job_id':'str','runner_id':'str'})
runners_dataset=runners_dataset.astype({'id':'str','ip_address':'str'})
def ts_last_update(table_name):
    lastUpdate_dataset = pd.DataFrame(columns = ['gitlab_pipelines_tbl','gitlab_jobs_tbl', 'gitlab_runners_tbl','gitlab_job_id_runner_id_tbl'])
    print(table_name)
    #{} = read_last_data(db)
    data = pd.Series({'gitlab_pipelines_tbl':datetime.now(),'gitlab_jobs_tbl':datetime.now(),'gitlab_runners_tbl':datetime.now(),'gitlab_job_id_runner_id_tbl':datetime.now()},name=0)
    lastUpdate_dataset = lastUpdate_dataset.append(data)
    print("*lastUpdate_dataset* Done")
    return lastUpdate_dataset

def read_ts_last_update_from_db(table_name):
    # Create an engine instance
    alchemyEngine = create_engine('postgresql://user:pass@ip:5432/db',pool_recycle=3600)
    dbConnection=alchemyEngine.connect()
    dataFrame = pd.read_sql("select * from \"gitlab_ts_last_update_tbl\"", dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    # Select last row of the dataframe as a series
    last_row = df.iloc[-1]
    print("last row Of Dataframe: ")
    print(last_row)
    # Select last row of the dataframe as a dataframe object
    last_row_df = df.iloc[-1:]
    print("last row Of Dataframe: ")
    print(last_row_df)
    
    print(dataFrame.loc[0,table_name])
    ts = dataFrame.loc[0,table_name]
    # Close the database connection
    dbConnection.close()
    return ts

try:
    engine = create_engine('postgresql://user:pass@ip:5432/db')
    runners_dataset.to_sql(name='gitlab_runners_info_tbl',if_exists='append',con = engine)
    runners_jobs_dataset.to_sql(name='gitlab_job_id_runner_id_tbl',if_exists='append',con = engine)
except Exception as error:
    engine.close()
    engine = create_engine('postgresql://user:pass@ip:5432/db')
    sleep(30)
    runners_dataset.to_sql(name='gitlab_runners_info_tbl',if_exists='append',con = engine)
    runners_jobs_dataset.to_sql(name='gitlab_job_id_runner_id_tbl',if_exists='append',con = engine)
read_ts_last_update_from_db("gitlab_ts_last_update_tbl")
lastUpdate_dataset=ts_last_update("gitlab_ts_last_update_tbl")
lastUpdate_dataset.to_sql(name="gitlab_ts_last_update_tbl",if_exists='append',con = engine)
