#!/usr/bin/env python3
import gitlab
from datetime import datetime, timedelta
import pandas as pd
from psycopg2 import connect
from sqlalchemy import create_engine, null, true
import re


now=datetime.now()
url = 'https://gitlabxxx.com'
private_token = 'token'
treshold_h=24
project_id=3
append = True
jobs_dataset = pd.DataFrame(columns=['job_id','pipeline_id','status','commit-title','branch','queued_duration','tag_list','stage','name','ref','prefix','user','created_at','started_at','min_round_started_at','hour_round_started_at','day_round_started_at','finished_at'])


def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

def rounderM(t):
    if t.minute >= 30:
        if t.hour < 23:
            return t.replace(second=0, microsecond=0, minute=0, hour=t.hour+1)
    else:
        return t.replace(second=0, microsecond=0, minute=30)

def rounderH(t):
    if t.hour >= 23:
        return t.replace(second=0, microsecond=0, minute=0, hour=0)
    else:
        return t.replace(second=0, microsecond=0, minute=0,hour=t.hour+1)
def rounderD(t):
        return t.replace(second=0, microsecond=0, minute=0, hour=0)

# private token or personal token authentication (self-hosted GitLab instance)
gl=gitlab.Gitlab(url='https://gitlabxxx.com',private_token='token')
project=gl.projects.get(project_id)

#get all pipelines in the last 3 hours
pipelines=project.pipelines.list(updated_after=((now.utcnow() - timedelta(hours=treshold_h)).isoformat()),scope=('finished'),as_list=True,all=True,sort='asc')
for pipeline in pipelines:
    #print(pipeline)
    jobs=pipeline.jobs.list(all=True)
    for job in jobs:
        tag_list = ','.join(job.tag_list)
        #print(job)
        #print(job.started_at)
        if job.started_at is not None:
            result = re.split('/', job.ref)
            if result[0] is not null:
                prefix = result[0]
            if len(result) > 1:
                user = result[1]
            if prefix =='TEAM' or prefix =='release':
                prefix = None
                user = None
            #print(prefix,user)
            #2022-01-25 19:01:45.693000
            #"2022-01-24T09:24:53.558Z"
            if job.started_at :
                started_at = datetime.strptime(job.started_at, '%Y-%m-%dT%H:%M:%S.%fZ')
                print(started_at)
            if job.created_at :
                created_at = datetime.strptime(job.created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
                print(created_at)
            if job.finished_at :
                finished_at = datetime.strptime(job.finished_at, '%Y-%m-%dT%H:%M:%S.%fZ')
                print(finished_at)
            min_round = rounderM(started_at)
            print(min_round)
            hour_round = rounderH(started_at)
            print(hour_round)
            day_round = (started_at).strftime('%m-%d-%Y')
            print(day_round)
            branch = 'None'
            if job.commit['title'].startswith("Merge "):
                word_list = job.commit['title'].split("into")
                if len(word_list) > 2:
                    branch = word_list[1]
            s = pd.Series({'job_id':job.id,'pipeline_id':job.pipeline_id,'status':job.status,'commit-title':job.commit['title'],'branch':branch,'queued_duration':job.queued_duration,'tag_list':tag_list,'stage':job.stage,'name':job.name,'ref':job.ref,'prefix':prefix,'user':user,'created_at':created_at,'started_at':started_at,'min_round_started_at': min_round,'hour_round_started_at': hour_round,'day_round_started_at': day_round,'finished_at': finished_at},name='0')
            jobs_dataset = jobs_dataset.append(s)
print(jobs_dataset)
jobs_dataset=jobs_dataset.astype({'job_id':'str','pipeline_id':'str','status':'str','stage':'str','name':'str','ref':'str','created_at':'str','started_at':'str','finished_at':'str'})
print("Done")

engine = create_engine('postgresql://user:pass@ip:5432/db')
print(engine)
jobs_dataset.to_sql('gitlab_jobs_info_tbl', engine, if_exists='append')
