#!/usr/bin/env python3

import queue
import sys
import logging
import glob
import gitlab
import pandas as pd
from typing import Counter
from datetime import datetime, timedelta
from psycopg2 import connect
from sqlalchemy import create_engine

print(sys.version)
print(sys.executable)
now = datetime.now()
url = 'https://git.url.com'
private_token = 'private_token'
treshold_h=360
orion_project_id=3
# initialize list of lists
data = [['gitlab_pipelines_tbl', datetime.now().utcnow()],['gitlab_jobs_tbl', datetime.now().utcnow()],['gitlab_runners_tbl', datetime.now().utcnow()], ['gitlab_job_id_runner_id_tbl', datetime.now().utcnow()]]
#Create the pandas DataFrame
lastUpdate_dataset = pd.DataFrame(data, columns = ['DateTime', 'Table'])


class GitLab_a():
    def __init__(self, loglevel, url, token):
        self.loglevel = loglevel
        self.logger = self.logger(loglevel)
         # private token or personal token authentication (self-hosted GitLab instance)
         # gl = self.gitlab.Gitlab(url='https://git.url.com', private_token='private_token')
        self.gitlab_url = url
        self.gitlab_token = token
        self=gitlab.Gitlab(self.gitlab_url,self.gitlab_token)  
           
    def logger(self, loglevel):
        loglevel = logging.INFO
        if loglevel == "verbose":



            loglevel = logging.DEBUG
        logging.basicConfig(
            level=loglevel,
            format='%(levelname)s: %(asctime)s: %(message)s')
        logging.StreamHandler(sys.stdout)
        return logging

    def get_project(self,proj):
        # private token or personal token authentication (self-hosted GitLab instance)
        gl=gitlab.Gitlab(url='https://git.url.com',private_token='private_token')
        #get the project orion with id == 3
        project=gl.projects.get(proj)
        return project

    #pipeline_attributes
    def get_pipelines_per_project(self,project):
        #get all pipelines in the last 3 hours
        try:
            pipelines_dataset = pd.DataFrame(columns = ['id', 'project_id','ref','status','source','created_at','updated_at','web_url','tag','user','username','started_at','finished_at','finished_at','committed_at','duration','queued_duration','coverage'])
            lastUpdate_dataset = pd.DataFrame()
            gl=gitlab.Gitlab(url='https://git.url.com',private_token='private_token')
            #get the project orion with id == 3
            project=gl.projects.get(3)
            pipelines=project.pipelines.list(updated_after=((now.utcnow() - timedelta(minutes=treshold_h)).isoformat()),as_list=True,all=True,sort='asc')
            for pipeline in pipelines:
                attr=project.pipelines.get(pipeline.id).attributes
                if attr['status'] == 'success':
                    print(attr['status'])
                attr_s = pd.Series({'id':attr['id'],'project_id':attr['project_id'],'ref':attr['ref'],'status':attr['status'],'source':attr['source'],'created_at':attr['created_at'],'updated_at':attr['updated_at'],'web_url':attr['web_url'],'tag': 'False','user':attr['user']['id'],'username':attr['user']['username'],'started_at':attr['started_at'],'finished_at':attr['finished_at'],'committed_at':attr['committed_at'],'duration':attr['duration'],'queued_duration':attr['queued_duration'],'coverage':attr['coverage']},name=0)
                pipelines_dataset = pipelines_dataset.append(attr_s)
                print(pipelines_dataset)
                #attr = pipeline.attributes
                #bridges = pipeline.bridges.list()
                #self.pipelines_dataset = self.pipelines_dataset.append(data_attr,ignore_index=True)
            print("Done")
            print(pipelines_dataset)
            print(datetime.now().utcnow())
            pipelines_dataset=pipelines_dataset.astype({'id':'str','project_id':'str'})
            for i in range(len(pipelines_dataset)):
                try:
                    print(i)
                   # engine = create_engine('postgresql://user:pass@ipaddress:5432/dbname')
                   # pipelines_dataset.iloc[i:i+1].to_sql(name="gitlab_pipelines_info_tbl",if_exists='append',con = engine)
                except Exception as error:
                    pass
           # lastUpdate_dataset = lastUpdate_dataset.append(data,ignore_index=True)
        except Exception as error:
            self.logger.info(f"Moving faulted {project.project_id} to failures project")
            sys.exit(1)
            print(f"pulled all pipelines per {project.project_id} to {project.pipelines}")
        return pipelines_dataset
git = GitLab_a(10,'https://git.url.com','private_token')
project=git.get_project(3)
print(project)
pipelines_dataset=git.get_pipelines_per_project(project)
#print(pipelines_dataset)
engine = create_engine('postgresql://user:pass@ipaddress:5432/dbname')
print(engine)
#pipelines_dataset.to_sql('gitlab_pipelines_info_tbl', engine)
#lastUpdate_dataset.to_sql('gitlab_last_update_tbl',engine)

        


