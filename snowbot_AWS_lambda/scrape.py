#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests, json, boto3
from datetime import datetime


# In[6]:


def get_data():
    lifts_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/lifts.ashx')
    lifts_res.raise_for_status()
    weather_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/weather.ashx')
    weather_res.raise_for_status()
    terrain_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/terrain.ashx')
    terrain_res.raise_for_status()

    lifts_json = json.dumps({'timestamp': str(datetime.now()), 'lifts': lifts_res.json()})
    
    return lifts_json



# In[7]:


def handler(event, context):
    
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()

    cur_dt = "{:%B %d, %Y}".format(datetime.now())

    BUCKET_NAME = 'snowbot-pv'
    FILE_NAME = cur_dt + "ADD_LABEL_HERE.json"

    data = get_data()

    # S3 Connect
    s3 = session.resource(
        's3'
    )

    # Uploaded File
    s3.Bucket(BUCKET_NAME).put_object(Key=FILE_NAME, Body=data)
    return "Uploaded file!"


# In[8]:


handler('', '')


# In[ ]:




