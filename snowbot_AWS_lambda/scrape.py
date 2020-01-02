#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests, json, boto3
from datetime import datetime

#%%
# Filter for specific resort
def keep_whistler(data):
    whis_data = data["resortID"] == 13
    return whis_data

# In[6]:

def get_data():
    lifts_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/lifts.ashx')
    lifts_res.raise_for_status()
    weather_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/weather.ashx')
    weather_res.raise_for_status()
    terrain_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/terrain.ashx')
    terrain_res.raise_for_status()

    # lifts_json = json.dumps({'timestamp': str(datetime.now()), 'lifts': lifts_res.json()})
    
    lifts = json.loads(lifts_res.text)["lifts"]
    whis_lifts = list(filter(keep_whistler, lifts))
    whis_lifts_json = json.dumps({'timestamp': str(datetime.now()), 'lifts': whis_lifts})
    
    return whis_lifts_json


# In[7]:


def handler(event, context):
    
    session = boto3.Session()
    
    # remove?
    #credentials = session.get_credentials()
    #credentials = credentials.get_frozen_credentials()

    cur_dt = "{:%Y_%m_%d_%H:%M}".format(datetime.now())

    BUCKET_NAME = 'snowbot-pv'
    FILE_NAME = cur_dt + "_wb_lifts.json"

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




