#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests, json, boto3, pytz
from datetime import datetime

#%%

TZ = pytz.timezone('America/Vancouver')

# Filter for specific resort
def keep_whistler(data):
    whis_data = data["resortID"] == 13
    return whis_data

# In[6]:

def get_data():
    """old version"""
    lifts_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/lifts.ashx')
    lifts_res.raise_for_status()
    weather_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/weather.ashx')
    weather_res.raise_for_status()
    terrain_res = requests.get('http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/terrain.ashx')
    terrain_res.raise_for_status()

    # lifts_json = json.dumps({'timestamp': str(datetime.now()), 'lifts': lifts_res.json()})
    
    lifts = json.loads(lifts_res.text)["lifts"]
    whis_lifts = list(filter(keep_whistler, lifts))
    whis_lifts_json = json.dumps({'timestamp': str(datetime.now(TZ)), 'lifts': whis_lifts})

    return whis_lifts_json


#%%

# new version of get data
def get_data():
    """new version"""
    API_URL = 'http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/'
    DATA_LIST = {'lifts': 'lifts', 'weather': 'snowconditions', 'terrain': 'terrains'}  # keys are used in the requests, the values and used in the response
    json_data = dict()

    for d, name in DATA_LIST.items():
        res = requests.get(API_URL + d + '.ashx')
        res.raise_for_status()
        data = json.loads(res.text)[name]
        data = list(filter(keep_whistler, data))
        json_data[d] = json.dumps({'timestamp': str(datetime.now(TZ)), d: data})
    
    return json_data

# In[7]:

def handler(event, context):
    
    session = boto3.Session()
    
    # remove?
    #credentials = session.get_credentials()
    #credentials = credentials.get_frozen_credentials()

    cur_dt = "{:%Y_%m_%d_%H:%M}".format(datetime.now(TZ))

    BUCKET_NAME = 'snowbot-pv'
    

    # data = get_data()

    # S3 Connect
    s3 = session.resource(
        's3'
    )

    # Upload Files
    for k, data in get_data().items():
        file_name = cur_dt + "_wb_" + k + ".json"
        print(file_name)
        s3.Bucket(BUCKET_NAME).put_object(Key=file_name, Body=data)
    
    return "Uploaded files!"


# In[8]:


# handler('', '')


# In[ ]:




