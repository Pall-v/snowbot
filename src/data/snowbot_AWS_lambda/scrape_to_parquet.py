#!/usr/bin/env python
# coding: utf-8

# In[5]:

# For dealing with the library compression that was used to overcome AWS
# Lambda's size limitations
try:
  import unzip_requirements
except ImportError:
  pass

import json
from datetime import datetime

import boto3
import botocore
import pandas as pd
import pytz
import requests
import s3fs
from fastparquet import ParquetFile, write

#%%

TZ = pytz.timezone('America/Vancouver')

fs = s3fs.S3FileSystem()
myopen = fs.open
nop = lambda *args, **kwargs: None

session = boto3.Session()
BUCKET_NAME = 'snowbot-pv'

# S3 Connect
s3 = session.resource('s3')
bucket = s3.Bucket(BUCKET_NAME)

HISTORY_FNAME = 'wb_lifts_history.parquet'
PRIOR_STATUS_FNAME = 'lifts_prior.json'

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


def set_lifts_df_datatypes(df):

    # Important to set categories because when writing incrementally to parquet, some increments
    # may not include all statuses.  Manually setting the categories avoids errors due to
    # different catergory indexing between increments.
    status_cat_dtype = pd.api.types.CategoricalDtype(
        categories=['X', 'H', 'O'], ordered=True)

    # set datatypes for lift table
    df = df.astype({
        "liftID": 'category',
        "resortID": 'category',
        "liftName": 'category',
        "status": status_cat_dtype,
        "timeToRide": "int"
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


def jsons_to_df(jsons):
    df = pd.DataFrame.from_dict(pd.json_normalize(
        jsons, record_path='lifts', meta='timestamp'))
    df = set_lifts_df_datatypes(df)
    return df


def get_status_changes(df, keep_oldest=False):
    """
    Filter out rows that do not represent a status change.

    Parameters
    ----------
    df : pandas.DataFrame
        Includes 'status' and timestamp columns.  Lists status of every lift for each timestamp.
    keep_oldest : boolean
        Indicates if the returned DataFrame should keep the oldest status for each lift even if
        a lift has no status changes.  This is so that the earliest status for each lift is not
        lost, and all lifts are listed the returned DataFrame even if their status has not
        changed.  Use `False` when there is just one DataFrame to process.  Use `True` is cases
        where the status chages will be appended to an existing dataframe that already has at
        least one row for each lift.

    Returns
    -------
    pandas.DataFrame
        Only includes the rows from the original dataframe where there was a change to a new
        status.
    """

    def calc_status_change(df, keep_oldest=keep_oldest):
        change_rows = df[df.status.ne(df.status.shift())]

        if keep_oldest:
            firstrow = df.loc[df['timestamp'].idxmin()]
            keep_df = firstrow.to_frame().T.append(change_rows)
        else:
            keep_df = change_rows

        # Remove so that we don't need to write another column to S3 as we scrape?
        # Just calculate it when plotting and predicting?
        # keep_df['time_diff'] = keep_df['timestamp'].diff(1).shift(-1)

        return keep_df

    df = df.groupby('liftName', group_keys=False)\
           .apply(calc_status_change)\
           .reset_index(drop=True)

    df = set_lifts_df_datatypes(df)

    return df

def write_dataframe_to_parquet_on_s3(df, fname):
    """ Write a dataframe to a Parquet file on S3.  Creates a new parquet file if one
    doesn't already exist.
    """

    def write_parquet(df, fname, app=True):

        output_file = f"s3://{BUCKET_NAME}/{fname}"
        write(output_file,
              df,
              # partition_on=['timestamp'],
              file_scheme='hive',
              append=app,  # need to remove or catch exception to work when file doesn't exist
              open_with=myopen,
              mkdirs=nop)
        print(f"Writing {len(df)} records to {fname}.")

    # Unshift the timezone because parquet engines don't handle shifted timezones
    df.loc[:, 'timestamp'] = df.loc[:, 'timestamp'].dt.tz_convert(pytz.utc)

    s3_object = s3.Object(BUCKET_NAME, fname)

    if not list(bucket.objects.filter(Prefix=fname)):
        print(f"File {fname} not found.  Creating new file.")
        # Keep oldest status for each lift because creating new file
        df = get_status_changes(df, keep_oldest=True)
        write_parquet(df, fname, app=False)

    else:
        print(f"File {fname} found.")
        df = get_status_changes(df, keep_oldest=False)
        write_parquet(df, fname, app=True)


class ParquetWriter():
    """Identifies new data and writes it to Parquet file on S3."""

    def __init__(self):
        # Get current lift status info json
        lifts_current = get_data()['lifts']  # String.
        self.lifts_current_json = json.loads(lifts_current)

        self.lifts_prior_object = s3.Object(BUCKET_NAME, PRIOR_STATUS_FNAME)

    def write_new_data(self):
        """If new data since the last update of Parquet file is found, add it to the Parquet
        file.  Save the current data as json to serve as the prior in the next comparison.
        """

        # Get prior lift status info json
        try:
            self.lifts_prior_object.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("Prior doesn't exist")
                # Create the prior file
                self.save_prior_data()
                print(f"Created {PRIOR_STATUS_FNAME}")
            else:
                # Something else has gone wrong.
                raise
        else:
            # The prior exists
            self.get_prior_data()
            if self.data_changed():

                # Get a df with the status chages between the prior and current json data
                df = jsons_to_df([self.lifts_prior_json, self.lifts_current_json])
                write_dataframe_to_parquet_on_s3(df, HISTORY_FNAME)

                # save current lift status info json as prior
                self.save_prior_data()
                print(
                    f"Replaced data in {self.lifts_prior_object.key} with current data.")

    def get_prior_data(self):
        lifts_prior = self.lifts_prior_object.get()[
            'Body'].read().decode('utf-8')
        self.lifts_prior_json = json.loads(lifts_prior)
        print("Loaded prior json data from S3")

    def data_changed(self):
        """Compare current data json with prior data json without their timestamps.  The timestamps
        on the current json will always be more recent even when none of the lift statuses have changed.
        """
        if self.lifts_prior_json['lifts'] == self.lifts_current_json['lifts']:
            print("No differences between current and prior data were found.")
            return False
        else:
            print("Found differences between current and prior data.")
            return True

    def save_prior_data(self):
        """Save the current data as prior data on S3."""
        bucket.put_object(Key=PRIOR_STATUS_FNAME,
                          Body=bytes(json.dumps(self.lifts_current_json).encode('UTF-8')))
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
    # remove?
    # cur_dt = "{:%Y_%m_%d_%H:%M}".format(datetime.now(TZ))



    # Write data Parquet on S3
    ParquetWriter().write_new_data()


# In[8]:


# handler('', '')


# In[ ]:
