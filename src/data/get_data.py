import json
import os
import warnings
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import List, Union

import boto3
import botocore
import pandas as pd
import pytz
import requests
import s3fs
from fastparquet import ParquetFile, write

session = boto3.Session()
BUCKET_NAME = 'snowbot-pv'

# S3 Connect
s3 = session.resource('s3')
bucket = s3.Bucket(BUCKET_NAME)



# parquet engines don't handle shifted timezones
TZ = pytz.timezone('America/Vancouver')

DATA_DIR = "../data/"
MERGED_JSON_FILENAME = "merged_file.json"
merged_json_file = DATA_DIR + MERGED_JSON_FILENAME

# Used for weather data in jsons_to_df()
weather_meta_fields = [
    'newSnow', 'last24Hours', 'last48Hours', 'last7Days', 'midMountainBase',
    'resortID'
]
weather_record_path = ['weather', 'weatherForecast']
weather_meta = [['weather', i] for i in weather_meta_fields]
weather_meta.append('timestamp')

# Used for lift and terrain status in jsons_to_df()
# Important to set categories because when writing incrementally to parquet, some increments
# may not include all statuses.  Manually setting the categories avoids errors due to
# different catergory indexing between increments.
status_cat_dtype = pd.api.types.CategoricalDtype(categories=['X', 'H', 'O'],
                                                 ordered=True)
groomed_cat_dtype = pd.api.types.CategoricalDtype(categories=['No', 'Yes'],
                                                  ordered=True)

# Column dtypes that are to be set for each dataframe
df_dtypes = {
    "lifts": {
        'liftID': 'int8',
        'resortID': 'int8',
        'liftName': 'object',
        'status': status_cat_dtype,
        'timeToRide': 'int8'
    },
    'terrain': {
        'runID': 'int16',
        'resortID': 'int8',
        'groomed': groomed_cat_dtype,
        'runName': 'object',
        'runType': 'int8',
        'status': status_cat_dtype,
        'terrainName': 'object'
    },
    'weather': {
        'resortID': 'int8',
        'forecast.dayDescription': 'object',
        'forecast.daycode': 'int8',
        'forecast.forecastString': 'object',
        'forecast.iconName': 'object',
        'forecast.summaryDescription': 'object',
        'forecast.temperatureHigh': 'int8',
        'forecast.temperatureLow': 'int8',
        'weather.last24Hours': 'int8',
        'weather.last48Hours': 'int8',
        'weather.last7Days': 'int8',
        'weather.midMountainBase': 'int16',
        'weather.newSnow': 'int8'
    }
}

def flatten(items):
    """Yield items from any nested iterable"""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x


# The columns that serve to identify records for each topic
topic_ID_col_names = {
    'lifts': ['resortID', 'liftName'],
    'terrain': ['resortID', 'runID', 'terrainName'],
    'weather': 'resortID',
    'all_topics': 'timestamp'
}
# All of the column names that serve to identify records in at least one of the topics
all_ID_col_names = set(flatten(topic_ID_col_names.values()))

fs = s3fs.S3FileSystem()
myopen = fs.open
nop = lambda *args, **kwargs: None

HISTORY_SUFFIX = '_history_DEV.parquet'
PRIOR_SUFFIX = '_prior_DEV.json'



# from https://alexwlchan.net/2019/07/listing-s3-keys/
def get_matching_s3_objects(bucket, prefix="", suffix=""):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # We can pass the prefix directly to the S3 API.  If the user has passed
    # a tuple or list of prefixes, we go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.endswith(suffix):
                    yield obj


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj["Key"]


def merge_matching_jsons_on_s3(save_file, prefix="", suffix=""):
    """Merges json files on S3 that match the suffix into a new json and save it
    as the save_file on S3."""

    result = []

    for f in get_matching_s3_keys(BUCKET_NAME, prefix=prefix, suffix=suffix):
        # TBD: more efficient to go straight to df w/o saving json to file?

        # Write the file from S3 into a local temp file
        with open('temp', 'wb') as tfw:
            bucket.download_fileobj(f, tfw)

        # Append the local temp file into the result list
        with open('temp', 'rb') as tfr:
            result.append(json.load(tfr))

    os.remove("temp")

    # Fill the output file with the merged content
    with open(save_file, "w") as outfile:
        json.dump(result, outfile)


def set_df_datatypes(df, topic):
    """Set the datatypes for a df according to the topic that
    it represents."""
    df = df.astype(df_dtypes[topic])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def jsons_to_df(jsons, record_path, meta='timestamp'):
    """Convert a json containing one or more timestamps to a dataframe."""
    if record_path == 'weather':
        # Deal with the nested object that the weather data uses to store the weather forecast
        df = pd.json_normalize(jsons, record_path=weather_record_path,
                               meta=weather_meta, record_prefix='forecast.')
        df.rename(columns={"weather.resortID": "resortID"}, inplace=True)
    else:
        df = pd.json_normalize(jsons, record_path=record_path,
                               meta=meta)

    df = set_df_datatypes(df, record_path)
    return df


def load_json_as_df(merged_json_file, record_path):
    """Load json file containing one or more timestamps as a dataframe."""
    with open(merged_json_file, "r") as f:
        d = json.load(f)
        df = jsons_to_df(d, record_path)
        return df


def get_data_changes(df, topic, keep_oldest=False):
    """
    Filter out rows that do not represent changed data.

    Parameters
    ----------
    df : pandas.DataFrame
        Includes 'timestamp' identifying and data columns.  Lists data for each timestamp.
    keep_oldest : boolean
        Indicates if the returned DataFrame should keep the oldest record for each entity (i.e.
        lift, resort, tor terrain) even if an entity has no data changes.  This is so that the
        earliest data for each entity is not lost, and all entities are listed the returned DataFrame
        even if their data has not changed.  Use `False` when there is just one DataFrame to process.
        Use `True` is cases where the data changes will be appended to an existing dataframe that
        already has at least one row for each entity.

    Returns
    -------
    pandas.DataFrame
        Only includes the rows from the original dataframe where there was a change to new values
        in the data columns.
    """
    ID_columns = topic_ID_col_names[topic]
    data_columns = [c for c in df.columns if c not in all_ID_col_names]

    def filter_for_data_changes(df, keep_oldest=keep_oldest):
        """Filter out rows where data is unchanged for adjacent timestamps.
        Required to handle cases when there are > 2 rows per entity.
        """
        # TBD: optimize via slice_shift(), which doesn't copy data,
        # instead of shift()?
        keep_idx = df[data_columns].ne(df[data_columns].shift()).any(
            axis=1).values[1:]  # True for rows with data changes
        changed_rows = df.reset_index(drop=True).drop(index=0)[keep_idx]

        if keep_oldest:
            firstrow = df.loc[df['timestamp'].idxmin()]
            keep_df = firstrow.to_frame().T.append(changed_rows)
        else:
            keep_df = changed_rows

        return keep_df

    # Drop any rows that are complete duplicates so that conditional evaluation will
    # work.  This is required for Peak 2 Peak Gondola because it is duplicated in the
    # lifts data.  Maybe others as well.
    df.drop_duplicates(inplace=True)

    # 1 means that there were up to 2 rows found per group
    if df.groupby(ID_columns, group_keys=False).cumcount().max() < 2:
        # Most efficient method.  Only works if there are 2 or less rows per entity.
        subset = df.columns.drop('timestamp')
        df = df.sort_values('timestamp')

        if keep_oldest:
            df = df.drop_duplicates(subset=subset, keep='first')
        else:
            df = df.drop_duplicates(subset=subset, keep=False)
            df = df.drop_duplicates(subset=ID_columns, keep='last')

    else:
        # Less efficient method.  Required if there are > 2 rows per entity.
        df = df.sort_values('timestamp').groupby(ID_columns, group_keys=False)\
               .apply(filter_for_data_changes)\
               .reset_index(drop=True)

    records_are_unique(df, include_timestamp_in_colnames(ID_columns))
    
    # TBD: may not be neccessary if this will already have be done on the input df
    # Remove to optimize?
    df = set_df_datatypes(df, topic)

    return df


def records_are_unique(df: pd.DataFrame, record_id_cols: List[str]) -> bool:
    """Check if records in df can be uniquely identified using record_id_cols
    and raise warning if they are not."""
    df_indexed = df.set_index(record_id_cols)
    are_unique = df_indexed.index.is_unique
    if not are_unique:
        warnings.warn(
            f"\nSome records can not be uniquely identified using {record_id_cols}"
            f"\n{df_indexed[df_indexed.index.duplicated(keep = False)]}"
        )
    return are_unique


def include_timestamp_in_colnames(col_names: Union[List[str], str]) -> List[str]:
    """Returns a list of strings which includes 'timestamp' in addition to the list
    or sting given for `col_names`.
    
    >>> include_timestamp_in_colnames(topic_ID_col_names['terrain'])
    ['resortID', 'runID', 'terrainName', 'timestamp']
    """
    col_names = deepcopy(col_names)
    if type(col_names) == str : col_names = [col_names]
    col_names.extend(['timestamp'])
    return col_names


def get_status_durations(lifts_df):
    '''Calculate values and add columns for the time difference between the
    timestamp for the current status and the timestamp for the next status
    for each lift:
    `time_diff` column: Gives the duration that the lift was in the status indicated in the `status` column.
    `time_diff_seconds` column: `time_diff` converted to seconds.
    '''
    # TBD: optimize if needed via # 3 under:
    # https://towardsdatascience.com/pandas-tips-and-tricks-33bcc8a40bb9
    record_id_cols = include_timestamp_in_colnames(topic_ID_col_names['lifts'])
    df = lifts_df.sort_values(by=record_id_cols)
    df['time_diff'] = df.groupby(topic_ID_col_names['lifts'])['timestamp'].diff(1).shift(-1)

    # Fill in the durations which will be missing for the most recent status changes
    missing_time_diffs_idx = df.loc[(df['time_diff'].isnull()) & (
        df['timestamp'] >= df['timestamp'].min()), 'timestamp'].index.values

    df.loc[missing_time_diffs_idx, 'time_diff'] = df['timestamp'].max(
    ) - df.loc[missing_time_diffs_idx, 'timestamp']

    # Convert to seconds
    df['time_diff_seconds'] = df['time_diff'].dt.total_seconds()

    return df



def write_dataframe_to_parquet_on_s3(df, topic, fname):
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

    if not list(bucket.objects.filter(Prefix=fname)):
        print(f"File {fname} not found.  Creating new file.")
        # Keep oldest record for each entity because creating new file
        df = get_data_changes(df, topic=topic, keep_oldest=True)
        write_parquet(df, fname, app=False)

    else:
        print(f"File {fname} found on S3.")
        df = get_data_changes(df, topic=topic, keep_oldest=False)
        write_parquet(df, fname, app=True)


def filter_resort(data, resortID: int = None) -> dict:
    """Filter for a specific resort."""
    if resortID:
        return data["resortID"] == resortID
    else:
        return data


def get_data(filter_topic: Union[str, List] = None, filter_resortID: int = None) -> dict:
    """Get data from EpicMix API. Defaults to all resorts.  Option to filter for a
    specific resort or topic.
    """
    API_URL = 'http://www.epicmix.com/vailresorts/sites/epicmix/api/mobile/'
    # keys are used in the requests, the values and used in the response
    DATA_LIST = {'lifts': 'lifts',
                 'weather': 'snowconditions', 'terrain': 'terrains'}
    json_data = dict()

    # Create lists to filter by topic
    if filter_topic is not None:
        filtered_data_list = {k: v for k,
                              v in DATA_LIST.items() if k in filter_topic}
    else:
        filtered_data_list = DATA_LIST

    for d, name in filtered_data_list.items():
        res = requests.get(API_URL + d + '.ashx')
        res.raise_for_status()
        data = json.loads(res.text)[name]
        data = list(filter(lambda x: filter_resort(x, filter_resortID), data))
        json_data[d] = json.dumps(
            {'timestamp': str(datetime.now(TZ)), d: data})

    return json_data


def s3_object_exists(fname):
    """Check if an s3 object exists.  Returns `True` if the object exists."""
    try:
        bucket.Object(fname)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"{fname} doesn't exist")
        else:
            raise
    return True


def load_dataframe_from_parquet_on_s3(fname):
    """ Load a dataframe from a Parquet file on S3. """
    if s3_object_exists(fname):
        read_file = f"s3://{BUCKET_NAME}/{fname}"
        pf = ParquetFile(read_file, open_with=myopen)
        df = pf.to_pandas()

        # Reshift the timezone because parquet engines don't handle shifted timezones
        df.loc[:, 'timestamp'] = df.loc[:, 'timestamp'].dt.tz_convert(TZ)

        return df


class ApiData():
    def __init__(self, topic: str, current_json: str):
        self.topic = topic
        self.current_json = current_json
        # May not exist yet
        self.prior_fname = topic + PRIOR_SUFFIX
        self.prior_object = bucket.Object(self.prior_fname)
        self.check_prior_object()

    def check_prior_object(self):
        """Get prior data json"""
        try:
            self.prior_object.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(f"Prior json for {self.topic} doesn't exist")
                self.prior_exists = False
            else:
                # Something else has gone wrong.
                raise
        else:
            self.prior_exists = True
        return self.prior_exists

    def get_prior_data_json(self):
        """Get prior data json from S3."""
        if self.prior_exists == True:
            prior = self.prior_object.get()['Body'].read().decode('utf-8')
            self.prior_json = json.loads(prior)
            print(f"Loaded prior {self.topic} json data from S3")
            return self.prior_json
        else:
            print(f"Prior json for {self.topic} doesn't exist")

    def data_changed(self):
        """Compare current data json with prior data json without their timestamps.  The timestamps
        on the current json will always be more recent even when none of the other data has changed.
        """
        if self.prior_json[self.topic] == self.current_json[self.topic]:
            print(
                f"No differences between current and prior {self.topic} data were found.")
            return False
        else:
            print(
                f"Found differences between current and prior {self.topic} data.")
            # Check for changed keys using the first example in each list
            self.compare_dict_keys_recusive(
                self.prior_json[self.topic][0], self.current_json[self.topic][0])
            return True

    def save_prior_data(self):
        """Save the current data as prior data on S3."""
        bucket.put_object(Key=self.prior_fname,
                          Body=bytes(json.dumps(self.current_json).encode('UTF-8')))

    @staticmethod
    def compare_dict_keys_recusive(prior_dict: dict, curr_dict: dict) -> None:
        """Compare keys of two dictionaries and raise warning if they have been changed.
        Recursively checks keys of nested dictionaries.  Nested dictionaries are expected
        to be stored within lists.
        """
        if prior_dict.keys() - curr_dict.keys():
            warnings.warn(
                f'keys were changed:'
                f'\nOriginal keys: {prior_dict.keys()}'
                f'\n\tRemoved keys: {prior_dict.keys() - curr_dict.keys()}'
                f'\n\tAdded keys: {curr_dict.keys() - prior_dict.keys()}\n'
            )
        # Compare keys of nested dicts (using the prior dict)
        for k, v in prior_dict.items():
            if isinstance(v, list):
                ApiData.compare_dict_keys_recusive(v[0], curr_dict[k][0])


class ParquetWriter():
    """Identifies new data and writes it to Parquet file on S3."""

    def __init__(self):
        # Get current data
        self.data_current_all = get_data()  # String.

    def write_new_data_all(self):
        """Writes new data for each type (i.e. 'lift', 'weather', 'terrian')
        of data returned by the API.
        """
        for topic in self.data_current_all:
            current_json = json.loads(self.data_current_all[topic])
            data = ApiData(topic, current_json)
            self.write_new_data(data)

    def write_new_data(self, ApiData):
        """If current data has changed since the last update of Parquet file is, add it
        to the Parquet file.  Save the current data as json to serve as the prior for
        the next comparison.
        """

        if ApiData.prior_exists:
            ApiData.get_prior_data_json()
            if ApiData.data_changed():
                # Get a df with the chages between the prior and current json data
                df = jsons_to_df(
                    [ApiData.prior_json, ApiData.current_json], record_path=ApiData.topic)
                write_dataframe_to_parquet_on_s3(
                    df, ApiData.topic, ApiData.topic + HISTORY_SUFFIX)

                # save current data json as prior
                ApiData.save_prior_data()
                print(
                    f"Replaced data in {ApiData.prior_object.key} with current data.")
        else:
            print(f"Prior json for {ApiData.topic} doesn't exist")
            # Create the prior file
            ApiData.save_prior_data()
            print(f"Created {ApiData.prior_fname}")
        print('\n')
