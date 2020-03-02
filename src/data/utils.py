def save_parquet(df, fname):
    """Save df to (local) parquet file.

    >>> save_parquet(df[0:3].copy(), 'wb_lifts_history')"""
    # parquet engines don't handle shifted timezones
    df.loc[:, 'timestamp'] = df.loc[:, 'timestamp'].dt.tz_convert(pytz.utc)

    # Note: May need snappy-python as a req to run on AWS Lambda
    df.to_parquet(DATA_DIR + fname + '.parquet',
                  engine='fastparquet',
                  partition_on=['timestamp'],
                  file_scheme='mixed')


def load_prior_json_from_s3(topic: str) -> dict:
    """>>> load_prior_json_from_s3('weather')"""
    prior_object = bucket.Object(topic + PRIOR_SUFFIX)
    prior = prior_object.get()['Body'].read().decode('utf-8')
    return json.loads(prior)


def dataframe_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames.
    Based on https://hackersandslackers.com/compare-rows-pandas-dataframes/"""
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    return diff_df