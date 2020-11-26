from typing import List
import json
import requests
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO
import os

CH_PASSWORD = os.environ['CH_PASSWORD']
CH_HOST = 'https://{host}:8443/?database={db}'.format(
    host=os.environ['CH_HOST'],
    db=os.environ['CH_DB'])
AUTH = {
    'X-ClickHouse-User': os.environ['CH_USER'],
    'X-ClickHouse-Key': CH_PASSWORD,
}
CERT = './CA.pem'
BUCKET = os.environ['STORAGE_BUCKET']
FOLDER = os.environ['STORAGE_FOLDER']
TABLE = os.environ['CH_TABLE']
columns = [
    'billing_account_id',
    'billing_account_name',
    'cloud_id',
    'cloud_name',
    'folder_id',
    'folder_name',
    'resource_id',
    'service_id',
    'service_name',
    'sku_id',
    'sku_name',
    'date',
    'currency',
    'pricing_quantity',
    'pricing_unit',
    'cost',
    'credit',
    'monetary_grant_credit',
    'volume_incentive_credit',
    'cud_credit',
    'misc_credit',
    'locale',
    'updated_at',
    'exported_at'
]


def request():
    url = (CH_HOST + '&query={query}').format(
        query='SELECT version()')

    res = requests.get(
        url,
        headers=AUTH,
        verify=CERT)
    res.raise_for_status()
    return res.text


def get_clickhouse_data(query, host=CH_HOST, connection_timeout=1500):
    r = requests.post(host, params={'query': query}, headers=AUTH, verify=CERT, timeout=connection_timeout)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.text)


def get_clickhouse_df(query, host=CH_HOST, connection_timeout=1500):
    data = get_clickhouse_data(query, host, connection_timeout)
    df = pd.read_csv(StringIO(data), sep='\t')
    return df


def upload(table, content, host=CH_HOST):
    content = content.encode('utf-8')
    query_dict = {
        'query': 'INSERT INTO ' + table + ' FORMAT TabSeparatedWithNames '
    }
    r = requests.post(host, data=content, params=query_dict, headers=AUTH, verify=CERT)
    result = r.text
    if r.status_code == 200:
        return result
    else:
        raise ValueError(r.text)


# Shape Data
def shape_df(tmp_df):
    shaped_df = pd.DataFrame(columns=columns)
    for col in columns:
        try:
            shaped_df[col] = tmp_df[col]
        except KeyError as cerr:
            shaped_df[col] = ""

    shaped_df["date"] = pd.to_datetime(shaped_df["date"]).dt.round('D')
    # tmp_df["exported_at"] = tmp_df["exported_at"].dt.round('s')

    decimal_columns: List[str] = [
        'pricing_quantity',
        'cost',
        'credit',
        'monetary_grant_credit',
        'volume_incentive_credit',
        'cud_credit',
        'misc_credit'
    ]

    for col in decimal_columns:
        shaped_df[col] = pd.to_numeric(shaped_df[col])
        shaped_df[col] = shaped_df[col].round(10)

    return shaped_df


def init(drop=False):
    if (drop):
        q = '''drop table if exists ''' + TABLE + ''' on cluster '{cluster}';'''
        get_clickhouse_data(q)

    q = '''
        CREATE TABLE IF NOT EXISTS ''' + TABLE + ''' on cluster '{cluster}'
        (
            billing_account_id  String,
            billing_account_name   String,
            cloud_id String,
            cloud_name String, 
            folder_id String,
            folder_name    String,
            resource_id String,
            service_id String,
            service_name String,   
            sku_id String,
            sku_name String,   
            date date, 
            currency String,   
            pricing_quantity decimal(25,10),   
            pricing_unit String,
            cost   decimal(25,10),
            credit decimal(25,10),
            monetary_grant_credit  decimal(25,10),
            volume_incentive_credit  decimal(25,10),
            cud_credit  decimal(25,10),
            misc_credit  decimal(25,10),
            locale  String,
            updated_at  String,
            exported_at String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/''' + TABLE + '''', '{replica}') 
        PARTITION BY date 
        ORDER BY (date, sku_id) 
        '''
    get_clickhouse_data(q)


def clear_part(part_dt):
    q = '''ALTER TABLE ''' + TABLE + ''' DROP PARTITION ''' + pd.to_datetime(part_dt).strftime("'%Y-%m-%d'")
    get_clickhouse_data(q)


def reload(event, context):
    init(True)

    # q = '''select concat(replace(toString(
    #                     subtractDays(COALESCE(maxOrNull(date), toDate('2018-01-03')),2)
    #                                      ),'-',''),'.csv') from ''' + TABLE
    # try:
    #     start_key = get_clickhouse_data(q).rstrip()
    # except ValueError:
    #     start_key = '20180102.csv'
    start_key = '20190101.csv'
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

    kwargs = {"Bucket": BUCKET, "Prefix": FOLDER, "MaxKeys": 100, "StartAfter": FOLDER + '/' + start_key}
    continuation_token = None
    bck_cnt = 0
    while True:
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token
        obj_list = s3.list_objects_v2(**kwargs)
        for key in obj_list['Contents']:
            try:
                get_object_response = s3.get_object(Bucket=BUCKET, Key=key['Key'])
                df = pd.read_csv(StringIO(get_object_response['Body'].read().decode('utf-8')),
                                 usecols=lambda x: x in columns)
                df = shape_df(df)
                for part_dt in df["date"].unique():
                    clear_part(part_dt)
                upload(
                    TABLE,
                    df.to_csv(index=False, sep='\t'))
                print('object ' + key['Key'] + ' uploaded')
                bck_cnt = bck_cnt + 1
            except KeyError as err:
                print('object ' + key['Key'] + ' error: ' + str(err))
                # print ('No objects found in Bucket ' + BUCKET + ' with prefix ' + FOLDER)
            except ValueError as err:
                print('object ' + key['Key'] + ' error: ' + str(err))
        if not obj_list.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = obj_list.get('NextContinuationToken')
    return {
        'statusCode': 200,
        'body': str(bck_cnt) + ' objects loaded',
        'isBase64Encoded': False,
    }


def increment(event, context):
    init(False)
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )

    for record in event['messages']:
        cur_bucket = record['details']['bucket_id']
        cur_object = record['details']['object_id']

        get_object_response = s3.get_object(Bucket=cur_bucket, Key=cur_object)
        df = pd.read_csv(StringIO(get_object_response['Body'].read().decode('utf-8')), usecols=lambda x: x in columns)
        df = shape_df(df)
        for part_dt in df["date"].unique():
            clear_part(part_dt)
        upload(
            TABLE,
            df.to_csv(index=False, sep='\t'))
        print('object ' + cur_object + ' uploaded')

    return {
        'statusCode': 200,
        #  'body': str(bck_cnt) + ' objects loaded',
        'isBase64Encoded': False,
    }


def handler(event, context):
    try:
        method = event['queryStringParameters']['method']
    except KeyError as err:
        method = ''

    if method == 'reload':
        return reload(event, context)

    try:
        cur_bucket = event['messages'][0]['details']['bucket_id']
        cur_object = event['messages'][0]['details']['object_id']
    except KeyError as err:
        print("Wrong function call")
        return {
            'statusCode': 400,
            'body': 'Wrong function call - either pass method=reload as a parameter or use s3 trigger',
            'isBase64Encoded': False,
        }

    if (cur_bucket != '' and cur_object != ''):
        return increment(event, context)
    else:
        return {
            'statusCode': 400,
            'body': 'Wrong function call - either pass method=reload as a parameter or use s3 trigger',
            'isBase64Encoded': False,
        }

handler(json.loads("""{
  "messages": [
    {
      "event_metadata": {
        "event_id": "bb1dd06d-a82c-49b4-af98-d8e0c5a1d8f0",
        "event_type": "yandex.cloud.events.storage.ObjectDelete",
        "created_at": "2019-12-19T14:17:47.847365Z",
        "tracing_context": {
          "trace_id": "dd52ace79c62892f",
          "span_id": "",
          "parent_span_id": ""
        },
        "cloud_id": "b1gvlrnlei4l5idm9cbj",
        "folder_id": "b1g88tflru0ek1omtsu0"
      },
      "details": {
"bucket_id": "vsgrab-dev",
        "object_id": "test.csv"
      }
    }
  ]
}
"""), '')