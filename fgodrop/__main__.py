import boto3
import botocore
import os
import io
import urllib.request
import urllib.parse
import csv
import json
from itertools import groupby, zip_longest
from operator import itemgetter
import re
from decimal import Decimal
from string import ascii_lowercase


def merge(h0, h1):
    if '素材' in h0:
        return h1
    elif re.match('.石|ピース|モニュ', h0):
        if h1:
            return h1 + h0[0]
        else:
            return ''
    else:
        return h0

def base36(n):
    '''
    convert number in range(36) to base 36 string
    '''
    if n < 0 or 35 < n:
        raise ValueError(f'Invalid base36 converting from {n}')
    if n < 10:
        return str(n)
    else:
        return ascii_lowercase[n - 10]

def get_section(area):
    if '修練場' in area:
        return '修練場'
    elif area in ('冬木', 'オルレアン', 'セプテム', 'オケアノス',
                'ロンドン', '北米', 'キャメロット', 'バビロニア'):
        return '第1部'
    elif area in ('新宿', 'アガルタ', '下総国', 'セイレム'):
        return '第1.5部'
    else:
        return '第2部'

def parse(values):
    header = values[1:3]
    # forward fill
    f = ''
    header[0] = [(f := i) if i else f for i in header[0]]
    merged_header = [merge(h[0], h[1])
                     for h in zip_longest(*header)]
    table = [
        {k: v for k, v in zip(merged_header, row)}
        for row in values[3:]
        if row and row[0] not in ('', 'エリア')
    ]
    items = [
        (category, name)
        for category, name in zip(header[0], merged_header)
        if re.match('.素材|.石|ピース|モニュ', category) and name
    ]
    item_ids = {
        name: base36(i) + base36(j)
        for i, (category, group) in enumerate(groupby(items, itemgetter(0)))
        for j, (category, name) in enumerate(group)
    }
    items = [
        {'category': category, 'name': name, 'id': item_ids[name]}
        for category, name in items
    ]
    quest_info_headers = {
        'AP': 'ap',
        'サンプル数': 'samples',
        '基本絆P': 'bp',
        'EXP': 'exp',
        'QP': 'qp'
    }
    quests = [
        (get_section(area:=row['エリア']), area, row['クエスト名'])
        for row in table
    ]
    quest_ids = {
        name: base36(i) + base36(j) + base36(k)
        for i, (section, section_group) in enumerate(groupby(quests, itemgetter(0)))
        for j, (area, area_group) in enumerate(groupby(section_group, itemgetter(1)))
        for k, (section, area, name) in enumerate(area_group)
    }
    quests = [
        dict(
            **{'section': section, 'area': area, 'name': name, 'id': quest_ids[name]},
            **{
                quest_info_headers[key]: int(value.replace(',', ''))
                for key, value in row.items()
                if key in quest_info_headers and value
            }
        )
        for (section, area, name), row in zip(quests, table)
    ]
    drop_rates = [
        {
            'quest_id': quest_ids[row['クエスト名']],
            'quest_name': row['クエスト名'],
            'item_id': item['id'],
            'item_name': item['name'],
            'drop_rate': float(Decimal(value) / 100)
        }
        for row in table
        for item in items
        if (value := row.get(item['name']))
    ]
    return {
        'quests': quests,
        'items': items,
        'drop_rates': drop_rates,
    }


def export_to_dir(files, dir_):
    for file_name, rows in files.items():
        with open(dir_ + '/' + file_name + '.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

def update_s3_object(obj, body):
    try:
        response_body = obj.get()['Body'].read().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            response_body = '404'
        else:
            print(e.response)
            raise
    if response_body == '404' or response_body != body:
        obj.put(Body=body)

def export_to_s3(files, bucket_name):
    s3 = boto3.resource('s3')
    for key, rows in files.items():
        obj = s3.Object(bucket_name, key + '.csv')
        with io.StringIO(newline='') as s:
            writer = csv.DictWriter(s, rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            update_s3_object(obj, s.getvalue())

def handler(event, context):
    client = boto3.client(service_name='secretsmanager')
    secret_name = os.environ['SECRET_NAME']
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    google_sheets_api_key = secret['GOOGLE_SHEETS_API_KEY']
    spreadsheet_id = '1DxFVWa1xsBh-TJVVTrJf7ttVxf7msCHhxuZyM-shPx0'
    query = {
        'ranges': 'ドロップ率表!A:DC',
        'key': google_sheets_api_key
    }
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet?{urllib.parse.urlencode(query)}'
    with urllib.request.urlopen(url) as response:
        body = response.read()
    body = json.loads(body)
    if 'error' in body:
        return body
    values = body['valueRanges'][0]['values']
    parsed = parse(values)
    export_to_s3(parsed, 'fgodrop')
