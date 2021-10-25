from collections import defaultdict
import boto3
import botocore
import os
import urllib.request
import urllib.parse
import json
from itertools import groupby
from operator import itemgetter
import re
from decimal import Decimal
from string import ascii_lowercase
import gzip
import io


def merge_header(h0, h1):
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
    elif n < 10:
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


def parse(values, version):
    header = values[1:3]
    # forward fill
    f = ''
    header[0] = [(f := i) if i else f for i in header[0]]
    merged_header = [merge_header(h[0], h[1])
                     for h in zip(*header)]
    table = [
        {k: v for k, v in zip(merged_header, row) if v}
        for row in values[3:]
        if row and row[0] not in ('', 'エリア', 'HOME')
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
        'サンプル数': 'samples_' + version,
        '基本絆P': 'bp',
        'EXP': 'exp',
        'QP': 'qp'
    }
    quests = [
        (get_section(area := row['エリア']), area, row['クエスト名'])
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
    for quest in quests:
        if quest['id'][0] == '0':
            quest['ap'] = (4 - int(quest['id'][-1])) * 10
    drop_rates = [
        {
            'quest_id': quest_ids[row['クエスト名']],
            'item_id': item['id'],
            'drop_rate_' + version: float(Decimal(value) / 100)
        }
        for row in table
        for item in items
        if (value := row.get(item['name']))
        and not value.startswith('#')
    ]
    return {
        'quests': quests,
        'items': items,
        'drop_rates': drop_rates,
    }


def merge(src, dst):
    return {
        'items': merge_rows(src['items'], dst['items'], 'id'),
        'quests': merge_rows(src['quests'], dst['quests'], 'id'),
        'drop_rates': merge_rows(src['drop_rates'], dst['drop_rates'], 'item_id', 'quest_id')
    }


def merge_rows(src, dst, *keys):
    dd = defaultdict(dict)
    for row in src + dst:
        dd[''.join(row[key] for key in keys)].update(row)
    return list(dd.values())


def get_gzip(obj):
    body = obj.get()['Body'].read()
    bio = io.BytesIO(body)
    with gzip.open(bio, 'rt', encoding='utf-8') as f:
        d = json.load(f)
    for key, value in d.items():
        d[key] = [
            {k: v for k, v in row.items() if v != ""}
            for row in value
        ]
    d['quests'] = [{**row, 'samples_1': int(row['samples_1'])}
                   for row in d['quests'] if 'samples_1' in row]
    d['drop_rates'] = [{**row, 'drop_rate_1': float(row['drop_rate_1'])}
                       for row in d['drop_rates'] if 'drop_rate_1' in row]
    return d


def put_gzip(obj, body):
    bio = io.BytesIO()
    with gzip.open(bio, 'wt', encoding='utf-8') as f:
        json.dump(body, f, ensure_ascii=False)
    obj.put(Body=bio.getvalue())


def export_to_s3(body):
    s3 = boto3.resource('s3')
    obj = s3.Object('fgodrop', 'all.json.gz')
    old = get_gzip(obj)
    new = merge(old, body)
    if old != new:
        put_gzip(obj, new)


def get_secret(key):
    client = boto3.client(service_name='secretsmanager')
    secret_name = os.environ['SECRET_NAME']
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret[key]


def get_values(spreadsheet_id, spreadsheet_range, api_key):
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{spreadsheet_range}?key={api_key}'
    with urllib.request.urlopen(url) as response:
        body = response.read()
    body = json.loads(body)
    if 'error' in body:
        return body
    return body['values']


def handler(event, context):
    #spreadsheet_id = '1DxFVWa1xsBh-TJVVTrJf7ttVxf7msCHhxuZyM-shPx0'
    #spreadsheet_id = '1CmH3z71ymRJMlBO11cBthABxKuqdHrzXwiKa3cqRrMQ'
    spreadsheet_id = '1qjiymRgcpdAYv201jdzcfRSPKrNaquNRJGIRYFlaimo'
    values = get_values(
        spreadsheet_id=spreadsheet_id,
        spreadsheet_range=urllib.parse.quote('ドロップ率表'),
        api_key=os.environ.get('GOOGLE_SHEETS_API_KEY')
    )
    version = '2'
    parsed = parse(values, version)
    export_to_s3(parsed)
