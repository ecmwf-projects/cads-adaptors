import datetime
import os

import dateutil.parser
import yaml
import sqlalchemy
import requests
import calendar
from itertools import product
import socket
from cads_adaptors.cache import cacheable


header_template = """
########################################################################################
# This file contains data retrieved from the CDS {cds_url}
# This is a C3S product under the following licences:
# {licences}
# This is a CSV file following the CDS convention {csv_convention}
# Data source: {source}
# Version: {version}
# Time extent: {first_date} - {last_date}
# Geographic area: {bbox}
# Variables selected and units
# {variables}
########################################################################################
"""


def get_public_hostname():
    return os.environ.get('PROJECT_URL', 'Project url not defined')


def is_sparse(x, translator=int):
    if isinstance(x, str):
        x = [x]
    if isinstance(x[0], list):
        x = x[0]
    x = [x] if isinstance(x, str) else x
    y = list(map(translator, x))
    if len(y) == 1:
        return False
    else:
        y.sort()
        for i in range(1, len(y)):
            if abs(y[i] - y[i - 1]) > 1:
                return True
    return False


def adjust_time(query):
    if 'time' in query:
        return query
    if is_sparse(query['year']) or \
       is_sparse(query['month']) or \
       is_sparse(query['day']):
        return query
    d = list(map(int, query['day'] if isinstance(query['day'], list) else [query['day']]))
    m = list(map(int, query['month'] if isinstance(query['month'], list) else [query['month']]))
    y = list(map(int, query['year'] if isinstance(query['year'], list) else [query['year']]))

    all_months = m == list(range(1, 13))
    all_days = d == list(range(1, 32))
    fd = 1 if all_days else min(d)
    ld = calendar.monthrange(max(y), max(m))[1] if all_days else max(d)

    fm = 1 if all_months else min(m)
    lm = 12 if all_months else max(m)

    query.update(
        {
            'time': f'{min(y):04d}-{fm:02d}-{fd:02d}T00:00:00/'
                    f'{max(y):04d}-{lm:02d}-{ld:02d}T23:59:59'
        }
    )
    del query['year']
    del query['month']
    del query['day']
    return query


def iterate_over_days(query):
    out = query.copy()
    if 'year' in query:
        del out['year']
        del out['month']
        del out['day']
        for y,m,d in product(query['year'], query['month'], query['day']):
            out.update({'time': f'{y}-{m}-{d}/{y}-{m}-{d}'})
            yield out
    elif 'time' in query:
        ts, te = [dateutil.parser.parse(_) for _ in query['time'].split('/')]
        while ts <= te:
            out.update({'time': f'{ts.strftime("%Y-%m-%d")}/{ts.strftime("%Y-%m-%d")}'})
            ts += datetime.timedelta(days=1)
            yield out

@cacheable
def par_get(url, request, out_f):
    cwd = os.getcwd()
    hostname = socket.gethostname()
    with requests.get(url, params=request, timeout=(60 * 60 * 10 * 10, 60 * 60 * 10 * 10), stream=True) as res:
        print(res.request.url)
        print(res.request.body)
        print(res.request.headers)
        print(f'yyyyyyy {res.status_code} {res.reason}')
        assert res.status_code in [200, 304], f"Error returned by the data provider: {res.content}" \
                                              f"When calling {res.request.url}"
        with open(out_f, 'wb') as f:
            f.write(res.content)
        return open(out_f, 'rb')



def get_licences(form):
    out = [_ for _ in form if _.get('type', 'not type') == 'LicenceWidget'][0]
    return '\n'.join([_.get('label', 'unspecified licence') for _ in out.get('details', {}).get('licences', [])])


def get_end_points(resource):
    with open(f'/opt/cds/forms/{resource}/generate.yaml', 'r') as f:
        api_url = yaml.safe_load(f)['api']['url']
    return api_url


def sql_engine(api_url, source, config):
    res = requests.get(
        f'{api_url}/{source}/db_engine'
    ).json()
    res['sqlalchemy.url'] = res['sqlalchemy.url'].replace('obs-insitu', config['db'])
    return sqlalchemy.engine_from_config(res)


def sql_2_csv(query, db_engine, csv_file):
    with open(csv_file, 'w') as f:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
           query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, f)

zipped_file_template = '{source}_{first_date}_{last_date}_{area_type}_{csv_convention}_{version}.csv'


def variables_units(api_url, variables, source):
    service_definition = requests.get(f'{api_url}/service_definition').json()
    descriptions = service_definition['sources'].get(source, {}).get('descriptions', {})
    out = []
    variables = [variables] if isinstance(variables, str) else variables
    variables.sort()
    for i, _v in enumerate(variables):
        v = requests.get(f'{api_url}/{source}/out_to_in/', params={'columns': [_v]}).json()[0]
        out.append(f"#\t{_v} [ {descriptions[v]['units'] if 'units' in descriptions[v] else ''} ]")
    return "\n".join(out)


def csv_header(api_url, query, config={}, form={}):

    print(query, form)
    resource = config.get('uri', 'not specified')
    source = query.get('source', ['not specified'])[0]
    variables = variables_units(api_url, query.get('variable'), source)

    y = query.get('year', ['not set'])
    y.sort()
    y0 = y[0]
    y1 = y[-1]
    m = query.get('month', ['not set'])
    m.sort()
    m0 = m[0]
    m1 = m[-1]
    d = query.get('day', ['not set'])
    d.sort()
    d0 = d[0]
    d1 = d[-1]
    area = query.get('area') if 'area' in query else 'global'

    if area != 'global':
        area = f'{area[0]}_{area[1]}_{area[2]}_{area[3]}'
    area = 'global' if area == '90_-180_-90_180' else area
    fmts = dict(
        cds_url=f"{os.environ.get('PROJECT_URL', 'cads-portal-url')}/datasets/{resource}",
        source=source,
        licences=get_licences(form),
        first_date=f'{y0}{m0}{d0}',
        last_date=f'{y1}{m1}{d1}',
        bbox=area,
        csv_convention='cdm-lev' if 'csv-lev' in query.get('format', ['csv-lev'])[0] else 'cdm-obs',
        area_type=area if area == 'global' else 'subset',
        variables=variables,
        version=query.get('version', [''])[0]
    )

    return header_template.format(**fmts), zipped_file_template.format(**fmts)
