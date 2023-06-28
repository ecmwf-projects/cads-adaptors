import os

import yaml
import sqlalchemy
import requests

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
    with open('/etc/cds/settings.yaml', 'r') as f:
        host = yaml.load(f, Loader=yaml.SafeLoader)['publichost']['publichost']
        return f'https://{host}'


def get_licences(resource):
    return 'licence is udefined'


def get_end_points(resource):
    with open(f'/opt/cds/forms/{resource}/generate.yaml', 'r') as f:
        api_url = yaml.safe_load(f)['api']['url']
    return api_url


<<<<<<< HEAD
def sql_engine(api_url, source):
    return sqlalchemy.engine_from_config(
        requests.get(
<<<<<<< HEAD
            f'{api_url}/db_engine'
=======
            f'{api_url}/{source}/db_engine'
>>>>>>> bbe23be (install develop packages before starting service)
        ).json()
    )
=======
def sql_engine(api_url, source, config):
    res = requests.get(
        f'{api_url}/{source}/db_engine'
    ).json()
    res['sqlalchemy.url'] = res['sqlalchemy.url'].replace('obs-insitu', config['db'])
    return sqlalchemy.engine_from_config(res)
>>>>>>> 5955e33 (install develop packages before starting service)


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
    resource = 'a dataset to be specified'#context.request['metadata']['resource']
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
        licences=get_licences(resource),
        first_date=f'{y0}{m0}{d0}',
        last_date=f'{y1}{m1}{d1}',
        bbox=area,
        csv_convention='cdm-lev' if 'csv-lev' in query.get('format', ['csv-lev'])[0] else 'cdm-obs',
        area_type=area if area == 'global' else 'subset',
        variables=variables,
        version=query.get('version', [''])[0]
    )

    return header_template.format(**fmts), zipped_file_template.format(**fmts)
