from cdscompute.decorators import configure
from cds_common.system import utils
from . import baron_csv_cdm, insitu_utils
from .converters import csv2netcdf, csv2odb
import sqlalchemy
import requests
import pandas as pd
import zipfile
import time


endpoints = {
    'insitu-observations-gruan-reference-network': 'obs-base-ref',
    'insitu-observations-igra-baseline-network': 'obs-base-ref',
    'insitu-observations-total-column-ozone-ozonosounding': 'obs-woudc'
}

db_users = {
    'obs-base-ref': 'baron',
    'obs-woudc': 'baron_woudc',
}

db_name = {
    'obs-base-ref': 'baron',
    'obs-woudc': 'woudc',
}

def sql_engine(context):
    resource = context.request['metadata']['resource']
    endpoint = endpoints[resource]

    ENGINE_CONFIG = {
        'sqlalchemy.url': f'postgresql+psycopg2://{db_users[endpoint]}@{endpoint}.shared.cds/{db_name[endpoint]}',
        'sqlalchemy.echo': False,
        'sqlalchemy.pool_size': 1,
        'sqlalchemy.pool_recycle': 300,
    }
    return sqlalchemy.engine_from_config(ENGINE_CONFIG)

header_template = """
########################################################################################
# This file contains data retrieved from the CDS {cds_url}
# This is a C3S product under the following licences:
{licences}
# This is a CSV file following the CDS convention {csv_convention}
# Data source: {source}
# Time extent: {first_date} - {last_date}
# Geographic area: {bbox}
# Variables selected and units
{variables}
########################################################################################
"""

zipped_file_template = '{source}_{first_date}_{last_date}_{area_type}_{csv_convention}.csv'


def variables_units(context, variables, source):
    resource = context.request['metadata']['resource']
    service_definition = requests.get(f'http://{endpoints[resource]}/api/service_definition').json()
    context.debug('variables')
    descriptions = service_definition['sources'][source]['descriptions']
    out = []
    variables.sort()
    for i, _v in enumerate(variables):
        v = requests.get(f'http://{endpoints[resource]}/api/out_to_in/', params={'source': source, 'columns': [_v]}).json()[0]
        out.append(f"#\t{_v} [ {descriptions[v]['units'] if 'units' in descriptions[v] else ''} ]")
    return "\n".join(out)


def csv_header(context, query):
    resource = context.request['metadata']['resource']
    source = query.get('source', ['not specified'])[0]
    variables = variables_units(context, query.get('variable'), source)

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
        cds_url=utils.get_public_hostname() + '/cdsapp#!/dataset/' + resource,
        source=source,
        licences=insitu_utils.get_licences(resource),
        first_date=f'{y0}{m0}{d0}',
        last_date=f'{y1}{m1}{d1}',
        bbox=area,
        csv_convention='cdm-lev' if 'csv-lev' in query.get('format', ['csv-lev'])[0] else 'cdm-obs',
        area_type=area if area == 'global' else 'subset',
        variables=variables,
        version='Not Available'
    )

    return header_template.format(**fmts), zipped_file_template.format(**fmts)


@configure(cacheable=True, name='retrieve_legacy')
def retrieve(context, query):
    try:
        context.debug(f"all in:{context.config} - {context.fullconfig}")
    except Exception as err:
        context.debug(f"{err}")
    context.debug(f"metadata: {context.request['metadata']}")
    engine = sql_engine(context)

    resource = context.request['metadata']['resource']
    endpoint = endpoints[resource]

    is_toolbox = False #context.request['metadata']['origin'] == 'subrequest'

    context.info(query)
    _q = {}
    if isinstance(query, list):
        for q in query:
            q.setdefault('format', ['nc'])
            if _q == {}:
                _q.update(q)
            else:
                for _k in _q:
                    if _k == 'area':
                        _q[_k] = q[_k]
                    else:
                        _q[_k] = list(set(_q[_k]).union(set(q[_k])))
    else:
        query.setdefault('format', 'nc')
        for q in query:
            _q[q] = [query[q]] if not isinstance(query[q], list) else query[q]

    version = _q.get('version', ['v1'])[0]
    source = _q.get('source', ['not specified'])
    source = source[0] if isinstance(source, list) else source

    context.info("REQUEST recomposed: [{}]".format(_q))

    header, out_name = csv_header(context, _q)
    context.debug(f'insitu: {header}, {out_name}')

    try:
        if endpoint != 'obs-woudc':
            _q['variable'] = requests.get(f'http://{endpoint}/api/out_to_in/', params= {'source': _q['source'][0], 'columns': _q['variable']}).json()
    except Exception as err:
        context.info(f"ERROR: [{err}]".format(err))

    context.debug(f"REQUEST renamed: [{_q}]")

    api_url = f'http://{endpoint}/api/{version}/compose'

    res = requests.get(api_url, params=_q)

    context.debug(f"db query: [{res.json()}]")

    fmt = 'csv-lev.zip' if is_toolbox else _q['format'][0]
    context.debug(f'~~~~~~~ format requested: {fmt},  {_q["format"]}')

    if is_toolbox or _q['format'][0] in ['nc', 'cdf', 'netcdf']:
        _csv = context.create_temp_file('.csv')
        insitu_utils.sql_2_csv(sqlalchemy.text(res.json()), engine, _csv)
        flist = csv2netcdf.convert(
            _csv,
            service_definition=requests.get(f'http://{endpoints[resource]}/api/service_definition').json(),
            context=context,
            columns=_q['variable']
        )
        context.debug(f'netcdf files: {flist}')
        output = context.create_result_file('.zip')
        with zipfile.ZipFile(output.path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for v in flist:
                zipf.write(flist[v], f"{_q['source'][0]}_{v}.nc")
        return output

    csv_path = context.create_temp_file(".csv")

    t0 = time.time()
    insitu_utils.sql_2_csv(sqlalchemy.text(res.json()), engine, csv_path)
    engine.dispose()
    context.debug("timing: time elapsed retrieving from db streaming to csv file %6.3f" % (time.time() - t0))

    if not fmt in ['csv-lev.zip', 'csv.zip', '.zip', 'zip'] :
        t2 = time.time()
        csv_obs_path = context.create_temp_file(".csv")
        if source == 'GRUAN':
            csv_path = baron_csv_cdm.baron_cdm_converter(csv_path, source=source, out_file=csv_obs_path)
        else:
            csv_path = baron_csv_cdm.cdm_converter(csv_path, source, end_point=endpoint, out_file=csv_obs_path)
        context.debug("timing: time elapsed converting to cdm-obs the file %6.3f" % (time.time() - t2))

    if fmt in ['ODB', 'odb']:
        t2 = time.time()
        output = context.create_result_file('.odb')
        csv2odb.convert(csv_path, output.path)
        context.debug("timing: time elapsed encoding odb %6.3f" % (time.time() - t2))

    elif fmt in ['nc', 'NetCDF', 'netcdf']:
        '''
        here the files are going to be created in a large number
        on file per variable and station ID
        Not all the csv file can be loaded in memory, 
        so it is required to be able to append to existing files created
        
        '''
        pass

    else:
        t2 = time.time()
        # prepending the header to the output file
        csv_path_out = context.create_temp_file(".csv")
        with open(csv_path_out, 'w', encoding='utf-8') as fo:
            fo.write(header)
            with open(csv_path, 'r', encoding='utf-8') as fi:
                while True:
                    data = fi.read(65536)
                    if data:
                        fo.write(data)
                    else:
                        break

        output = context.create_result_file('.zip')
        with zipfile.ZipFile(output.path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path_out, out_name)
        context.debug("timing: time elapsed compressing the file %6.3f" % (time.time() - t2))

    return [output] if context.request['metadata']['origin'] == 'subrequest' else output


@configure(cacheable=False, name='cost')
def cost(context, query):
    context.debug("REQUEST: [{}]".format(query))

    _query = insitu_utils.query_cost(query, clean=True)

    context.debug("REQUEST: [{}]".format(_query,))

    with sql_engine(context).begin() as conn:
        df = pd.read_sql(_query, conn)

    return float(df['num_rows'][0])


execute = [retrieve, cost]
