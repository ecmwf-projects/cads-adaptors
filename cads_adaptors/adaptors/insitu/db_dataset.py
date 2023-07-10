import os
import shutil
from typing import Any, BinaryIO

from cads_adaptors import adaptor, constraints, costing, mapping
from cads_adaptors.adaptor_cds import AbstractCdsAdaptor

from cads_adaptors.adaptors.insitu import insitu_lib


import time
import zipfile
import logging
import requests
import sqlalchemy

# import dask


class DbDataset(AbstractCdsAdaptor):
    logger = logging.Logger(__name__)

    def retrieve(self, request: adaptor.Request):

        from .insitu_lib import insitu_utils, baron_csv_cdm

        print(f"{request},\n\n {self.config} \n\n {self.form}")
        try:
            print(f"all in:{self.config} - {dir(self)}")
        except Exception as err:
            print(f"{err}")
        print(f"metadata: {request.get('metadata', 'no metadata')}")

        api_url: str = self.config['api']

        dataset = api_url.split('/')[-1]
        endpoint = api_url.replace('http://', '').split('/')[0]

        print(request)
        _q = {}

        request = mapping.apply_mapping(request, self.mapping)

        if isinstance(request, list):
            for q in request:
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
            request.setdefault('format', 'nc')
            for q in request:
                _q[q] = [request[q]] if not isinstance(request[q], list) else request[q]
        print(request)

        version = _q.get('version', ['v1'])[0]
        source = _q.get('source', ['not specified'])

        source = source[0] if isinstance(source, list) else source

        print("REQUEST recomposed: [{}]".format(_q))

        header, out_name = insitu_utils.csv_header(api_url, _q, self.config, self.form)
        #self.logger.info(f'insitu: {header}, {out_name}')

        print(f"REQUEST renamed: [{_q}]")

        res = requests.get(f'{api_url}/compose', params=_q)
        table = res.json().lower().split(' from ')[1].split(' ')[0]

        print(f"db request: [{res.json()}]")
        print(f"table: [{table}]")

        fmt = _q['format']
        fmt = fmt[0] if isinstance(fmt, list) else fmt
        print(f'~~~~~~~ format requested: {fmt},  {_q["format"]}')

        engine = insitu_utils.sql_engine(api_url, source, self.config)

        # If not netCDF we will always need a temporary csv file
        csv_path = "temp.csv"
        t0 = time.time()
        insitu_utils.sql_2_csv(sqlalchemy.text(res.json()), engine, csv_path)

        engine.dispose()

        print("timing: time elapsed retrieving from db streaming to csv file %6.3f" % (time.time() - t0))
        print(f"format requested =  {_q['format']} - {fmt}")
        # If necessary convert to one row per observation
        if not fmt in ['csv-lev.zip', 'csv.zip', '.zip', 'zip']:
            t1 = time.time()
            csv_obs_path = "temp2.csv"
            csv_path = baron_csv_cdm.cdm_converter(
                csv_path, source,
                dataset=dataset,
                end_point=endpoint,
                out_file=csv_obs_path
            )
            print("timing: time elapsed converting to cdm-obs the file %6.3f" % (time.time() - t1))

        t2 = time.time()
        # prepending the header to the output file
        csv_path_out = 'tmp3.csv'
        with open(csv_path_out, 'w', encoding='utf-8') as fo:
            fo.write(header)
            with open(csv_path, 'r', encoding='utf-8') as fi:
                while True:
                    data = fi.read(65536)
                    if data:
                        fo.write(data)
                    else:
                        break

        output = f'{out_name}.zip'
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path_out, out_name)
        print("timing: time elapsed compressing the file %6.3f" % (time.time() - t2))
        return open(output, 'rb')
