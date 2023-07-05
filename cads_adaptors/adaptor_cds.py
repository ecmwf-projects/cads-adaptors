import os
from typing import Any, BinaryIO

from cads_adaptors import adaptor, constraints, costing, mapping
from . import tools

import time
import zipfile
import logging
import requests
import sqlalchemy

import dask


class AbstractCdsAdaptor(adaptor.AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}

    def __init__(self, form: dict[str, Any], **config: Any):
        self.form = form
        self.collection_id = config.pop("collection_id", "unknown-collection")
        self.constraints = config.pop("constraints", [])
        self.mapping = config.pop("mapping", {})
        self.licences: list[tuple[str, int]] = config.pop("licences", [])
        self.config = config

    def validate(self, request: adaptor.Request) -> bool:
        return True

    def apply_constraints(self, request: adaptor.Request) -> dict[str, Any]:
        return constraints.validate_constraints(self.form, request, self.constraints)

    def estimate_costs(self, request: adaptor.Request) -> dict[str, int]:
        costs = {"size": costing.estimate_size(self.form, request, self.constraints)}
        return costs

    def get_licences(self, request: adaptor.Request) -> list[tuple[str, int]]:
        return self.licences


class UrlCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        from cads_adaptors.tools import download_tools, url_tools

        download_format = request.pop("format", "zip")  # TODO: Remove legacy syntax
        # CADS syntax over-rules legacy syntax
        download_format = request.pop("download_format", download_format)

        # Do not need to check twice
        # if download_format not in {"zip", "tgz"}:
        #     raise ValueError(f"{download_format} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        urls = [ru["url"] for ru in requests_urls]
        paths = url_tools.try_download(urls)

        download_kwargs = dict(base_target=f"{self.collection_id}-{hash(tuple(urls))}")

        return download_tools.DOWNLOAD_FORMATS[download_format](
            paths, **download_kwargs
        )


class LegacyCdsAdaptor(AbstractCdsAdaptor):
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        import cdsapi

        # parse input options
        collection_id = self.config.pop("collection_id", None)
        if not collection_id:
            raise ValueError("collection_id is required in request")

        # retrieve data
        client = cdsapi.Client(self.config["url"], self.config["key"], retry_max=1)
        result_path = client.retrieve(collection_id, request).download()
        return open(result_path, "rb")


class DirectMarsCdsAdaptor(AbstractCdsAdaptor):
    resources = {"MARS_CLIENT": 1}

    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        import subprocess

        with open("r", "w") as fp:
            print("retrieve, target=data.grib", file=fp)
            for key, value in request.items():
                if not isinstance(value, (list, tuple)):
                    value = [value]
                print(f", {key}={'/'.join(str(v) for v in value)}", file=fp)

        env = dict(**os.environ)
        # FIXME: set with the namespace and user_id
        namespace = "cads"
        user_id = 0
        env["MARS_USER"] = f"{namespace}-{user_id}"

        subprocess.run(["/usr/local/bin/mars", "r"], check=True, env=env)

        return open("data.grib")  # type: ignore


class MarsCdsAdaptor(DirectMarsCdsAdaptor):
    def retrieve(self, request: adaptor.Request) -> BinaryIO:
        format = request.pop("format", ["grib"])
        request.pop("download_format", "raw")
        assert len(format) == 1

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore
        if format[0] != "grib":
            # FIXME: reformat if needed
            pass
        return super().retrieve(mapped_request)


class DbDataset(AbstractCdsAdaptor):
    logger = logging.Logger(__name__)

    def __init__(
        self,
        form,
        **config,  #: dict[str, Any],
    ):

        super().__init__(form, **config)
        #print(form, config)
        self.adaptors = {}
        self.values = {}
        for adaptor_tag, adaptor_desc in config.get("adaptors", {}).items():
            self.adaptors[adaptor_tag] = tools.get_adaptor(adaptor_desc, form)
            self.values[adaptor_tag] = adaptor_desc.get("values", {})

    def retrieve(self, request: adaptor.Request):
        from .tools.insitu_lib import insitu_utils, baron_csv_cdm

        print(f"{request},\n\n {self.config} \n\n {self.form}")
        try:
            print(f"all in:{self.config} - {dir(self)}")
        except Exception as err:
            print(f"{err}")
        print(f"metadata: {request.get('metadata', 'no metadata')}")

        api_url: str = self.config['api']

        dataset = api_url.split('/')[-1]
        endpoint = api_url.replace('http://', '').split('/')[0]

        self.logger.info(request)
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

        self.logger.info("REQUEST recomposed: [{}]".format(_q))

        header, out_name = insitu_utils.csv_header(api_url, _q, self.config, self.form)
        #self.logger.info(f'insitu: {header}, {out_name}')

        self.logger.info(f"REQUEST renamed: [{_q}]")

        res = requests.get(f'{api_url}/compose', params=_q)
        table = res.json().lower().split(' from ')[1].split(' ')[0]

        self.logger.info(f"db request: [{res.json()}]")
        self.logger.info(f"table: [{table}]")

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

class GlamodDb(DbDataset):
    def retrieve(self, request: adaptor.Request):
        from .tools.insitu_lib import insitu_utils
        resource = self.config['uri']
        domain = 'land' if 'land' in resource else 'marine'
        print(f'request:::::::{resource} {request}')
        request = mapping.apply_mapping(request, self.mapping)
        print(f'request{"~" * 10}{resource} {request}')

        # url = self.config['urls']['requests'].replace(
        #     self.config['urls']['internal']['pattern'],
        #     self.config['urls']['internal']['ip']
        # )
        # print(url)
        url = self.config['urls']['requests']

        _q = request

        _q['domain'] = domain
        _q['compress'] = 'true'
        if 'area' in _q:
            bbox = _q.get('area', [90, -180, -90, 180])  # what service want : [<w>,<s>,<e>,<n>]
            # {'n':90., 'w': -180., 's': -90., 'e': 180.})
            bbox = ','.join([str(bbox[i]) for i in [1, 2, 3, 0]])
            _q['bbox'] = bbox
            if bbox == '180,90,-180,-90':
                _q.pop('bbox', None)
                _q.pop('area', None)
        #_q = insitu_utils.adjust_time(_q)
        _q.pop('format', None)

        mid_processing = 'tmp.zip'

        with zipfile.ZipFile(mid_processing, 'a') as z_out:
            outs = [dask.delayed(insitu_utils)(url, __q, f'tmp_{i}.zip')
                    for i, __q in enumerate(insitu_utils.iterate_over_days(_q))]
            outs = dask.compute(*outs)
            print(outs)
            for azf in outs:
                with zipfile.ZipFile(azf, 'r') as z:
                    for zitem in z.namelist():
                        # assuming zitem is not a memory blowing up file
                        z_out.writestr(zitem, z.read(zitem))

        return open(mid_processing, 'rb')
