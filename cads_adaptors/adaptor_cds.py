import os
from typing import Any, BinaryIO

from . import adaptor, constraints, costing, mapping
from . import tools
from .tools import insitu_lib


import time
import zipfile
import logging
import requests
import sqlalchemy


class AbstractCdsAdaptor(adaptor.AbstractAdaptor):
    resources = {"CADS_ADAPTORS": 1}

    def __init__(self, form: dict[str, Any], **config: Any):
        self.form = form
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
        from .tools import url_tools

        data_format = request.pop("format", "zip")

        if data_format not in {"zip", "tgz"}:
            raise ValueError(f"{data_format=} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        path = url_tools.download_from_urls(
            [ru["url"] for ru in requests_urls], data_format=data_format
        )
        return open(path, "rb")


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
        assert len(format) == 1

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore
        if format[0] != "grib":
            # FIXME: reformat if needed
            pass
        return super().retrieve(mapped_request)


class DbDataset(AbstractCdsAdaptor):
    logger = logging.Logger(__name__)
    @staticmethod
    def split_request(
        full_request,  #: dict[str, Any],   #Request,
        this_values,  #: dict[str, Any],
        **kwargs,
    ):
        """
        This basic request splitter, splits based on whether the values are relevant to
        the specific adaptor.

        """
        this_request = {}
        for key, vals in full_request.items():
            this_request[key] = [v for v in vals if v in this_values.get(key, [])]
        return this_request

    @staticmethod
    def merge_results(results):
        import zipfile

        base_target = str(hash(tuple(results)))

        target = f"{base_target}.zip"

        with zipfile.ZipFile(target, mode="w") as archive:
            for p in results:
                archive.write(p)

        # for p in results:
        #     os.remove(p)

        return target

    def __init__(
        self,
        form,
        **config,  #: dict[str, Any],
    ):

        super().__init__(form, **config)
        self.adaptors = {}
        self.values = {}
        self.logger.info(config)
        for adaptor_tag, adaptor_desc in config.get("adaptors", {}).items():
            self.adaptors[adaptor_tag] = tools.get_adaptor(adaptor_desc, form)
            self.values[adaptor_tag] = adaptor_desc.get("values", {})

    def retrieve(self, request: adaptor.Request):
        self.logger.info(f"{request}, {self.config}")
        try:
            self.logger.info(f"all in:{self.config} - {dir(self)}")
        except Exception as err:
            self.logger.info(f"{err}")
        self.logger.info(f"metadata: {request.get('metadata', 'no metadata')}")

        #resource = request['metadata']['resource']
        api_url: str = self.config['api']

        dataset = api_url.split('/')[-1]
        endpoint = api_url.replace('http://', '').split('/')[0]

        self.logger.info(request)
        _q = {}
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

        version = _q.get('version', ['v1'])[0]
        source = _q.get('source', ['not specified'])

        source = source[0] if isinstance(source, list) else source

        self.logger.info("REQUEST recomposed: [{}]".format(_q))

        header, out_name = insitu_lib.insitu_utils.csv_header(api_url, _q)
        #self.logger.info(f'insitu: {header}, {out_name}')

        self.logger.info(f"REQUEST renamed: [{_q}]")

        res = requests.get(f'{api_url}/compose', params=_q)
        table = res.json().lower().split(' from ')[1].split(' ')[0]

        self.logger.info(f"db request: [{res.json()}]")
        self.logger.info(f"table: [{table}]")

        fmt = _q['format']
        fmt = fmt[0] if isinstance(fmt, list) else fmt
        self.logger.info(f'~~~~~~~ format requested: {fmt},  {_q["format"]}')

        engine = insitu_lib.insitu_utils.sql_engine(self.logger, source)

        # If not netCDF we will always need a temporary csv file
        csv_path = "temp.csv"

        insitu_lib.insitu_utils.sql_2_csv(sqlalchemy.text(res.json()), engine, csv_path)

        engine.dispose()

        self.logger.info("timing: time elapsed retrieving from db streaming to csv file %6.3f" % (time.time() - t0))
        self.logger.info(f"format requested =  {_q['format']} - {fmt}")
        # If necessary convert to one row per observation
        if not fmt in ['csv-lev.zip', 'csv.zip', '.zip', 'zip']:
            t1 = time.time()
            csv_obs_path = "temp2.csv"
            csv_path = insitu_lib.converters.baron_csv_cdm.cdm_converter(
                csv_path, source,
                dataset=dataset,
                end_point=endpoint,
                out_file=csv_obs_path
            )
            self.logger.info("timing: time elapsed converting to cdm-obs the file %6.3f" % (time.time() - t1))

            # if observation database
            if fmt in ['ODB', 'odb']:
                t2 = time.time()
                output = 'out.odb'
                insitu_lib.converters.csv2odb.convert(csv_path, output)
                self.logger.info("timing: time elapsed encoding odb %6.3f" % (time.time() - t2))
                return output

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
        # self.logger.info("timing: time elapsed compressing the file %6.3f" % (time.time() - t2))
        return output

