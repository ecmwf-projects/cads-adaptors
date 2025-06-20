import os
import time
import zipfile
from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor


class InsituDatabaseCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_id = self.config.get(
            "collection_id", self.config.get("uri", "not specified")
        )

    def retrieve(self, request: Request) -> BinaryIO:
        import requests
        import sqlalchemy

        from cads_adaptors.adaptors.insitu.tools import csvlev2obs, insitu_utils

        self.context.logger.debug(f"{request},\n\n {self.config} \n\n {self.form}")
        try:
            self.context.logger.debug(f"all in:{self.config} - {dir(self)}")
        except Exception as err:
            self.context.logger.debug(f"{err}")

        api_url: str = self.config["api"]

        dataset = api_url.split("/")[-1]
        endpoint = api_url.replace("http://", "").split("/")[0]

        request = mapping.apply_mapping(request, self.mapping, context=self.context)

        _q = {}
        if isinstance(request, list):
            for q in request:
                q.setdefault("format", ["nc"])
                if _q == {}:
                    _q.update(q)
                else:
                    for _k in _q:
                        if _k == "area":
                            _q[_k] = q[_k]
                        else:
                            _q[_k] = list(set(_q[_k]).union(set(q[_k])))
        else:
            request.setdefault("format", "nc")
            for q in request:
                _q[q] = [request[q]] if not isinstance(request[q], list) else request[q]
        self.context.logger.debug(request)

        _q.get("version", ["v1"])[0]
        source = _q.get("source", ["not specified"])

        source = source[0] if isinstance(source, list) else source

        self.context.logger.debug("REQUEST recomposed: [{}]".format(_q))

        header, out_name = insitu_utils.csv_header(
            api_url, _q, self.collection_id, self.config, self.form
        )

        self.context.logger.debug(f"REQUEST renamed: [{_q}]")

        sql_equivalent = requests.get(f"{api_url}/compose", params=_q)
        table = sql_equivalent.json().lower().split(" from ")[1].split(" ")[0]

        self.context.logger.debug(f"db request: [{sql_equivalent.json()}]")
        self.context.logger.debug(f"table: [{table}]")

        fmt = _q["format"]
        fmt = fmt[0] if isinstance(fmt, list) else fmt
        self.context.logger.debug(f'~~~~~~~ format requested: {fmt},  {_q["format"]}')

        engine = insitu_utils.sql_engine(api_url, source, self.config)

        # If not netCDF we will always need a temporary csv file
        csv_path = "temp.csv"
        t0 = time.time()
        insitu_utils.sql_2_csv(sqlalchemy.text(sql_equivalent.json()), engine, csv_path)

        engine.dispose()

        self.context.logger.info(
            "timing: time elapsed retrieving from db streaming to csv file %6.3f"
            % (time.time() - t0)
        )
        self.context.logger.info(f"format requested =  {_q['format']} - {fmt}")
        # If necessary convert to one row per observation
        if fmt not in ["csv-lev.zip", "csv.zip", ".zip", "zip"]:
            t1 = time.time()
            csv_obs_path = "temp2.csv"
            csv_path = csvlev2obs.cdm_converter(
                csv_path,
                source,
                dataset=dataset,
                end_point=endpoint,
                out_file=csv_obs_path,
            )
            self.context.logger.info(
                "timing: time elapsed converting to cdm-obs the file %6.3f"
                % (time.time() - t1)
            )

        t2 = time.time()
        # prepending the header to the output file
        csv_path_out = "tmp3.csv"
        with open(csv_path_out, "w", encoding="utf-8") as fo:
            fo.write(header)
            with open(csv_path, "r", encoding="utf-8") as fi:
                while True:
                    data = fi.read(65536)
                    if data:
                        fo.write(data)
                    else:
                        break

        output = f"{out_name}.zip"
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path_out, out_name)
        self.context.logger.info(
            "timing: time elapsed compressing the file %6.3f" % (time.time() - t2)
        )
        return open(output, "rb")

    def csv_header(self, api_url, query):
        from cads_adaptors.adaptors.insitu.tools import insitu_utils

        self.context.logger.debug(query, self.form)
        source = query.get("source", ["not specified"])[0]
        variables = insitu_utils.variables_units(api_url, query.get("variable"), source)

        y = query.get("year", ["not set"])
        y.sort()
        y0 = y[0]
        y1 = y[-1]
        m = query.get("month", [f"{_:%02d}" for _ in range(1, 13)])
        m.sort()
        m0 = m[0]
        m1 = m[-1]
        d = query.get("day", [f"{_:%02d}" for _ in range(1, 32)])
        d.sort()
        d0 = d[0]
        d1 = d[-1]
        area = query.get("area") if "area" in query else "global"

        if area != "global":
            area = f"{area[0]}_{area[1]}_{area[2]}_{area[3]}"
        area = "global" if area == "90_-180_-90_180" else area
        fmts = dict(
            cds_url=f"{os.environ.get('PROJECT_URL', 'cads-portal-url')}/datasets/{self.collection_id}",
            source=source,
            licences=insitu_utils.get_licences(self.form),
            first_date=f"{y0}{m0}{d0}",
            last_date=f"{y1}{m1}{d1}",
            bbox=area,
            csv_convention="cdm-lev"
            if "csv-lev" in query.get("format", ["csv-lev"])[0]
            else "cdm-obs",
            area_type=area if area == "global" else "subset",
            variables=variables,
            version=query.get("version", [""])[0],
        )

        return insitu_utils.header_template.format(
            **fmts
        ), insitu_utils.zipped_file_template.format(**fmts)
