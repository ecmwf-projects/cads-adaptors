import os
import zipfile
from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.tools.logger import logger


class InsituGlamodCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_id = self.config.get(
            "collection_id", self.config.get("uri", "not specified")
        )

    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.adaptors.insitu.tools import insitu_utils

        resource = self.config["uri"]
        domain = "land" if "land" in resource else "marine"
        logger.debug(f"request:::::::{resource} {request}")
        request = mapping.apply_mapping(request, self.mapping)
        logger.debug(f'request{"~" * 10}{resource} {request}')

        url = self.config["urls"]["requests"]

        _q = request.copy()

        _q["domain"] = domain
        _q["compress"] = "true"
        if "area" in _q:
            bbox = _q.get(
                "area", [90, -180, -90, 180]
            )  # what service want : [<w>,<s>,<e>,<n>]
            # {'n':90., 'w': -180., 's': -90., 'e': 180.})
            bbox = ",".join([str(bbox[i]) for i in [1, 2, 3, 0]])
            _q["bbox"] = bbox
            if bbox == "180,90,-180,-90":
                _q.pop("bbox", None)
                _q.pop("area", None)
        # _q = insitu_utils.adjust_time(_q)
        _q.pop("format", None)

        mid_processing = "tmp.zip"
        # import dask
        with zipfile.ZipFile(mid_processing, "a") as z_out:
            # outs = [dask.delayed(insitu_utils.par_get)(url, __q, f'/tmp/tmp_{i}.zip')
            #         for i, __q in enumerate(insitu_utils.iterate_over_days(_q))]
            # outs = dask.compute(*outs)
            outs = [
                insitu_utils.par_get(url, __q, f"tmp_{i}.zip")
                for i, __q in enumerate(insitu_utils.iterate_over_days(_q))
            ]

            for azf_url in outs:
                try:
                    with zipfile.ZipFile(azf_url, "r") as z:
                        for zitem in z.namelist():
                            # assuming zitem is not a memory blowing up file
                            z_out.writestr(zitem, z.read(zitem))
                    os.remove(azf_url)
                except Exception as _err:
                    logger.warning(msg=f"failed unexpected {_err.__str__()}", exc_info=True)
                except FileNotFoundError:
                    logger.warning(msg=f"failed {azf_url}", exc_info=True)

        return open(mid_processing, "rb")
