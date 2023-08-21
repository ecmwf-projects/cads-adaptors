from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Context, Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def retrieve(self, request: Request, context: Context) -> BinaryIO:
        from cads_adaptors.tools import download_tools, url_tools

        download_format = request.pop("format", "zip")  # TODO: Remove legacy syntax
        # CADS syntax over-rules legacy syntax
        download_format = request.pop("download_format", download_format)

        # Do not need to check twice
        # if download_format not in {"zip", "tgz"}:
        #     raise ValueError(f"{download_format} is not supported")

        mapped_request = mapping.apply_mapping(request, self.mapping)  # type: ignore

        # Convert request to list of URLs
        requests_urls = url_tools.requests_to_urls(
            mapped_request, patterns=self.config["patterns"]
        )

        # try to download URLs
        urls = [ru["url"] for ru in requests_urls]
        paths = url_tools.try_download(urls)

        download_kwargs = {"base_target": f"{self.collection_id}-{hash(tuple(urls))}"}

        return download_tools.DOWNLOAD_FORMATS[download_format](
            paths, **download_kwargs
        )
