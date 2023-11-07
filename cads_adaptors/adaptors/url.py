from typing import BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        super().retrieve(request=request)

        from cads_adaptors.tools import url_tools

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

        return self.make_download_object(paths)
