from copy import deepcopy
from typing import Any, BinaryIO

from cads_adaptors import mapping
from cads_adaptors.adaptors import Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        # TODO: Remove legacy syntax all together
        if "format" in request:
            _download_format = request.pop("format")
            request.setdefault("download_format", _download_format)

        area = request.pop("area", None)

        # Not using common _pre_retrieve, as we are now dealing with lists of re
        # self._pre_retrieve(request=request)

        self.input_request = deepcopy(request)

        self.receipt = request.pop("receipt", False)

        valid_requests = self.intersect_constraints(request)

        self.mapped_requests = [
            mapping.apply_mapping(valid_request, self.mapping)
            for valid_request in valid_requests
        ]

        self.download_format = [
            mapped_request[0].pop("download_format", "zip")
            for mapped_request in self.mapped_requests
        ][0]

        from cads_adaptors.tools import area_selector, url_tools

        # Convert request to list of URLs
        requests_urls = url_tools.requests_to_urls(
            self.mapped_requests, patterns=self.config["patterns"]
        )

        # try to download URLs
        urls = [ru["url"] for ru in requests_urls]
        download_kwargs: dict[str, Any] = self.config.get("download_kwargs", {})
        # Handle legacy syntax for authentication
        if "auth" in self.config:
            download_kwargs.setdefault(
                "auth",
                (self.config["auth"]["username"], self.config["auth"]["password"]),
            )
        paths = url_tools.try_download(urls, context=self.context, **download_kwargs)

        if area is not None:
            paths = area_selector.area_selector_paths(paths, area, self.context)

        return self.make_download_object(paths)
