from typing import Any, BinaryIO

from cads_adaptors.adaptors import Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.area: list[Any] | None = None

    def pre_mapping_modifications(self, request: Request[str, Any]) -> Request[str, Any]:
        request = super().pre_mapping_modifications(request)

        # TODO: Remove legacy syntax all together
        _download_format = request.pop("format", "zip")
        request.setdefault("download_format", _download_format)

        self.area = request.pop("area", None)

        return request

    def retrieve(self, request: Request) -> BinaryIO:
        from cads_adaptors.tools import area_selector, url_tools

        # Convert request to list of URLs
        requests = [
            self.apply_mapping(request)
            for request in self.intersect_constraints(request)
        ]
        requests_urls = url_tools.requests_to_urls(
            requests,
            patterns=self.config["patterns"],
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

        if self.area is not None:
            paths = area_selector.area_selector_paths(paths, self.area, self.context)

        return self.make_download_object(paths)
