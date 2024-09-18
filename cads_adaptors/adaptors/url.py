from typing import Any

from cads_adaptors.adaptors import Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def multi_retrieve(self, request: Request) -> cds.T_MULTI_RETRIEVE:
        # TODO: Remove legacy syntax all together
        if "format" in request:
            _download_format = request.pop("format")
            request.setdefault("download_format", _download_format)

        area = request.pop("area", None)

        self._pre_retrieve(request=request)

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

        if area is not None:
            paths = area_selector.area_selector_paths(paths, area, self.context)

        return self.make_download_object(paths)
