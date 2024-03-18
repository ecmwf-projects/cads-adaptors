from typing import BinaryIO

from cads_adaptors.adaptors import Request, cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def retrieve(self, request: Request) -> BinaryIO:
        # TODO: Remove legacy syntax all together
        if "format" in request:
            _download_format = request.pop("format")
            request.setdefault("download_format", _download_format)

        area = request.pop("area", None)

        self._pre_retrieve(request=request)

        from cads_adaptors.tools import area_selector, url_tools

        # Convert request to list of URLs
        requests_urls = url_tools.requests_to_urls(
            self.mapped_request, patterns=self.config["patterns"]
        )

        # try to download URLs
        urls = [ru["url"] for ru in requests_urls]
        paths = url_tools.try_download(urls, context=self.context)

        import os
        print('2'*100 +'\n', os.getcwd(), paths)
        if area is not None:
            paths = area_selector.area_selector_paths(paths, area, self.context)

        return self.make_download_object(paths)
