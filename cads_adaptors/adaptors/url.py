from typing import Any

from cads_adaptors import mapping
from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, ProcessingKwargs, Request


class UrlCdsAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # URL adaptor should default to intersecting constraints
        self.intersect_constraints_bool: bool = self.config.get(
            "intersect_constraints", True
        )

    def pre_mapping_modifications(
        self, request: dict[str, Any]
    ) -> tuple[Request, ProcessingKwargs]:
        request, kwargs = super().pre_mapping_modifications(request)

        # TODO: Remove legacy syntax all together
        download_format = request.pop("format", kwargs["download_format"])
        download_format = request.pop("download_format", download_format)
        kwargs["download_format"] = self.get_download_format(download_format)

        # TODO: Implement for all adaptors in normalise_request
        request = mapping.area_as_mapping(request, self.mapping, self.context)

        # If area still in request it is to be used by the area selector post-processor
        kwargs["area"] = request.pop("area", None)

        return request, kwargs

    def retrieve_list_of_results(
        self,
        mapped_requests: list[Request],
        processing_kwargs: ProcessingKwargs,
    ) -> list[str]:
        from cads_adaptors.tools import area_selector, general, url_tools

        requests_urls = url_tools.requests_to_urls(
            mapped_requests,
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
        if "auth" in download_kwargs:
            username, password = download_kwargs["auth"]
            password = general.decrypt(token=password, ignore_errors=True)
            download_kwargs["auth"] = (username, password)

        paths = url_tools.try_download(urls, context=self.context, **download_kwargs)

        if (area := processing_kwargs["area"]) is not None:
            paths = area_selector.area_selector_paths(
                paths,
                area,
                self.context,
                **self.config.get("post_processing_kwargs", {}),
            )

        return paths
