from typing import Any

from cads_adaptors.adaptors import cds


class UrlCdsAdaptor(cds.AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.area: list[Any] | None = None
        # URL adaptor should default to intersecting constraints
        self.intersect_constraints_bool: bool = self.config.get(
            "intersect_constraints", True
        )

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        request = super().pre_mapping_modifications(request)
        default_download_format = "zip"

        # TODO: Remove legacy syntax all together
        download_format = request.pop("format", default_download_format)
        download_format = request.pop("download_format", download_format)
        self.set_download_format(download_format)

        self.area = request.pop("area", None)

        return request

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        from cads_adaptors.tools import area_selector, general, url_tools

        request = self.normalise_request(request)
        assert self.mapped_requests is not None  # Type-setting

        requests_urls = url_tools.requests_to_urls(
            self.mapped_requests,
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
            password = general.decrypt(
                token=password,
                key_name="URL_ADAPTOR_DECRYPTION_KEY",  # TODO: name of the env variable. To be set by ECMWF
                raises=False,
            )
            download_kwargs["auth"] = (username, password)

        paths = url_tools.try_download(urls, context=self.context, **download_kwargs)

        if self.area is not None:
            paths = area_selector.area_selector_paths(
                paths,
                self.area,
                self.context,
                **self.config.get("post_processing_kwargs", {}),
            )

        return paths
