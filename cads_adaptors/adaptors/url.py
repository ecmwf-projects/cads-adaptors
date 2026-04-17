import eumdac
import os
import shutil
from typing import Any
import zipfile

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
        download_interface_type = self.config.get("download_interface_type", "multiurl")
        
        if download_interface_type == "multiurl":
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
        elif download_interface_type == "eumdac":
            # fetch credentials and collection id from config and environment variables
            consumer_key = os.environ["EUMDAC_CONSUMER_KEY"]
            consumer_secret = os.environ["EUMDAC_CONSUMER_SECRET"]
            eum_collection_id = self.config["eum_collection_id"]

            # connect to EUM data store and the collection, search for products and download them
            credentials = (consumer_key, consumer_secret)
            token = eumdac.AccessToken(credentials)
            datastore = eumdac.DataStore(token)
            selected_collection = datastore.get_collection(eum_collection_id)

            # search for products in the EUM data store collection
            titles = "{" + ",".join(urls) + "}"
            products = selected_collection.search(title=titles)

            self.context.add_stdout(f"Found Datasets: {products.total_results} datasets for the given time range")
            for i, product in enumerate(products):
                self.context.add_stdout(f"{i}, {str(product)}, {product.entries}")

            # download the matched products (which might come as zip archives)
            downloaded_products = []
            for product in products:
                with product.open() as fsrc, open(fsrc.name, mode='wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                    downloaded_products.append(fsrc.name)
                    self.context.add_stdout(f'Download of product {product} finished.')

            # extract the individual zip archives and keep only the netcdf parts
            paths = []
            for product in downloaded_products:
                self.context.debug(f"Extracting downloaded product: {product}...")
                if product.endswith(".zip"):
                    with zipfile.ZipFile(product, "r") as archive:
                        for file in archive.namelist():
                            if file.endswith(".nc"):
                                path = archive.extract(file)
                                paths.append(path)
                                self.context.debug(f" - {product}:{path}")
                elif product.endswith(".nc"):
                    paths.append(product)
                    self.context.debug(f" - {product}:{product}")

        if (area := processing_kwargs["area"]) is not None:
            paths = area_selector.area_selector_paths(
                paths,
                area,
                self.context,
                **self.config.get("post_processing_kwargs", {}),
            )

        return paths
