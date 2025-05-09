import copy
import datetime
import itertools
import os
import shutil
from typing import Any

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import InvalidRequest


class EUMDACAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.eum_collection_id = self.config["eum_collection_id"]

        # schema should go here

        self.token = self.authenticate()

    def authenticate(self):
        import eumdac

        consumer_key = os.environ["EUMDAC_CONSUMER_KEY"]
        consumer_secret = os.environ["EUMDAC_CONSUMER_SECRET"]

        credentials = (consumer_key, consumer_secret)

        self.token = eumdac.AccessToken(credentials)

        self.context.debug(f"The active token is '{self.token}'. It expires {self.token.expiration}.")

        return self.token

    NON_EUMDAC_KEYS = ["__in_adaptor_no_cache"]
    DATE_INPUT_KEYS = ["dtstart", "dtend"]

    def cds_to_eumdac_preprocessing(self, request):
        eumdac_request = copy.deepcopy(request)

        # remove keys that are not supported by EUMDAC
        for non_eumdac_key in EUMDACAdaptor.NON_EUMDAC_KEYS:
            eumdac_request.pop(non_eumdac_key, None)

        eumdac_request["dtstart"] = eumdac_request["date"][0]
        eumdac_request["dtend"] = eumdac_request["date"][0]
        eumdac_request.pop('date', None)

        # convert date arguments to the expected type
        for date_input_key in EUMDACAdaptor.DATE_INPUT_KEYS:
            if date_input_key in eumdac_request:
                if isinstance(eumdac_request[date_input_key], str):
                    eumdac_request[date_input_key] = datetime.datetime.strptime(
                        eumdac_request[date_input_key], "%Y%m%d"
                    )
        return eumdac_request

    def has_token_expired(self):
        return self.token.expiration < datetime.datetime.now()

    def search(self, request):
        import eumdac

        if self.has_token_expired():
            self.authenticate()
        datastore = eumdac.DataStore(self.token)

        selected_collection = datastore.get_collection(self.eum_collection_id)

        products = selected_collection.search(**request)

        self.context.debug(
            f"{products.total_results} products found."
        )

        return products

    def download(self, request):
        products = self.search(request)

        downloaded_products = []
        for product in products:
            with product.open() as fsrc, open(fsrc.name, mode="wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)
                downloaded_products.append(fsrc.name)
                self.context.debug(f"Download of product {product} finished.")
        self.context.debug("All products have been downloaded.")

        return downloaded_products

    def get_result_size(self, request):
        products = self.search(request)

        number_of_products = products.total_results

        total_size_in_kb = 0
        for product in products:
            total_size_in_kb += product.size
        self.context.debug(
            f"The total size is {total_size_in_kb}KB (before any DS post-processing or packing) for {number_of_products} products."
        )

        return number_of_products, total_size_in_kb

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        return request

    def compute_result_size(self, request: dict[str, Any]):
        try:
            eumdac_request = self.cds_to_eumdac_preprocessing(request)
            result_size = self.get_result_size(eumdac_request)
        except Exception as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            #raise InvalidRequest(msg)
            return 0, 0

        return result_size

    def estimate_costs(self, request, **kwargs):
        costs= {}
        costs['number_of_fields'], costs["precise_size"] = self.compute_result_size(request)
        costs["size"] = costs["precise_size"]
        return costs

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        self.context.debug(f"Request is {request!r}")

        self.normalise_request(request)

        downloaded_products = []
        try:
            for subrequest in self.mapped_requests:
                eumdac_request = self.cds_to_eumdac_preprocessing(subrequest)
                downloaded_products_for_subrequest = self.download(eumdac_request)
                self.context.add_stdout(f"Calling EUMDAC for: {eumdac_request}")
                downloaded_products.extend(downloaded_products_for_subrequest)
                self.context.add_stdout(f"Downloaded products: {downloaded_products_for_subrequest}")
        except Exception as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return downloaded_products
