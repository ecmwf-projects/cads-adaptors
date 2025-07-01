import copy
import datetime
import os
import shutil
from typing import Any

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor
from cads_adaptors.exceptions import InvalidRequest
from cads_adaptors.tools.general import ensure_list


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

        self.context.debug(
            f"The active token is '{self.token}'. It expires {self.token.expiration}."
        )

        return self.token

    NON_EUMDAC_KEYS = ["__in_adaptor_no_cache"]

    def str_to_date(self, date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d")

    def merge_into_maximal_date_intervals(self, request):
        dates = copy.deepcopy(request["date"])
        request["date"] = []
        dates = sorted(ensure_list(dates))
        current_interval = (dates[0], dates[0])
        previous_date = dates[0]
        ONE_DAY = datetime.timedelta(days=1)
        for date in dates[1:]:
            if self.str_to_date(date) != self.str_to_date(previous_date) + ONE_DAY:
                current_interval = (current_interval[0], previous_date)
                request["date"].append(current_interval)
                current_interval = (date, date)
            previous_date = date
        current_interval = (current_interval[0], dates[-1])
        request["date"].append(current_interval)
        return request

    def cds_to_eumdac_preprocessing(self, request):
        eumdac_request = copy.deepcopy(request)

        # remove keys that are not supported by EUMDAC
        for non_eumdac_key in EUMDACAdaptor.NON_EUMDAC_KEYS:
            eumdac_request.pop(non_eumdac_key, None)

        eumdac_request = self.merge_into_maximal_date_intervals(eumdac_request)

        # convert lists to EUMDAC {,}-format
        for eumdac_key in eumdac_request:
            if eumdac_key != "date":
                if isinstance(eumdac_request[eumdac_key], list):
                    if eumdac_key == "bbox":
                        if len(eumdac_request[eumdac_key]) == 4:
                            # the EUMDAC bounding box orders limits as (W, S, E, N)
                            # while the CDS API orders them as (N, W, S E)
                            # also, the resulting string is NOT surrounded by {}
                            eumdac_request[eumdac_key] = (
                                eumdac_request[eumdac_key][1:]
                                + eumdac_request[eumdac_key][:1]
                            )
                            eumdac_request[eumdac_key] = ", ".join(
                                [str(x) for x in eumdac_request[eumdac_key]]
                            )
                        else:
                            self.context.add_user_visible_error(
                                f"Invalid bounding box: {eumdac_request[eumdac_key]}. "
                                "It should contain exactly 4 values."
                            )
                            raise InvalidRequest(
                                f"Invalid bounding box: {eumdac_request[eumdac_key]}"
                            )
                    elif len(eumdac_request[eumdac_key]) > 1:
                        eumdac_request[eumdac_key] = (
                            "{"
                            + ",".join([str(x) for x in eumdac_request[eumdac_key]])
                            + "}"
                        )
                    else:
                        eumdac_request[eumdac_key] = str(eumdac_request[eumdac_key][0])

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

        self.context.debug(f"{products.total_results} products found.")

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

    def get_result_size(self, eumdac_request):
        request = copy.deepcopy(eumdac_request)

        total_size_in_kb = 0
        number_of_products = 0

        date_intervals = request.pop("date", None)
        for date_interval in date_intervals:
            request["dtstart"], request["dtend"] = date_interval
            products = self.search(request)

            number_of_products += products.total_results

            for product in products:
                total_size_in_kb += product.size

        self.context.debug(
            f"The total size is {total_size_in_kb}KB (before any DS post-processing or packing) "
            f"for {number_of_products} products."
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
        number_of_products, total_size_in_kb = 0, 0
        try:
            self.normalise_request(request)
            for subrequest in self.mapped_requests:
                eumdac_request = self.cds_to_eumdac_preprocessing(subrequest)
                subrequest_result_size = self.get_result_size(eumdac_request)
                number_of_products += subrequest_result_size[0]
                total_size_in_kb += subrequest_result_size[1]
        except Exception as e:
            self.context.debug(f"Exception: {e!r}")
            # raise InvalidRequest(msg)
            return 0, 0

        return number_of_products, total_size_in_kb

    def estimate_costs(self, request, **kwargs):
        # the EUM approach
        # costs = {}
        # costs["number_of_fields"], costs["precise_size"] = self.compute_result_size(
        #     request
        # )
        # costs["size"] = costs["number_of_fields"]

        # for now, we rely on the general DS cost estimation procedure
        # and do not provide any specific costs from EUM
        costs = super().estimate_costs(request, **kwargs)
        return costs

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        self.context.debug(f"Request is {request!r}")

        self.normalise_request(request)

        downloaded_products = []
        try:
            for subrequest in self.mapped_requests:
                eumdac_request = self.cds_to_eumdac_preprocessing(subrequest)
                date_intervals = eumdac_request.pop("date", None)
                for date_interval in date_intervals:
                    eumdac_request["dtstart"], eumdac_request["dtend"] = date_interval
                    self.context.debug(f"Calling EUMDAC for: {eumdac_request}")
                    downloaded_products_for_subrequest = self.download(eumdac_request)
                    downloaded_products.extend(downloaded_products_for_subrequest)
                    self.context.debug(
                        f"Downloaded products: {downloaded_products_for_subrequest}"
                    )
        except Exception as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return downloaded_products
