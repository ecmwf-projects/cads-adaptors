import copy
import datetime
import os
import shutil
import zipfile
from enum import Enum
from typing import Any

import eumdac
import requests

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, ProcessingKwargs, Request
from cads_adaptors.exceptions import CdsConfigError, InvalidRequest
from cads_adaptors.tools.general import ensure_list


class TimeOfDay(Enum):
    START_OF_DAY = "00:00:00"
    FIRST_SECOND_OF_DAY = "00:00:01"
    NOON = "12:00:00"
    END_OF_DAY = "23:59:59"


class EUMDACCostingApproach(Enum):
    CDS = "cds"
    EUM = "eum"


def str_to_date(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")


class EUMDACAdaptor(AbstractCdsAdaptor):
    ALLOWED_NON_EUMDAC_KEYS = ["date"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.eum_collection_id = self.config["eum_collection_id"]

    def pre_mapping_modifications(
        self, request: dict[str, Any]
    ) -> tuple[Request, ProcessingKwargs]:
        """Implemented in normalise_request, before the mapping is applied."""
        request, kwargs = super().pre_mapping_modifications(request)

        download_format = request.pop("download_format", kwargs["download_format"])
        kwargs["download_format"] = self.get_download_format(download_format)

        kwargs["area"] = request.pop("area", None)

        return request, kwargs

    def merge_into_maximal_date_intervals(self, request):
        dates = copy.deepcopy(request["date"])
        request["date"] = []

        # adjust the dates, so that we do not get more (or less) data than wanted
        time_of_day_to_search_for = TimeOfDay[
            self.config.get("time_of_day_to_search_for", TimeOfDay.NOON.name)
        ]
        dates = [date + "T" + time_of_day_to_search_for.value for date in dates]

        dates = sorted(ensure_list(dates))
        current_interval = (dates[0], dates[0])
        previous_date = dates[0]
        ONE_DAY = datetime.timedelta(days=1)
        for date in dates[1:]:
            if str_to_date(date) != str_to_date(previous_date) + ONE_DAY:
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
        for key in list(eumdac_request.keys()):
            if (
                key not in self.eumdac_keys
                and key not in EUMDACAdaptor.ALLOWED_NON_EUMDAC_KEYS
            ):
                eumdac_request.pop(key, None)

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

    def authenticate(self):
        consumer_key = os.environ["EUMDAC_CONSUMER_KEY"]
        consumer_secret = os.environ["EUMDAC_CONSUMER_SECRET"]

        credentials = (consumer_key, consumer_secret)

        self.token = eumdac.AccessToken(credentials)

        self.context.debug(
            f"The active token is '{self.token}'. It expires {self.token.expiration}."
        )

        return self.token

    def has_token_expired(self):
        return self.token.expiration < datetime.datetime.now()

    def connect_to_collection(self):
        if not hasattr(self, "token") or self.has_token_expired():
            try:
                self.authenticate()
                self.datastore = eumdac.DataStore(self.token)
                self.selected_collection = self.datastore.get_collection(
                    self.eum_collection_id
                )
            except (
                requests.exceptions.ConnectionError,
                eumdac.datastore.DataStoreError,
                eumdac.collection.CollectionError,
            ) as e:
                msg = f"The connection to EUMETSAT data store and/or the collection failed: {e!r}"
                self.context.add_user_visible_error(msg)
                raise RuntimeError(msg)
            except Exception as e:
                msg = f"An unexpected error occurred: {e!r}"
                self.context.add_user_visible_error(msg)
                raise RuntimeError(msg)
            if not hasattr(self, "eumdac_keys"):
                self.eumdac_keys = self.selected_collection.search_options

    def search(self, request):
        products = self.selected_collection.search(**request)

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

    def compute_result_size(self, request: dict[str, Any]):
        self.connect_to_collection()

        number_of_products, total_size_in_kb = 0, 0
        try:
            cachingArgs = self.get_caching_args(request)
            for subrequest in cachingArgs.mapped_requests:
                eumdac_request = self.cds_to_eumdac_preprocessing(subrequest)
                subrequest_result_size = self.get_result_size(eumdac_request)
                number_of_products += subrequest_result_size[0]
                total_size_in_kb += subrequest_result_size[1]
        except (
            requests.exceptions.ConnectionError,
            eumdac.collection.CollectionError,
        ) as e:
            msg = (
                f"The connection to EUMETSAT data store and/or collection failed: {e!r}"
            )
            self.context.add_user_visible_error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"The request is likely invalid: {e!r}"
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return number_of_products, total_size_in_kb

    def estimate_costs(self, request, **kwargs):
        approach = (
            self.config.get("costing", dict())
            .get("costing_kwargs", dict())
            .get("approach", "cds")
        )
        if approach == EUMDACCostingApproach.CDS.value:
            costs = super().estimate_costs(request, **kwargs)
        elif approach == EUMDACCostingApproach.EUM.value:
            costs = {}
            costs["number_of_fields"], costs["precise_size"] = self.compute_result_size(
                request
            )
            costs["size"] = costs["number_of_fields"]
        else:
            raise CdsConfigError(f"Invalid cost estimation approach: {approach}.")
        return costs

    def retrieve_list_of_results(
        self, mapped_requests: list[Request], processing_kwargs: ProcessingKwargs
    ) -> list[str]:
        from cads_adaptors.tools import area_selector

        self.connect_to_collection()

        downloaded_products = []
        try:
            for subrequest in mapped_requests:
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
        except (requests.exceptions.ConnectionError, eumdac.product.ProductError) as e:
            msg = f"Downloading a product from the EUMETSAT data store failed: {e!r}"
            self.context.add_user_visible_error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"The request is likely invalid: {e!r}"
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        # extract the individual files zip archives, keep only the netcdf parts
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
        self.context.debug(f"After area selection, these are the files: {paths}")

        return paths
