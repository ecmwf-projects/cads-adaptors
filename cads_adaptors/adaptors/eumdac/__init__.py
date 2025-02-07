import copy
import datetime
import os
import shutil
from typing import Any, BinaryIO

from cads_adaptors.adaptors.cds import AbstractCdsAdaptor, Request
from cads_adaptors.exceptions import InvalidRequest


def authenticate(context):
    import eumdac

    consumer_key = os.environ["EUMDAC_CONSUMER_KEY"]
    consumer_secret = os.environ["EUMDAC_CONSUMER_SECRET"]

    credentials = (consumer_key, consumer_secret)

    token = eumdac.AccessToken(credentials)

    context.debug(f"This token '{token}' expires {token.expiration}")

    return token


DATE_INPUT_KEYS = ["dtstart", "dtend"]
NON_EUMDAC_KEYS = ["__in_adaptor_no_cache"]


def cds_to_eumdac_preprocessing(request):
    eumdac_request = copy.deepcopy(request)
    
    # remove keys that are not supported by EUMDAC
    for non_eumdac_key in NON_EUMDAC_KEYS:
        eumdac_request.pop(non_eumdac_key, None)
    
    # convert date arguments to the expected type
    for date_input_key in DATE_INPUT_KEYS:
        if isinstance(eumdac_request[date_input_key], str):
            eumdac_request[date_input_key] = datetime.datetime.strptime(
                eumdac_request[date_input_key], "%Y%m%d"
            )
    return eumdac_request


def download(token, collection_id, request, context):
    import eumdac

    datastore = eumdac.DataStore(token)

    selected_collection = datastore.get_collection(collection_id)

    products = selected_collection.search(**request)

    context.debug(
        f"Found Datasets: {products.total_results} datasets for the given time range"
    )

    downloaded_products = []
    for product in products:
        with product.open() as fsrc, open(fsrc.name, mode="wb") as fdst:
            shutil.copyfileobj(fsrc, fdst)
            downloaded_products.append(fsrc.name)
            context.debug(f"Download of product {product} finished.")
    context.debug("All downloads are finished.")

    return downloaded_products


class EUMDACAdaptor(AbstractCdsAdaptor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # schema should go here

    def pre_mapping_modifications(self, request: dict[str, Any]) -> dict[str, Any]:
        """Implemented in normalise_request, before the mapping is applied."""
        request = super().pre_mapping_modifications(request)

        default_download_format = "as_source"
        download_format = request.pop("download_format", default_download_format)
        self.set_download_format(
            download_format, default_download_format=default_download_format
        )

        return request

    def retrieve_list_of_results(self, request: dict[str, Any]) -> list[str]:
        self.context.debug(f"Request is {request!r}")

        # self.normalise_request(request)

        try:
            eumdac_request = cds_to_eumdac_preprocessing(request)
            eum_collection_id = self.config["eum_collection_id"]
            api_token = authenticate(self.context)
            downloaded_products = download(
                api_token, eum_collection_id, eumdac_request, self.context
            )
        except Exception as e:
            msg = e.args[0]
            self.context.add_user_visible_error(msg)
            raise InvalidRequest(msg)

        return downloaded_products
