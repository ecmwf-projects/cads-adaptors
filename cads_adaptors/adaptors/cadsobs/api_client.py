from typing import Literal

import requests


class CadsobsApiClient:
    """API Client for the observations repository HTTP API."""

    def __init__(self, baseurl: str):
        self.baseurl = baseurl

    def _send_request(
        self, method: Literal["GET", "POST"], endpoint: str, payload: dict | None = None
    ):
        with requests.Session() as session:
            response = session.request(
                method=method, url=f"{self.baseurl}/{endpoint}", json=payload
            )
            response.raise_for_status()
        return response.json()

    def get_service_definition(self, dataset: str) -> dict:
        return self._send_request("GET", f"{dataset}/service_definition")

    def get_cdm_lite_variables(self):
        return self._send_request("GET", "cdm/lite_variables")

    def get_objects_to_retrieve(
        self, dataset_name: str, mapped_request: dict, size_limit: int
    ) -> list[str]:
        """
        Get the list of S3 objects that will be further read and filtered.

        Parameters
        ----------
        dataset_name: str
          Name of the dataset, for example insitu-observations-gnss
        mapped_request: dict
          Request parameters after being mapped by
        size_limit: int
          Size limit for the data request in bytes. Note that this is enforced based on
          an approximation. The size of each partition is multiplied by the percentage
          of "fields" (entries in the internal constraints) requested, and then added.
        """
        payload = dict(
            retrieve_args=dict(dataset=dataset_name, params=mapped_request),
            config=dict(size_limit=size_limit),
        )
        objects_to_retrieve = self._send_request(
            "POST", "get_object_urls_and_check_size", payload=payload
        )
        return objects_to_retrieve
