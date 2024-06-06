from typing import Literal

import requests


class CadsobsApiClient:
    """TODO: Inplement auth."""

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
        self, dataset_name: str, mapped_request: dict
    ) -> list[str]:
        payload = dict(
            retrieve_args=dict(dataset=dataset_name, params=mapped_request),
            config=dict(size_limit=100000),
        )
        objects_to_retrieve = self._send_request(
            "POST", "get_object_urls_and_check_size", payload=payload
        )
        return objects_to_retrieve
