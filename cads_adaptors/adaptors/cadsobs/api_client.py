from typing import Literal

from cads_adaptors import Context
from cads_adaptors.exceptions import CadsObsConnectionError

RequestMethod = Literal["GET", "POST"]


class CadsobsApiClient:
    """API Client for the observations repository HTTP API."""

    def __init__(self, baseurl: str, context: Context):
        import requests

        self.baseurl = baseurl
        self.context = context
        self.requests = requests

    def raise_user_visible_error(self, message: str, error_type=CadsObsConnectionError):
        # self.context.add_user_visible_error(message)
        raise error_type(message)

    def _send_request_and_capture_exceptions(
        self, method: RequestMethod, endpoint: str, payload: dict | None = None
    ):
        """Send a request and handle possible errors.

        Note that is raise_for_status will always raise a HTTPError. In this case,
        response will always be defined and we can get the information of the error from
        there. The server returns a human readable message and the traceback as separated
        keyswords in the "detail" field of the response. We raise the message and send
        the traceback to the stderr so it is visible in the worker log and captured
        by the monitoring system.
        """
        requests = self.requests
        try:
            response = self._send_request(endpoint, method, payload)
            response.raise_for_status()
        except (
            requests.ConnectionError,
            requests.ReadTimeout,
            requests.ConnectTimeout,
        ):
            self.raise_user_visible_error(
                "Can't connect to the observations API.",
                CadsObsConnectionError,
            )
        except requests.HTTPError:
            message, traceback = self._get_error_message(response)
            self.context.add_stderr(
                f"Observations API failed with the following message and traceback: {message} {traceback}"
            )
            self.raise_user_visible_error(
                f"Request to observations API failed: {message}",
                CadsObsConnectionError,
            )
        return response.json()

    def _send_request(self, endpoint: str, method: RequestMethod, payload: dict | None):
        with self.requests.Session() as session:
            response = session.request(
                method=method, url=f"{self.baseurl}/{endpoint}", json=payload
            )
        return response

    def _get_error_message(self, response) -> tuple[str, str]:
        """Get information on the error from the request response."""
        try:
            # This is not standard, we set it up like this in the server.
            detail = response.json()["detail"]
            message = detail["message"]
            traceback = detail["traceback"]
        except self.requests.JSONDecodeError:
            # When the exception is not handled well by the API server response.content
            # will not be JSON parseable. Then we can get the traceback like this.
            message = response.reason
            traceback = response.content.decode("UTF-8")
        return message, traceback

    def get_service_definition(self, dataset: str) -> dict:
        return self._send_request_and_capture_exceptions(
            "GET", f"{dataset}/service_definition"
        )

    def get_cdm_lite_variables(self) -> dict:
        return self._send_request_and_capture_exceptions("GET", "cdm/lite_variables")

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
        objects_to_retrieve = self._send_request_and_capture_exceptions(
            "POST", "get_object_urls_and_check_size", payload=payload
        )
        return objects_to_retrieve
