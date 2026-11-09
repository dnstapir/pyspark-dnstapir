from typing import Any

import httpx2


def _raise_for_status(response: httpx2.Response) -> None:
    try:
        response.raise_for_status()
    except httpx2.HTTPStatusError as e:
        raise TagManagerHTTPError(e) from e


class TagManager:
    """
    Client for interacting with the Tag Manager API.

    The exception behavior under "Raises" applies to all methods of this class that interact with the API
    and is therefore only documented here once. All raised exceptions derive from TagManagerError.

    Raises:
        TagManagerRequestError: Raised for errors occurring during the request to the Tag Manager API.
        TagManagerHTTPError: Raised for HTTP errors from the Tag Manager API.
        TagManagerResponseError: Raised when the response of the Tag Manager API is invalid or unexpected.
    """

    def __init__(self, base_url: str, api_key: str):
        self.client = httpx2.Client(base_url=base_url, headers={"X-API-Key": api_key})

    def _get(self, path: str) -> dict[str, Any]:
        try:
            response = self.client.get(path)
        except httpx2.ConnectError as e:
            raise TagManagerRequestError("Failed to connect to the Tag Manager API") from e
        except httpx2.ConnectTimeout as e:
            raise TagManagerRequestError("Connection to the Tag Manager API timed out") from e
        except httpx2.RequestError as e:
            raise TagManagerRequestError("Error issuing request to the Tag Manager API") from e

        _raise_for_status(response)
        try:
            obj = response.json()
        except Exception as e:
            raise TagManagerResponseError(f"Failed to parse JSON response of {path}: {e}") from e
        return obj

    def _get_key(self, path: str, key: str) -> Any:
        obj = self._get(path)

        if key not in obj:
            raise TagManagerResponseError(f"Response JSON of {path} does not contain key '{key}'")
        return obj[key]

    def _post(self, path: str, json: Any | None) -> None:
        try:
            response = self.client.post(path, json=json)
        except httpx2.ConnectError as e:
            raise TagManagerRequestError("Failed to connect to the Tag Manager API") from e
        except httpx2.ConnectTimeout as e:
            raise TagManagerRequestError("Connection to the Tag Manager API timed out") from e
        except httpx2.RequestError as e:
            raise TagManagerRequestError("Error issuing request to the Tag Manager API") from e
        _raise_for_status(response)

    def _get_tags(self, kind: str) -> list[str]:
        path = f"/{kind}/tags"
        return self._get_key(path, kind)

    def get_custom_tags(self) -> list[str]:
        """Returns a list of custom tags in use."""
        return self._get_tags("custom")

    def get_well_defined_tags(self) -> list[str]:
        """Returns a list of tags which are defined."""
        return self._get_tags("well-defined")

    def get_value(self, domain: str, tag: str) -> str:
        """Get the value of a tag on a domain."""
        path = f"/domains/{domain}/tags/{tag}"
        return self._get_key(path, "value")

    def get_tags_on_domain(self, domain: str) -> list[str]:
        """Returns a list of tags for a supplied domain."""
        path = f"/domains/{domain}/tags"
        return self._get_key(path, "tags")

    def get_domains_with_tag(self, tag: str) -> list[str]:
        """Returns a list of domains tagged with the supplied tag."""
        path = f"/tags/{tag}/domains"
        return self._get_key(path, "domains")

    def post_tag(self, domain: str, tag: str, value: str | None) -> None:
        """Post a tag for a domain with an optional value."""
        path = f"/domains/{domain}/tags/{tag}"
        self._post(path, json={"message": value})


class TagManagerError(Exception):
    """Base class for Tag Manager API errors"""


class TagManagerRequestError(TagManagerError):
    """Raised for request or connection errors to the Tag Manager API. See __cause__ for the inner exception."""


class TagManagerHTTPError(TagManagerError):
    """Raised for HTTP errors from the Tag Manager API"""

    def __init__(self, inner: httpx2.HTTPStatusError):
        self.inner = inner
        self.status_code = inner.response.status_code

        try:
            response_json = inner.response.json()
            if not isinstance(response_json, dict):
                response_json = {}
        except Exception:
            response_json = {}

        self.detail = response_json.get("detail", "")
        self.title = response_json.get("title", "")
        self.errors = response_json.get("errors")

        msg = f"HTTP error on {inner.request.url}: {self.status_code} {self.title} ({self.detail})"
        if self.errors is not None:
            msg += f"\nErrors: {self.errors}"
        super().__init__(msg)


class TagManagerResponseError(TagManagerError):
    """Raised when the response of the Tag Manager API is invalid or unexpected."""
