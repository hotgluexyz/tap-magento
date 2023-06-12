"""REST client handling, including MagentoStream base class."""

import requests
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Callable, List, Iterable
import copy
from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.authenticators import BearerTokenAuthenticator
from datetime import datetime
import backoff
from http.client import RemoteDisconnected



logging.getLogger("backoff").setLevel(logging.CRITICAL)


class TooManyRequestsError(Exception):
    pass


class MagentoStream(RESTStream):
    """Magento stream class."""

    access_token = None
    expires_in = None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store_url = self.config["store_url"]
        return f"{store_url}/rest/V1"

    @property
    def page_size(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        page_size = (
            self.config.get("page_size")
            if self.config.get("page_size") != None
            else 300
        )
        return page_size

    records_jsonpath = "$.items[*]"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        if self.config.get("username") and self.config.get("password") is not None:
            token = self.get_token()
        else:
            token = self.config.get("access_token")
        return BearerTokenAuthenticator.create_for_stream(self, token=token)

    def get_token(self):
        now = round(datetime.utcnow().timestamp())
        if not self.access_token:
            s = requests.Session()
            payload = {
                "Content-Type": "application/json",
                "username": self.config.get("username"),
                "password": self.config.get("password"),
            }
            try:
                login = s.post(
                    f"{self.config['store_url']}/index.php/rest/V1/integration/admin/token",
                    json=payload,
                )
                login.json()
                login.raise_for_status()
            except:
                login = s.post(
                    f"{self.config['store_url']}/rest/V1/integration/admin/token",
                    json=payload,
                )
            login.raise_for_status()

            self.access_token = login.json()

        return self.access_token

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {
            "Content-Type": "application/json",
        }
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        next_page_token = None
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        elif response.status_code == 404:
            return None
        else:
            json_data = response.json()
            total_count = json_data.get("total_count", 0)
            if json_data.get("search_criteria"):
                current_page = json_data.get("search_criteria").get("current_page")
            else:
                current_page = 1
            if total_count > current_page * self.page_size:
                next_page_token = current_page + 1
        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params["searchCriteria[pageSize]"] = self.page_size
        if not next_page_token:
            params["searchCriteria[currentPage]"] = 1
        else:
            params["searchCriteria[currentPage]"] = next_page_token

        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date is not None:
                start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
                params["sort"] = "asc"
                params[
                    "searchCriteria[filterGroups][0][filters][0][field]"
                ] = self.replication_key
                params[
                    "searchCriteria[filterGroups][0][filters][0][value]"
                ] = start_date
                params[
                    "searchCriteria[filterGroups][0][filters][0][conditionType]"
                ] = "gt"
            params["order_by"] = self.replication_key
        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code == 404:
            pass
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            if response.status_code == 429:
                raise TooManyRequestsError("Too Many Requests")
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if response.status_code == 404:
            return []
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                ConnectionError,
                TooManyRequestsError,
            ),
            max_tries=8,
            factor=4,
        )(func)
        return decorator
    
    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""

        @backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                ConnectionError,
                TooManyRequestsError,
                requests.exceptions.RequestException,
                RemoteDisconnected,
            ),
            max_tries=8,
            factor=4,
        )
        def decorated_function(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                if isinstance(e, requests.exceptions.RequestException) and 'RemoteDisconnected' in str(e):
                    raise RemoteDisconnected("RemoteDisconnected: Remote end closed connection without response") from e
                else:
                    raise

        return decorated_function


    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.

        Raises:
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.
        """
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            try:
                resp = decorated_request(prepared_request, context)
            except RemoteDisconnected as e:
                raise RuntimeError("RemoteDisconnected: Remote end closed connection without response") from e

            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to the prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token
