"""REST client handling, including MagentoStream base class."""

import backoff
import logging
import requests

from pathlib import Path
from typing import Any, Dict, Optional, Callable, Iterable

from datetime import datetime
from simplejson.scanner import JSONDecodeError
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256
from requests_oauthlib import OAuth1


logging.getLogger("backoff").setLevel(logging.CRITICAL)


class MagentoStream(RESTStream):
    """Magento stream class."""

    access_token = None
    expires_in = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.config.get("custom_cookies"):
            for cookie, cookie_value in self.config.get("custom_cookies", {}).items():
                self._requests_session.cookies[cookie] = cookie_value


    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store_url = self.config["store_url"]
        return f"{store_url}/rest"

    @property
    def page_size(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        page_size = self.config.get("page_size") if self.config.get("page_size") != None else 300
        return page_size

    records_jsonpath = "$.items[*]"

    def get_token(self):
        now = round(datetime.utcnow().timestamp())
        if not self.access_token:
            s = self.requests_session
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
                # login.raise_for_status()
                self.validate_response(login)
            except:
                login = s.post(
                    f"{self.config['store_url']}/rest/V1/integration/admin/token",
                    json=payload,
                )
            # login.raise_for_status()
            self.validate_response(login)

            self.access_token = login.json()

        return self.access_token

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        if self.config.get("username") and self.config.get("password") is not None:
            token = self.get_token()
        else:
            token = self.config.get("oauth_token", self.config.get("access_token"))
        return BearerTokenAuthenticator.create_for_stream(self, token=token)


    def prepare_request(self, context, next_page_token):
        request = super().prepare_request(context, next_page_token)

        if self.config.get("use_oauth"):
            request.auth = OAuth1(
                self.config.get('consumer_key'),
                client_secret=self.config.get('consumer_secret'),
                resource_owner_key=self.config.get("oauth_token", self.config.get("access_token")),
                resource_owner_secret=self.config.get("oauth_token_secret", self.config.get("access_token_secret")),
                signature_type="auth_header",
                signature_method=SIGNATURE_HMAC_SHA256
            )

        return request


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
        elif response.status_code in [404, 503]:
            return None
        else:
            json_data = response.json()
            total_count = json_data.get("total_count", 0)
            if json_data.get("search_criteria"):
                current_page = json_data.get("search_criteria").get("current_page")
            else:
                current_page = 1
            page_size = self.page_size
            if self.name=="source_items":
                page_size = self.get_source_items_page_size()

            if total_count > current_page * page_size:
                next_page_token = current_page + 1
        return next_page_token
    def get_source_items_page_size(self):
        return self.config.get("source_items_page_size",2000)
    def get_url_params(
        self, context, next_page_token
    ):
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        if context is None:
            context = {}

        params["searchCriteria[pageSize]"] = self.page_size
        if self.name == "source_items":
            params["searchCriteria[pageSize]"] = self.get_source_items_page_size()

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
                    "searchCriteria[filterGroups][0][filters][0][condition_type]"
                ] = "gt"
            params["order_by"] = self.replication_key

        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code == 429:
            raise RetriableAPIError(f"Too Many Requests for path: {self.path}")

        if response.status_code in [404, 503]:
            self.logger.info("Response status code: {} - Endpoint skipped".format(response.status_code))
            if response.status_code == 503:
                self.logger.info(f"This store is possibly going maintenance mode: {self.path}")
            pass
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
                f" with text:{response.text} "
            )
            raise FatalAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
                f" with text:{response.text} "
            )
            raise RetriableAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if response.status_code == 404 or response.status_code > 500:
            return []

        try:
            response_content = response.json()
        except JSONDecodeError:
            raise Exception(f"Unable to decode response from {response.url} with content: {response.content}")

        yield from extract_jsonpath(self.records_jsonpath, input=response_content)

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, ConnectionError),
            max_tries=8,
            factor=2,
        )(func)
        return decorator

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        if self.name == "source_items":
            use_inventory_source_items = self.config.get("use_inventory_source_items",True)
            if not use_inventory_source_items:
                return []
        #Skip product_item_stocks if use_item_stock is set to false
        if self.name == "product_item_stocks":
            use_item_stock = self.config.get("use_item_stock",True)
            if not use_item_stock:
                return []
        super()._sync_records(context=context)