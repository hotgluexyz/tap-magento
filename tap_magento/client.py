"""REST client handling, including MagentoStream base class."""

import backoff
import logging
import requests
import copy
from pathlib import Path
from typing import Any, Dict, Optional, Callable, Iterable

from datetime import datetime, timedelta, timezone
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256
from requests_oauthlib import OAuth1
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem, Popularity


logging.getLogger("backoff").setLevel(logging.CRITICAL)


class MagentoStream(RESTStream):
    """Magento stream class."""

    access_token = None
    expires_in = None
    software_names = [SoftwareName.FIREFOX.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.MAC.value]
    popularity = [Popularity.POPULAR.value]
    user_agents = UserAgent(software_names=software_names, operating_systems=operating_systems, popularity = popularity, limit=100)

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store_url = self.config["store_url"]
        return f"{store_url}/rest/V1"
    
    @property
    def page_size(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        page_size = self.config.get("page_size") if self.config.get("page_size") != None else 300
        return page_size

    records_jsonpath = "$.items[*]"

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
        if not self.config.get("user_agent"):
            headers["User-Agent"] = self.user_agents.get_random_user_agent()
        else:
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
        
        if context.get("store_id"):
            filter_idx = 0
            if self.replication_key:
                filter_idx += 1
            
            # This is just a workaround, magento doesn't support store_code very well.
            # In 80% of the cases, this workaround should work, on some other cases it
            # will fail.
            # More info on: https://github.com/magento/magento2/issues/15461
            if self.config.get("fetch_all_stores"):
                params[
                f"searchCriteria[filterGroups][{filter_idx}][filters][{filter_idx}][field]"
            ] = "store_id"
                params[
                    f"searchCriteria[filterGroups][{filter_idx}][filters][{filter_idx}][value]"
                ] = int(context.get("store_id"))
                
            elif self.config.get("store_id"):
                params[
                f"searchCriteria[filterGroups][{filter_idx}][filters][{filter_idx}][field]"
            ] = "store_id"
                params[
                    f"searchCriteria[filterGroups][{filter_idx}][filters][{filter_idx}][value]"
                ] = self.config.get("store_id")

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
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

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
        #Skip product_stock_statuses if use_stock_statuses is set to false
        if self.name == "product_stock_statuses":
            use_stock_statuses = self.config.get("use_stock_statuses",True)
            if not use_stock_statuses:
                return []
        super()._sync_records(context=context)

    @property
    def selected_fields(self):
        selected_fields = [
            b[1] for b, v 
            in self._tap_input_catalog.get(self.name).metadata.items() 
            if len(b) > 1 and b[0] == 'properties' and v.selected]
        return selected_fields


class ChunkedQueryMagentoStream(MagentoStream):
    """Chunked query Magento stream."""


    request_segment_size = 24 # Hours
    @property
    def replication_key(self):
        raise NotImplementedError("Replication key is required for chunked query streams")
    


    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        filter_keys = [key for key in params.keys() if key.startswith("searchCriteria[filterGroups]") and key.endswith("[field]")]
        existing_filter_count = len(filter_keys)
        start_date = context.get("chunk_start_date")
        end_date = context.get("chunk_end_date")

        start_date_filter_index = 0
        end_date_filter_index = existing_filter_count

        params[f"searchCriteria[filterGroups][{start_date_filter_index}][filters][0][field]"] = self.replication_key
        params[f"searchCriteria[filterGroups][{start_date_filter_index}][filters][0][value]"] = start_date
        params[f"searchCriteria[filterGroups][{start_date_filter_index}][filters][0][condition_type]"] = "gt"
        
        params[f"searchCriteria[filterGroups][{end_date_filter_index}][filters][{end_date_filter_index}][field]"] = self.replication_key
        params[f"searchCriteria[filterGroups][{end_date_filter_index}][filters][{end_date_filter_index}][value]"] = end_date
        params[f"searchCriteria[filterGroups][{end_date_filter_index}][filters][{end_date_filter_index}][condition_type]"] = "lteq"
        
        params.pop("order_by")
        params["order_by"] = self.replication_key

        params["fields"] = f"items[{','.join(self.selected_fields)}]"

        return params
    
    def get_ending_timestamp(self):
        if self.config.get("end_date"):
            return self.config.get("end_date")
        else:
            now = datetime.now(timezone.utc)
            return now

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        ultimate_start_date = self.get_starting_timestamp(context)
        ultimate_end_date = self.get_ending_timestamp()
        

        start_date = ultimate_start_date
        while start_date < ultimate_end_date:
            end_date = start_date + timedelta(hours=self.request_segment_size)
            if end_date > ultimate_end_date:
                end_date = ultimate_end_date

            next_page_token: Any = None
            chunk_finished = False
            decorated_request = self.request_decorator(self._request)
            context["chunk_start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S")
            context["chunk_end_date"] = end_date.strftime("%Y-%m-%d %H:%M:%S")
            while not chunk_finished:
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)
                yield from self.parse_response(resp)
                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                # Cycle until get_next_page_token() no longer returns a value
                chunk_finished = not next_page_token

            start_date = end_date
