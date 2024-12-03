"""REST client handling, including MagentoStream base class."""

import backoff
import logging
import requests

from pathlib import Path
from typing import Any, Dict, Optional, Callable, Iterable

from datetime import datetime, timedelta
from simplejson.scanner import JSONDecodeError
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256
from requests_oauthlib import OAuth1
from urllib3.exceptions import ProtocolError, InvalidChunkLength
import time
from pendulum import parse
import copy


# logging.getLogger("backoff").setLevel(logging.CRITICAL)
def handle_backoff(details):
    if details["tries"]==1:
        time.sleep(30)

class MagentoStream(RESTStream):
    """Magento stream class."""

    access_token = None
    expires_in = None
    default_page_size = 300
    current_page = None
    max_pagination = 200
    max_date = None
    retries_500_status = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.config.get("custom_cookies"):
            for cookie, cookie_value in self.config.get("custom_cookies", {}).items():
                self._requests_session.cookies[cookie] = cookie_value
        self.new_start_date = None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store_url = self.config["store_url"]
        return f"{store_url}/rest"

    @property
    def page_size(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        page_size = (
            self.config.get("page_size")
            if self.config.get("page_size")
            else self.default_page_size
        )
        if isinstance(page_size, float) or isinstance(page_size, str):
            page_size = int(page_size)
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

    def get_auth1(self):
        return OAuth1(
                self.config.get('consumer_key'),
                client_secret=self.config.get('consumer_secret'),
                resource_owner_key=self.config.get("oauth_token", self.config.get("access_token")),
                resource_owner_secret=self.config.get("oauth_token_secret", self.config.get("access_token_secret")),
                signature_type="auth_header",
                signature_method=SIGNATURE_HMAC_SHA256
            )

    def prepare_request(self, context, next_page_token):
        request = super().prepare_request(context, next_page_token)

        if self.config.get("use_oauth"):
            request.auth =self.get_auth1()

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
        elif response.status_code in [404]:
            return 1
        elif response.status_code in [503]:
            return None
        else:
            # Some pages give 500 due to an internal error, need to retry at least 3 times and then skip the page
            if response.status_code == 500 and self.retries_500_status > 3:
                #reset the retries count and move to next page
                self.retries_500_status = 0
                return previous_token + 1
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
        # store at global level the current page to change start_date for big amounts of data
        self.current_page = next_page_token     
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
        
        # calculate start_date
        start_date = self.get_starting_timestamp(context)
        # When override date is set it is not picked up by get_starting_timestamp
        # manually pick up date from the config
        if self.config.get("start_date") and not start_date:
            start_date = parse(self.config.get("start_date"))


        params["searchCriteria[pageSize]"] = self.page_size
        if self.name == "source_items":
            params["searchCriteria[pageSize]"] = self.get_source_items_page_size()

        if not next_page_token:
            params["searchCriteria[currentPage]"] = 1
        else:
            # if there are too many pages, every 200 pages update the start_date and restart pagination to avoid memory issues (503)
            if next_page_token > self.max_pagination:
                next_page_token = 1
            params["searchCriteria[currentPage]"] = next_page_token

        if self.replication_key:
            # if we surpassed 200 pages update the start date to avoid memory issues
            # update start_date to latest date fetched minus 1 second to not lose any data
            # duplicated rows are cleaned
            if self.max_date:
                start_date = self.max_date
            params["searchCriteria[sortOrders][0][field]"] = self.replication_key
            params["searchCriteria[sortOrders][0][direction]"] = "ASC"

            if self.name == "orders":
                params["searchCriteria[sortOrders][1][field]"] = "increment_id"
                params["searchCriteria[sortOrders][1][direction]"] = "ASC"

            if start_date is not None:
                start_date = start_date.strftime("%Y-%m-%d %H:%M:%S")
                params[
                    "searchCriteria[filterGroups][0][filters][0][field]"
                ] = self.replication_key
                params["searchCriteria[filterGroups][0][filters][0][value]"] = self.new_start_date or start_date
                params[
                    "searchCriteria[filterGroups][0][filters][0][condition_type]"
                ] = "gt"
                
                # end date
                end_date = self.config.get("end_date")
                if end_date:
                    try:
                        end_date = parse(end_date).strftime("%Y-%m-%d %H:%M:%S")
                        params[
                            "searchCriteria[filterGroups][1][filters][0][field]"
                        ] = self.replication_key
                        params[
                            "searchCriteria[filterGroups][1][filters][0][value]"
                        ] = end_date
                        params[
                            "searchCriteria[filterGroups][1][filters][0][condition_type]"
                        ] = "lteq"
                    except:
                        self.logger.info(f"End date is not a valid datetime {end_date}, running sync without end_date")


        if (
            (context.get("store_id")
            and self.config.get("fetch_all_stores"))
            or self.config.get("store_id")
        ):
            # This is just a workaround, magento doesn't support store_code very well.
            # In 80% of the cases, this workaround should work, on some other cases it
            # will fail.
            # More info on: https://github.com/magento/magento2/issues/15461
            if self.config.get("fetch_all_stores") and context.get("store_id"):
                params[
                f"searchCriteria[filterGroups][2][filters][0][field]"
            ] = "store_id"
                params[
                    f"searchCriteria[filterGroups][2][filters][0][value]"
                ] = int(context.get("store_id"))

            elif self.config.get("store_id"):
                params[
                f"searchCriteria[filterGroups][2][filters][0][field]"
            ] = "store_id"
                params[
                    f"searchCriteria[filterGroups][2][filters][0][value]"
                ] = self.config.get("store_id")
        #Log params for debug and error tracking        
        self.logger.info(f"Sending, path: {self.path}, params: {params}")
        return params
    
    def get_start_date(self):
        current_start_date = parse(self.stream_state.get("progress_markers", dict()).get("replication_key_value") or self.stream_state.get("replication_key_value") or self.config.get("start_date"))
        cur_start_date_timestamp = current_start_date.timestamp()

        def make_request(start_date):
            url = self.get_url(None)
            headers = self.http_headers
            start_date = datetime.fromtimestamp(start_date).strftime("%Y-%m-%d %H:%M:%S")

            params = self.get_url_params(None, None)
            params["searchCriteria[filterGroups][0][filters][0][value]"] = start_date

            auth = None
            if self.authenticator:
                authenticator = self.authenticator
                headers.update(authenticator.auth_headers)
            if self.config.get("use_oauth"):
                auth = self.get_auth1()
            # Format the URL with the date and make the request
            res = requests.get(url, headers=headers, auth=auth, params=params)
            return res
        
        # Initialize the search range: start_date as lower bound, today as upper bound
        lower_bound = cur_start_date_timestamp
        upper_bound = datetime.utcnow().timestamp()

        # Binary search loop
        while lower_bound <= upper_bound:
            mid_date = int(lower_bound + round((upper_bound - lower_bound) / 2))
            res = make_request(mid_date)
            
            if res.status_code == 200:
                # Found a valid date, move the upper bound to search for an earlier valid date
                upper_bound = mid_date - 1
            elif res.status_code == 404:
                # If 404, adjust the search to later dates
                lower_bound = mid_date + 1
            else:
                raise Exception(f"Failed while calculating start_date. Unexpected status code: {res.text}")

        return datetime.fromtimestamp(lower_bound).strftime("%Y-%m-%d %H:%M:%S")


    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        #Reset 500 status code retries counter on successful response
        if response.status_code == 200 and self.retries_500_status > 0:
            self.retries_500_status = 0
            
        if self.config.get("crawl_delay"):
            delay = 0
            try:
                delay = int(self.config.get("crawl_delay"))
            except ValueError:
                pass
            if delay >0:
                time.sleep(delay)
        if response.status_code == 429:
            raise RetriableAPIError(f"Too Many Requests for path: {self.path}")

        if response.status_code in [404]:
            if self.replication_key and response.json().get("message") == "Het aangevraagde product bestaat niet. Controleer het product en probeer het opnieuw.":
                if not self.new_start_date:
                    self.logger.info("Response status code: {} with response {} - Calculating new start_date".format(response.status_code, response.text))
                    self.new_start_date = self.get_start_date()
                else:
                    # get the greates date, either fetched records latest rep_key or new_start_date
                    current_rep_key_value = parse(self.stream_state["progress_markers"]["replication_key_value"])
                    greatest_date = current_rep_key_value if current_rep_key_value > parse(self.new_start_date) else parse(self.new_start_date)
                    # add a day and iterate
                    greatest_date = greatest_date + timedelta(days=1)
                    self.new_start_date = greatest_date.strftime("%Y-%m-%d %H:%M:%S")
            pass
        elif response.status_code == 503:
            msg = f"This store is possibly going maintenance mode: {self.path}, {response.request.url}. Content {response.text}"
            self.logger.info(msg)
            raise RetriableAPIError(msg)
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
            if response.status_code == 500:
                if self.retries_500_status > 3:
                    #Skip this page after retrying more than 3 times
                    self.logger.info(f"Skipping path: {response.request.url} after 3 retries.")
                    return
                else:
                    self.retries_500_status = self.retries_500_status + 1     
            raise RetriableAPIError(msg)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        #Already skipping 404 and 503 in the parent.
        if response.status_code == 404 or response.status_code > 500:
            return []
        try:
            response_content = response.json()
            max_date = None
            if self.replication_key and self.current_page == self.max_pagination:
                # get max date
                dates = [parse(x[self.replication_key]) for x in super().parse_response(response)]
                sorted_dates = list(set(dates))
                sorted_dates = sorted(sorted_dates, reverse=True)
                if len(sorted_dates) < 2:
                    # If the max date already matches, increase the max pagination
                    if self.max_date == (sorted_dates[0] - timedelta(seconds=1)):
                        self.max_pagination = self.max_pagination + 1
                    else:
                        # If this entire page is the same date, the max_date should be set to the current max_date - 1 second
                        max_date = None
                        self.max_date = sorted_dates[0] - timedelta(seconds=1)
                else:
                    # filtering params use "gt" therefore use second greatest max_date to avoid losing data
                    max_date = sorted_dates[0]
                    prev_date = sorted_dates[1]
                    self.max_date = prev_date

            # TODO: I think we should get rid of below
            for item in super().parse_response(response):
                if self.replication_key and max_date:
                    # in the request previous to change date only fetch records up to the second latest date to avoid duplicates
                    if parse(item[self.replication_key]) >= max_date:
                        continue
                yield item
        except JSONDecodeError:
            raise Exception(f"Unable to decode response from {response.url} with content: {response.content}")

        yield from extract_jsonpath(self.records_jsonpath, input=response_content)

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (RetriableAPIError, requests.exceptions.ReadTimeout, ConnectionError, ConnectionResetError,ProtocolError,InvalidChunkLength,requests.RequestException,requests.exceptions.JSONDecodeError),
            max_tries=12,
            factor=5,
            on_backoff=handle_backoff
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
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            prepared_request = self.prepare_request(
                context, next_page_token=next_page_token
            )
            resp = decorated_request(prepared_request, context)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            # when iterating daily due to 404 there could be same next_page_token 1
            if next_page_token and next_page_token == previous_token and next_page_token != 1:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token