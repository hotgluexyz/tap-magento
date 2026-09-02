"""Unit tests for product_prices store-scoped error handling."""

from unittest.mock import MagicMock

import backoff
import pytest
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_magento.streams import PricesStream


def _make_stream(**attrs) -> PricesStream:
    stream = PricesStream.__new__(PricesStream)
    stream._active_store_id = attrs.get("store_id", "12")
    stream._active_store_code = attrs.get("store_code", "fr")
    stream.logger = MagicMock()
    stream.name = "product_prices"
    stream.path = ""
    stream.current_visibility = attrs.get("visibility", 2)
    stream._config = {}
    stream._http_headers = {}
    stream.retries_500_status = 0
    stream.error_message = None
    stream.replication_key = None
    stream.allowed_error_messages = []
    stream.current_page = None
    stream.chunk_by_date = False
    stream.extra_retry_statuses = []
    return stream


def _response(status_code=200, text="{}", url="https://example.com/graphql", store="fr"):
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")
    response.url = url
    response.encoding = "utf-8"
    response.reason = "OK" if status_code == 200 else "Error"
    request = requests.Request("POST", url, headers={"store": store}).prepare()
    response.request = request
    return response


class TestActiveStoreLabel:
    def test_uses_tracked_store(self):
        stream = _make_stream(store_id="7", store_code="be")
        assert stream._active_store_label() == "store_id='7' store_code='be'"

    def test_falls_back_to_response_header(self):
        stream = _make_stream(store_id=None, store_code=None)
        response = _response(store="nl")
        assert "store_code='nl'" in stream._active_store_label(response)


class TestGiveUpHandler:
    def test_logs_and_raises_fatal_with_store(self):
        stream = _make_stream(store_id="12", store_code="fr")
        underlying = RetriableAPIError("Transient GraphQL payload missing page_info")

        try:
            raise underlying
        except RetriableAPIError:
            with pytest.raises(FatalAPIError, match="store_id='12'.*store_code='fr'") as exc_info:
                stream._give_up_product_prices({"tries": 12})

        msg = str(exc_info.value)
        assert "product_prices failed after 12 retries" in msg
        assert "Transient GraphQL payload missing page_info" in msg
        stream.logger.error.assert_called_once()
        assert stream.logger.error.call_args[0][0] == msg
        assert exc_info.value.__cause__ is underlying


class TestValidateResponse:
    def test_missing_page_info_includes_store_and_preview(self):
        stream = _make_stream()
        response = _response(text='{"data":{"products":null}}')

        with pytest.raises(RetriableAPIError) as exc_info:
            stream.validate_response(response)

        msg = str(exc_info.value)
        assert "missing page_info" in msg
        assert "store_id='12'" in msg
        assert "store_code='fr'" in msg
        assert "Response preview:" in msg

    def test_bare_brace_json_string_is_informative(self):
        """JSON body `\"}\"` parses to the string `}` — previously a useless error."""
        stream = _make_stream()
        response = _response(text='"}"')

        with pytest.raises(RetriableAPIError) as exc_info:
            stream.validate_response(response)

        msg = str(exc_info.value)
        assert "Unexpected non-object GraphQL JSON" in msg
        assert "store_id='12'" in msg
        assert "store_code='fr'" in msg
        assert "'}'" in msg or '"}"' in msg

    def test_invalid_json_is_wrapped_with_store(self):
        """Parent JSON check fires first; we still attach store context."""
        stream = _make_stream()
        response = _response(text="}")

        with pytest.raises(RetriableAPIError) as exc_info:
            stream.validate_response(response)

        msg = str(exc_info.value)
        assert "product_prices request failed for store_id='12' store_code='fr'" in msg
        assert "Invalid JSON" in msg
        assert "Response preview: '}'" in msg

    def test_valid_page_info_passes(self):
        stream = _make_stream()
        response = _response(
            text='{"data":{"products":{"page_info":{"current_page":1,"total_pages":2},"items":[]}}}'
        )
        stream.validate_response(response)

    def test_http_error_is_wrapped_with_store(self):
        stream = _make_stream()
        response = _response(status_code=400, text='{"message":"bad"}')

        with pytest.raises(FatalAPIError) as exc_info:
            stream.validate_response(response)

        msg = str(exc_info.value)
        assert "product_prices request failed for store_id='12' store_code='fr'" in msg
        assert "Response preview:" in msg


class TestGetNextPageToken:
    def test_returns_next_page(self):
        stream = _make_stream()
        response = _response(
            text='{"data":{"products":{"page_info":{"current_page":1,"total_pages":3}}}}'
        )
        assert stream.get_next_page_token(response, None) == 2

    def test_malformed_payload_raises_fatal_with_store(self):
        stream = _make_stream()
        response = _response(text="}")

        with pytest.raises(FatalAPIError) as exc_info:
            stream.get_next_page_token(response, None)

        msg = str(exc_info.value)
        assert "product_prices failed while reading GraphQL pagination" in msg
        assert "store_id='12'" in msg
        assert "store_code='fr'" in msg
        assert "Response preview: '}'" in msg
        stream.logger.error.assert_called_once()

    def test_visibility_3_skips_pagination(self):
        stream = _make_stream(visibility=3)
        assert stream.get_next_page_token(_response(text="}"), None) is None


class TestRequestDecoratorGiveUp:
    def test_exhausted_retries_raise_fatal_with_store(self):
        stream = _make_stream(store_id="99", store_code="de")

        decorated = backoff.on_exception(
            backoff.constant,
            RetriableAPIError,
            max_tries=3,
            interval=0,
            on_giveup=stream._give_up_product_prices,
        )(lambda: (_ for _ in ()).throw(RetriableAPIError("boom")))

        with pytest.raises(FatalAPIError) as exc_info:
            decorated()

        msg = str(exc_info.value)
        assert "product_prices failed after 3 retries" in msg
        assert "store_id='99'" in msg
        assert "store_code='de'" in msg
        assert "boom" in msg
        assert isinstance(exc_info.value.__cause__, RetriableAPIError)
        stream.logger.error.assert_called_once()

    def test_production_request_decorator_giveup_uses_store(self):
        """Exercise PricesStream.request_decorator with tiny retry budget."""
        stream = _make_stream(store_id="5", store_code="es")

        # Patch production decorator knobs so the test stays fast.
        original = stream.request_decorator

        def fast_decorator(func):
            return backoff.on_exception(
                backoff.constant,
                RetriableAPIError,
                max_tries=2,
                interval=0,
                on_giveup=stream._give_up_product_prices,
            )(func)

        stream.request_decorator = fast_decorator
        failing = stream.request_decorator(lambda: (_ for _ in ()).throw(RetriableAPIError("page_info missing")))

        with pytest.raises(FatalAPIError, match="store_id='5'.*store_code='es'.*page_info missing"):
            failing()

        stream.request_decorator = original


class TestHeadersTrackStore:
    def test_headers_set_active_store(self):
        stream = _make_stream(store_id=None, store_code=None)
        stream._http_headers = {"Authorization": "Bearer x"}

        headers = stream.get_additional_headers_with_context(
            {"store_id": "3", "store_code": "it"}
        )

        assert headers["store"] == "it"
        assert stream._active_store_id == "3"
        assert stream._active_store_code == "it"
        assert stream._active_store_label() == "store_id='3' store_code='it'"
