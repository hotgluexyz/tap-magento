"""Magento tap class."""

import json
import logging
from typing import Any, List

import requests

from hotglue_singer_sdk import Tap, Stream
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers

from tap_magento.streams import (
    OrdersStream,
    ProductsStream,
    ProductAttributesStream,
    ProductAttributeDetailsStream,
    ProductItemStocksStream,
    ProductStockStatusesStream,
    CategoryStream,
    SaleRulesStream,
    CouponsStream,
    InvoicesStream,
    StoreConfigsStream,
    StoreWebsitesStream,
    StoresStream,
    SourceItemsStream,
    CartsStream,
    CreditMemosStream,
    CustomersStream,
    ProductsRenderInfoStream,
    PricesStream
)


STREAM_TYPES = [
    # UsersStream,
    OrdersStream,
    ProductsStream,
    ProductAttributesStream,
    ProductAttributeDetailsStream,
    ProductItemStocksStream,
    ProductStockStatusesStream,
    CategoryStream,
    SaleRulesStream,
    CouponsStream,
    InvoicesStream,
    StoreConfigsStream,
    StoreWebsitesStream,
    StoresStream,
    SourceItemsStream,
    CartsStream,
    CreditMemosStream,
    CustomersStream,
    ProductsRenderInfoStream,
    PricesStream
]


class TapMagento(Tap):
    """Magento tap class."""

    name = "tap-magento"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            description="The token to authenticate against the API service",
        ),
        th.Property("username", th.StringType),
        th.Property("password", th.StringType),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync",
        ),
        th.Property(
            "store_url", th.StringType, required=True, description="The store url"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


SENSITIVE_KEYS = {
    "password",
    "access_token",
    "token",
    "secret",
    "authorization",
    "cookie",
    "consumer_key",
    "consumer_secret",
    "oauth_token",
    "oauth_token_secret",
}
SNAPSHOT_TEXT_LIMIT = 8000


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(s in lowered for s in SENSITIVE_KEYS)


def _redact_mapping(mapping: Any) -> dict:
    redacted: dict = {}
    if not mapping:
        return redacted

    for key, value in dict(mapping).items():
        key_str = str(key)
        if _is_sensitive_key(key_str):
            redacted[key_str] = "<REDACTED>"
        else:
            redacted[key_str] = value
    return redacted


def _response_snapshot(response: requests.Response) -> dict:
    request = getattr(response, "request", None)

    request_url = str(getattr(request, "url", "") or "")
    response_url = str(getattr(response, "url", "") or "")
    is_token_endpoint = "/integration/admin/token" in request_url or "/integration/admin/token" in response_url
    # Keep existing behavior for non-token requests.
    if is_token_endpoint:
        request_body_preview = "<REDACTED>"
        json_preview = "<REDACTED>"
        body_preview = "<REDACTED>"
    else:
        body_preview = (response.text or "")[:SNAPSHOT_TEXT_LIMIT]
        request_body_preview = str(getattr(request, "body", ""))[:SNAPSHOT_TEXT_LIMIT]

        try:
            json_preview = response.json()
        except Exception as err:
            json_preview = f"<json_parse_error:{type(err).__name__}>"

    return {
        "status_code": response.status_code,
        "reason": response.reason,
        "url": response.url,
        "request_method": getattr(request, "method", None),
        "request_url": getattr(request, "url", None),
        "request_headers": _redact_mapping(getattr(request, "headers", {})),
        "request_body_preview": request_body_preview,
        "response_headers": _redact_mapping(response.headers),
        "response_json_preview": json_preview,
        "response_body_preview": body_preview,
    }


def _iter_exception_chain(exc: BaseException):
    current = exc
    seen = set()
    while current and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _extract_response_snapshots(exc: BaseException) -> list[dict]:
    responses_by_id: dict[int, requests.Response] = {}

    for chained_exc in _iter_exception_chain(exc):
        response = getattr(chained_exc, "response", None)
        if isinstance(response, requests.Response):
            responses_by_id[id(response)] = response

        tb = chained_exc.__traceback__
        while tb:
            frame = tb.tb_frame
            for value in frame.f_locals.values():
                if isinstance(value, requests.Response):
                    responses_by_id[id(value)] = value
            tb = tb.tb_next

    return [_response_snapshot(response) for response in responses_by_id.values()]


def _log_response_details(exc: BaseException) -> None:
    snapshots = _extract_response_snapshots(exc)
    if snapshots:
        logging.error(
            "Captured HTTP response snapshot(s): %s",
            json.dumps(snapshots, default=str),
        )
    else:
        logging.error("No requests.Response object found in exception chain/traceback.")

if __name__ == "__main__":
    try:
        TapMagento.cli()
    except Exception as exc:
        try:
            _log_response_details(exc)
        except Exception:
            logging.exception("Failed to log response details")
        raise
