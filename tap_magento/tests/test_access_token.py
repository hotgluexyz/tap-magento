"""Tests Magento access-token support."""

import json

import pytest
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_magento.auth import get_admin_token
from tap_magento.tap import TapMagento


def test_access_token_support_fetches_admin_token(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "store_url": "store.example.com/",
                "username": "admin",
                "password": "password",
                "custom_cookies": {"session": "cookie"},
                "user_agent": "tap-magento/test",
            }
        )
    )

    class Response:
        is_redirect = False

        def raise_for_status(self):
            return None

        def json(self):
            return "magento-access-token"

    post_requests = []

    def post(*args, **kwargs):
        post_requests.append((args, kwargs))
        assert args[0].cookies["session"] == "cookie"
        return Response()

    monkeypatch.setattr("tap_magento.auth.requests.Session.post", post)

    tap = TapMagento(config=config_path)

    assert TapMagento.fetch_access_token(tap) == {
        "access_token": "magento-access-token"
    }
    assert json.loads(config_path.read_text())["access_token"] == "magento-access-token"
    assert post_requests[0][0][1:] == (
        "https://store.example.com/index.php/rest/V1/integration/admin/token",
    )
    assert post_requests[0][1] == {
        "json": {"username": "admin", "password": "password"},
        "headers": {"Content-Type": "application/json", "User-Agent": "tap-magento/test"},
        "timeout": 30,
        "allow_redirects": False,
    }


def test_access_token_support_retries_alternate_endpoint_after_html_response(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "store_url": "https://store.example.com",
                "username": "admin",
                "password": "password",
            }
        )
    )

    class HtmlResponse:
        is_redirect = False

        def json(self):
            raise ValueError

    class TokenResponse:
        is_redirect = False

        def raise_for_status(self):
            return None

        def json(self):
            return "magento-access-token"

    responses = iter([HtmlResponse(), TokenResponse()])
    post_requests = []

    def post(*args, **kwargs):
        post_requests.append((args, kwargs))
        return next(responses)

    monkeypatch.setattr("tap_magento.auth.requests.Session.post", post)

    tap = TapMagento(config=config_path)

    assert TapMagento.fetch_access_token(tap) == {
        "access_token": "magento-access-token"
    }
    assert post_requests[1][0][1:] == (
        "https://store.example.com/rest/V1/integration/admin/token",
    )
    assert post_requests[1][1]["timeout"] == 30
    assert post_requests[1][1]["allow_redirects"] is False


def test_get_admin_token_rejects_redirects():
    class RedirectResponse:
        is_redirect = True

    session = requests.Session()
    session.post = lambda *args, **kwargs: RedirectResponse()

    with pytest.raises(FatalAPIError):
        get_admin_token(
            session,
            "https://store.example.com/rest",
            {"username": "admin", "password": "password"},
            {},
            lambda response: response.raise_for_status(),
        )


def test_get_admin_token_retries_after_response_validation_failure():
    class Response:
        is_redirect = False

        def raise_for_status(self):
            return None

        def json(self):
            return "magento-access-token"

    session = requests.Session()
    requested_requests = []

    def post(url, **kwargs):
        requested_requests.append((url, kwargs))
        return Response()

    session.post = post
    validations = iter([FatalAPIError("blocked"), None])

    def validate_response(_response):
        result = next(validations)
        if result:
            raise result

    assert get_admin_token(
        session,
        "https://store.example.com/rest",
        {"username": "admin", "password": "password"},
        {},
        validate_response,
    ) == "magento-access-token"
    assert requested_requests == [
        (
            "https://store.example.com/index.php/rest/V1/integration/admin/token",
            {
                "json": {"username": "admin", "password": "password"},
                "headers": {},
                "timeout": 30,
                "allow_redirects": False,
            },
        ),
        (
            "https://store.example.com/rest/V1/integration/admin/token",
            {
                "json": {"username": "admin", "password": "password"},
                "headers": {},
                "timeout": 30,
                "allow_redirects": False,
            },
        ),
    ]


def test_access_token_support_uses_configured_token_without_password(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "store_url": "https://store.example.com",
                "username": "admin",
                "access_token": "configured-token",
            }
        )
    )

    monkeypatch.setattr(
        "tap_magento.auth.requests.Session.post",
        lambda *args, **kwargs: pytest.fail("should not request a token"),
    )

    assert TapMagento.fetch_access_token(TapMagento(config=config_path)) == {
        "access_token": "configured-token"
    }
