"""Tests Magento access-token support."""

import json

from tap_magento.tap import TapMagento


def test_access_token_support_fetches_admin_token(tmp_path, monkeypatch):
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

    class Response:
        is_redirect = False

        def raise_for_status(self):
            return None

        def json(self):
            return "magento-access-token"

    post_requests = []

    def post(*args, **kwargs):
        post_requests.append((args, kwargs))
        return Response()

    monkeypatch.setattr("tap_magento.auth.requests.post", post)

    tap = TapMagento(config=config_path)

    assert TapMagento.fetch_access_token(tap) == {
        "access_token": "magento-access-token"
    }
    assert json.loads(config_path.read_text())["access_token"] == "magento-access-token"
    assert post_requests[0][1] == {
        "json": {"username": "admin", "password": "password"},
        "timeout": 300,
        "allow_redirects": False,
    }
