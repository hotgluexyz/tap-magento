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
        def raise_for_status(self):
            return None

        def json(self):
            return "magento-access-token"

    monkeypatch.setattr("tap_magento.auth.requests.post", lambda *args, **kwargs: Response())

    tap = TapMagento(config=config_path)

    assert TapMagento.fetch_access_token(tap) == {
        "access_token": "magento-access-token"
    }
    assert json.loads(config_path.read_text())["access_token"] == "magento-access-token"
