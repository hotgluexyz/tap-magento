"""Authentication helpers for the Hotglue access-token flow."""

import json

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from hotglue_singer_sdk.exceptions import FatalAPIError


def get_magento_url_base(store_url):
    store_url = store_url if store_url.startswith(("https://", "http://")) else f"https://{store_url}"
    return f"{store_url.rstrip('/')}/rest"


def get_admin_token(session, url_base, config, headers, validate_response):
    payload = {
        "username": config["username"],
        "password": config["password"],
    }
    try:
        response = session.post(
            f"{url_base.replace('/rest', '/index.php/rest')}/V1/integration/admin/token",
            json=payload,
            headers=headers,
            timeout=30,
        )
        response.json()
        response.raise_for_status()
        validate_response(response)
    except (FatalAPIError, ValueError, requests.RequestException):
        response = session.post(
            f"{url_base}/V1/integration/admin/token",
            json=payload,
            headers=headers,
            timeout=30,
        )
        validate_response(response)
    return response.json()


class MagentoAuthenticator(OAuthAuthenticator):
    """Fetch Magento admin tokens for the access-token flow."""

    def update_access_token_locally(self) -> None:
        if self._tap._config.get("username") and self._tap._config.get("password"):
            session = requests.Session()
            for cookie, value in self._tap.config.get("custom_cookies", {}).items():
                session.cookies[cookie] = value
            headers = {"Content-Type": "application/json"}
            if "user_agent" in self._tap.config:
                headers["User-Agent"] = self._tap.config["user_agent"]
            self._tap._config["access_token"] = get_admin_token(
                session,
                get_magento_url_base(self._tap.config["store_url"]),
                self._tap.config,
                headers,
                lambda response: response.raise_for_status(),
            )

        with open(self._config_file, "w") as config_file:
            json.dump(self._tap._config, config_file, indent=4)
