"""Authentication helpers for the Hotglue access-token flow."""

import json

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator


def get_admin_token(session, config, validate_response):
    payload = {
        "Content-Type": "application/json",
        "username": config.get("username"),
        "password": config.get("password"),
    }
    try:
        response = session.post(
            f"{config['store_url']}/index.php/rest/V1/integration/admin/token",
            json=payload,
        )
        response.json()
        validate_response(response)
    except:
        response = session.post(
            f"{config['store_url']}/rest/V1/integration/admin/token",
            json=payload,
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
            self._tap._config["access_token"] = get_admin_token(
                session,
                self._tap.config,
                lambda response: response.raise_for_status(),
            )

        with open(self._config_file, "w") as config_file:
            json.dump(self._tap._config, config_file, indent=4)
