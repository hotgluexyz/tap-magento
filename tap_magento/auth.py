"""Authentication helpers for the Hotglue access-token flow."""

import json

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator


class MagentoAuthenticator(OAuthAuthenticator):
    """Fetch Magento admin tokens for the access-token flow."""

    _config_file: str

    def update_access_token_locally(self) -> None:
        if self._tap._config.get("username"):
            payload = {
                "username": self._tap._config["username"],
                "password": self._tap._config["password"],
            }
            try:
                response = requests.post(
                    f"{self._tap._config['store_url']}/index.php/rest/V1/integration/admin/token",
                    json=payload,
                )
                response.raise_for_status()
            except requests.RequestException:
                response = requests.post(
                    f"{self._tap._config['store_url']}/rest/V1/integration/admin/token",
                    json=payload,
                )
                response.raise_for_status()
            self._tap._config["access_token"] = response.json()

        with open(self._config_file, "w") as config_file:
            json.dump(self._tap._config, config_file, indent=4)
