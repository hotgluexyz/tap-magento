"""Authentication helpers for the Hotglue access-token flow."""

import json

import requests
from hotglue_singer_sdk.authenticators import OAuthAuthenticator


class MagentoAuthenticator(OAuthAuthenticator):
    """Fetch Magento admin tokens for the access-token flow."""

    _config_file: str

    @staticmethod
    def _request_token(url, payload):
        response = requests.post(
            url,
            json=payload,
            timeout=300,
            allow_redirects=False,
        )
        if response.is_redirect:
            raise RuntimeError("Magento token endpoint returned a redirect.")
        response.raise_for_status()
        return response

    def update_access_token_locally(self) -> None:
        if self._tap._config.get("username"):
            payload = {
                "username": self._tap._config["username"],
                "password": self._tap._config["password"],
            }
            try:
                response = self._request_token(
                    f"{self._tap._config['store_url']}/index.php/rest/V1/integration/admin/token",
                    payload,
                )
            except requests.RequestException:
                response = self._request_token(
                    f"{self._tap._config['store_url']}/rest/V1/integration/admin/token",
                    payload,
                )
            self._tap._config["access_token"] = response.json()

        with open(self._config_file, "w") as config_file:
            json.dump(self._tap._config, config_file, indent=4)
