from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class ConnectorTapTestRunner(VCRTapTestRunner):
    # custom_cookies are sent in the Cookie header; Authorization is filtered by default.
    FILTER_HEADERS = [*VCRTapTestRunner.FILTER_HEADERS, "cookie"]
    # Magento OAuth1 credentials and aliases not covered by the base token list.
    TOKEN_KEYS = [
        *VCRTapTestRunner.TOKEN_KEYS,
        "consumer_key",
        "consumer_secret",
        "oauth_token",
        "oauth_token_secret",
        "access_token_secret",
    ]

    PRESERVE_KEYS = {
        "id",
        "code",
        "entity_id",
        "updated_at",
        "total_count",
        "current_page",
        "total_pages",
        "visibility",
    }

    def module(self) -> str:
        return "tap_magento.tap"

    def launch(self):
        from tap_magento.tap import TapMagento

        TapMagento.cli()


if __name__ == "__main__":
    ConnectorTapTestRunner.main()
