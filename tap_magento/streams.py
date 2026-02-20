"""Stream type classes for tap-magento."""
from math import e
import requests
import pendulum
from datetime import datetime, timezone

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_magento.client import MagentoStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class StoresStream(MagentoStream):
    name = "stores"
    path = "/V1/store/storeConfigs"
    primary_keys = ["id"]
    records_jsonpath = "$.[*]"

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("website_id", th.NumberType),
    ).to_dict()

    def parse_response(self, response):
        if self.config.get("fetch_all_stores", False):
            yield from super().parse_response(response)

        resps = list(super().parse_response(response))
        store_ids_all = [] #initialize all store ids
        stores_by_id = {resp["id"]: resp for resp in resps}
        stores_by_code = {resp["code"]: resp for resp in resps}
        

        store_ids = self.config.get("store_id", [])

        if isinstance(store_ids, str) and store_ids:
            store_ids = store_ids.replace(" ", "").split(",")
            store_ids = [
                store_id if not store_id.isdigit() else int(store_id) for store_id in store_ids
            ]
        #In case store_id is an empty list or None. Populate with all stores
        if not store_ids:
            store_ids_all = list(stores_by_id.keys())
            store_ids = store_ids_all

        for store_id in store_ids:
            if isinstance(store_id, int) and store_id and store_id in stores_by_id:
                yield stores_by_id[store_id]
            elif isinstance(store_id, str) and store_id and store_id in stores_by_id:
                yield stores_by_code[store_id]
            else:
                self.logger.info(f"Skipping store_id/store_code: {store_id}")


    def get_child_context(self, record, context):
        return {
            "store_id": str(record["id"]),#We don't want default 0 store to be skipped
            "store_code": str(record["code"]),
            "base_currency_code": str(record["base_currency_code"])
        }

    def get_next_page_token(self, response, previous_token):
        return None


class UsersStream(MagentoStream):
    """Define custom stream."""

    name = "users"
    path = "/V1/users"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType, description="The user's system ID"),
        th.Property("age", th.NumberType, description="The user's age in years"),
        th.Property("email", th.StringType, description="The user's email address"),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property(
            "state", th.StringType, description="State name in ISO 3166-2 format"
        ),
        th.Property("zip", th.StringType),
    ).to_dict()


class OrdersStream(MagentoStream):
    """Define Order Stream"""

    name = "orders"
    path = "/V1/orders"
    primary_keys = []  # TODO
    replication_key = "updated_at"
    ids = []

    def get_url_params(self, context, next_page_token):
        """
        Overwrites get_url_params to add support for order_ids filtering
        """
        params = super().get_url_params(context, next_page_token)
        order_ids = [str(x) for x in self.config.get("order_ids", [])]
        if len(order_ids) > 0:
            params[
                "searchCriteria[filterGroups][0][filters][0][field]"
            ] = "entity_id"
            params[
                "searchCriteria[filterGroups][0][filters][0][value]"
            ] = ",".join(order_ids)
            params[
                "searchCriteria[filterGroups][0][filters][0][condition_type]"
            ] = "in"

        return params

    schema = th.PropertiesList(
        th.Property("adjustment_negative", th.NumberType),
        th.Property("adjustment_positive", th.NumberType),
        th.Property("applied_rule_ids", th.StringType),
        th.Property("base_adjustment_negative", th.NumberType),
        th.Property("base_adjustment_positive", th.NumberType),
        th.Property("base_currency_code", th.StringType),
        th.Property("base_discount_amount", th.NumberType),
        th.Property("base_discount_canceled", th.NumberType),
        th.Property("base_discount_invoiced", th.NumberType),
        th.Property("base_discount_refunded", th.NumberType),
        th.Property("base_grand_total", th.NumberType),
        th.Property("base_discount_tax_compensation_amount", th.NumberType),
        th.Property("base_discount_tax_compensation_invoiced", th.NumberType),
        th.Property("base_discount_tax_compensation_refunded", th.NumberType),
        th.Property("base_shipping_amount", th.NumberType),
        th.Property("base_shipping_canceled", th.NumberType),
        th.Property("base_shipping_discount_amount", th.NumberType),
        th.Property("base_shipping_discount_tax_compensation_amnt", th.NumberType),
        th.Property("base_shipping_incl_tax", th.NumberType),
        th.Property("base_shipping_invoiced", th.NumberType),
        th.Property("base_shipping_refunded", th.NumberType),
        th.Property("base_shipping_tax_amount", th.NumberType),
        th.Property("base_shipping_tax_refunded", th.NumberType),
        th.Property("base_subtotal", th.NumberType),
        th.Property("base_subtotal_canceled", th.NumberType),
        th.Property("base_subtotal_incl_tax", th.NumberType),
        th.Property("base_subtotal_invoiced", th.NumberType),
        th.Property("base_subtotal_refunded", th.NumberType),
        th.Property("base_tax_amount", th.NumberType),
        th.Property("base_tax_canceled", th.NumberType),
        th.Property("base_tax_invoiced", th.NumberType),
        th.Property("base_tax_refunded", th.NumberType),
        th.Property("base_total_canceled", th.NumberType),
        th.Property("base_total_due", th.NumberType),
        th.Property("base_total_invoiced", th.NumberType),
        th.Property("base_total_invoiced_cost", th.NumberType),
        th.Property("base_total_offline_refunded", th.NumberType),
        th.Property("base_total_online_refunded", th.NumberType),
        th.Property("base_total_paid", th.NumberType),
        th.Property("base_total_qty_ordered", th.NumberType),
        th.Property("base_total_refunded", th.NumberType),
        th.Property("base_to_global_rate", th.NumberType),
        th.Property("base_to_order_rate", th.NumberType),
        th.Property("billing_address_id", th.NumberType),
        th.Property("can_ship_partially", th.NumberType),
        th.Property("can_ship_partially_item", th.NumberType),
        th.Property("coupon_code", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("customer_dob", th.StringType),
        th.Property("customer_email", th.StringType),
        th.Property("customer_firstname", th.StringType),
        th.Property("customer_gender", th.NumberType),
        th.Property("customer_group_id", th.NumberType),
        th.Property("customer_id", th.NumberType),
        th.Property("customer_is_guest", th.NumberType),
        th.Property("customer_lastname", th.StringType),
        th.Property("customer_middlename", th.StringType),
        th.Property("customer_note", th.StringType),
        th.Property("customer_note_notify", th.NumberType),
        th.Property("customer_prefix", th.StringType),
        th.Property("customer_suffix", th.StringType),
        th.Property("customer_taxvat", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_canceled", th.NumberType),
        th.Property("discount_description", th.StringType),
        th.Property("discount_invoiced", th.NumberType),
        th.Property("discount_refunded", th.NumberType),
        th.Property("edit_increment", th.NumberType),
        th.Property("email_sent", th.NumberType),
        th.Property("entity_id", th.NumberType),
        th.Property("ext_customer_id", th.StringType),
        th.Property("ext_order_id", th.StringType),
        th.Property("forced_shipment_with_invoice", th.NumberType),
        th.Property("global_currency_code", th.StringType),
        th.Property("grand_total", th.NumberType),
        th.Property("discount_tax_compensation_amount", th.NumberType),
        th.Property("discount_tax_compensation_invoiced", th.NumberType),
        th.Property("discount_tax_compensation_refunded", th.NumberType),
        th.Property("hold_before_state", th.StringType),
        th.Property("hold_before_status", th.StringType),
        th.Property("increment_id", th.StringType),
        th.Property("is_virtual", th.NumberType),
        th.Property("order_currency_code", th.StringType),
        th.Property("original_increment_id", th.StringType),
        th.Property("payment_authorization_amount", th.NumberType),
        th.Property("payment_auth_expiration", th.NumberType),
        th.Property("protect_code", th.StringType),
        th.Property("quote_address_id", th.NumberType),
        th.Property("quote_id", th.NumberType),
        th.Property("relation_child_id", th.StringType),
        th.Property("relation_child_real_id", th.StringType),
        th.Property("relation_parent_id", th.StringType),
        th.Property("relation_parent_real_id", th.StringType),
        th.Property("remote_ip", th.StringType),
        th.Property("shipping_amount", th.NumberType),
        th.Property("shipping_canceled", th.NumberType),
        th.Property("shipping_description", th.StringType),
        th.Property("shipping_discount_amount", th.NumberType),
        th.Property("shipping_discount_tax_compensation_amount", th.NumberType),
        th.Property("shipping_incl_tax", th.NumberType),
        th.Property("shipping_invoiced", th.NumberType),
        th.Property("shipping_refunded", th.NumberType),
        th.Property("shipping_tax_amount", th.NumberType),
        th.Property("shipping_tax_refunded", th.NumberType),
        th.Property("state", th.StringType),
        th.Property("status", th.StringType),
        th.Property("store_currency_code", th.StringType),
        th.Property("store_id", th.NumberType),
        th.Property("store_name", th.StringType),
        th.Property("store_to_base_rate", th.NumberType),
        th.Property("store_to_order_rate", th.NumberType),
        th.Property("subtotal", th.NumberType),
        th.Property("subtotal_canceled", th.NumberType),
        th.Property("subtotal_incl_tax", th.NumberType),
        th.Property("subtotal_invoiced", th.NumberType),
        th.Property("subtotal_refunded", th.NumberType),
        th.Property("tax_amount", th.NumberType),
        th.Property("tax_canceled", th.NumberType),
        th.Property("tax_invoiced", th.NumberType),
        th.Property("tax_refunded", th.NumberType),
        th.Property("total_canceled", th.NumberType),
        th.Property("total_due", th.NumberType),
        th.Property("total_invoiced", th.NumberType),
        th.Property("total_item_count", th.NumberType),
        th.Property("total_offline_refunded", th.NumberType),
        th.Property("total_online_refunded", th.NumberType),
        th.Property("total_paid", th.NumberType),
        th.Property("total_qty_ordered", th.NumberType),
        th.Property("total_refunded", th.NumberType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("weight", th.NumberType),
        th.Property("x_forwarded_for", th.StringType),
        th.Property("items", th.ArrayType(th.CustomType({"type": ["string", "object"]}))),
        th.Property("billing_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment", th.CustomType({"type": ["object", "string"]})),
        th.Property(
            "status_histories",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "extension_attributes", th.CustomType({"type": ["object", "string"]})
        ),
    ).to_dict()

    def parse_response(self, response):
        try:
            items = response.json()['items']
            if items:
                max_date = max(pendulum.parse(x['updated_at']) for x in items)
                self.logger.info(f"Max date: {max_date}")

            for item in super().parse_response(response):
                if item["entity_id"] in self.ids:
                    continue

                self.ids.append(item["entity_id"])
                yield item
        except Exception as e:
            #Unable to parse the response. Log and then skip this page.
            self.logger.warn(f'Could not parse following response. {response.text}. {response.request.url} Skipping...')
            return []


class ProductsStream(MagentoStream):
    name = "products"
    path = "/{store_code}/V1/products"
    primary_keys = ["id", "store_id"]
    replication_key = "updated_at"
    parent_stream_type = StoresStream
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("sku", th.StringType),
        th.Property("store_id", th.StringType),
        th.Property("store_code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("attribute_set_id", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("status", th.NumberType),
        th.Property("visibility", th.NumberType),
        th.Property("type_id", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("weight", th.NumberType),
        th.Property(
            "extension_attributes", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "product_links",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "options",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "media_gallery_entries",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "tier_prices",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "custom_attributes",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "product_sku": record["sku"],
            "product_status": record["status"],
            "store_id": context["store_id"],
            "store_code": context["store_code"],
            "product_id": record["id"],
            "base_currency_code": context["base_currency_code"],
            "visibility": record["visibility"],
            "product_id": record["id"],
            "product_price": record["price"],
            "product_name": record["name"],
            "tier_prices": record.get("tier_prices", [])
        }

    def sync(self, context=None):
        super().sync(context)
        # Clear the batch (cache) for the prices stream
        prices_child_stream = next(s for s in self._tap.streams.values() if isinstance(s, PricesStream))
        
        if prices_child_stream.current_batch_context_dict:
            prices_child_stream.BATCH_SIZE = 0
            previous_key = list(prices_child_stream.current_batch_context_dict.keys())[-1]
            previous_context = prices_child_stream.current_batch_context_dict.get(previous_key)
            prices_child_stream._sync_records(previous_context)
        for sku, context in prices_child_stream.skus_with_visibility_1_this_store_dict.items():
            if sku in prices_child_stream.skus_variants_bundles_this_store_dict:
                continue
            prices_child_stream.current_visibility = 1
            prices_child_stream.clearing_visibility_1_skus = True
            prices_child_stream._sync_records(context)

        prices_child_stream.current_batch_context_dict = {}

        prices_child_stream.clearing_visibility_1_skus = False
        prices_child_stream.skus_with_visibility_1_this_store_dict = {}
        prices_child_stream.skus_variants_bundles_this_store_dict = {}
        prices_child_stream.processed_skus_this_store = []

class ProductsRenderInfoStream(MagentoStream):
    name = "products_render_info"
    path = "/{store_code}/V1/products-render-info"
    primary_keys = ["id", "store_id"]
    replication_key = None
    parent_stream_type = StoresStream
    ignore_parent_replication_key = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_datetime = datetime.now(timezone.utc)

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("url", th.StringType),
        th.Property("store_id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("hg_fetched_at", th.DateTimeType),
        th.Property("currency_code", th.StringType),
        th.Property("is_salable", th.StringType),
        th.Property(
            "extension_attributes", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "price_info", th.CustomType({"type": ["object", "string"]}),
        )
    ).to_dict()

    def get_url_params(self, context, next_page_token):

        """
        Overwrites get_url_params to add support for order_ids filtering
        """
        params = super().get_url_params(context, next_page_token)
        params[
            "storeId"
        ] = context["store_id"]
        params["currencyCode"] = context["base_currency_code"]
        params["searchCriteria[sortOrders][0][field]"] = "url"
        params["searchCriteria[sortOrders][0][direction]"] = "ASC"

        return params

    def post_process(self, row, context):
        row["hg_fetched_at"] = self.current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        return row

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        next_page_token = None
        response_json = response.json()
        if len(response_json.get("items", [])) == self.page_size:
            return (previous_token or 1) + 1
        else:
            return None

class PricesStream(MagentoStream):
    """
    This stream is used to fetch the prices of products.
    It is used to fetch the prices of the products with visibility 2 and 4 by querying the graphql endpoint normally.
    It is used to fetch the prices of the products with visibility 3 by querying the graphql endpoint and searching for each individual SKU.
    It simply returns the prices of products with visibility 1 from the parent products stream (as we cannot get more price information).
    """
    name = "product_prices_graphql_enabled"
    path = ""
    primary_keys = ["id", "store_id"]
    parent_stream_type = ProductsStream
    ignore_parent_replication_key = True
    replication_key = None
    skus_with_visibility_1_this_store_dict = {}
    skus_variants_bundles_this_store_dict = {}

    # Used to clear the cache of visibility 1 skus at end (to avoid fetching the same skus again)
    clearing_visibility_1_skus = False
    processed_skus_this_store = []

    # Don't remove, otherwise you will have a memory leak with context growing for each record.
    state_partitioning_keys = ["store_id"]
    current_visibility = None

    # Keeps track of which store ids have been queried for all products with visibility 2 and 4.
    fetched_store_ids_visibility_2_4 = []

    # Size of query batches for the search (visibility 3) endpoint.
    BATCH_SIZE = 3
    
    # Set individually for each visibility
    records_jsonpath = "$.data.products.items[*]"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_batch_context_dict: dict = {}
        self.current_datetime = datetime.now(timezone.utc)
        self.rest_method = "GET"


    @property
    def url_base(self) -> str:
        original_url_base = super().url_base

        if self.current_visibility == 1:
            self.records_jsonpath = "$.items[*]"
            return original_url_base


        if self.current_visibility == 3:
            self.records_jsonpath = "$.data[*]"
        else:
            self.records_jsonpath = "$.data.products.items[*]"
       
        return original_url_base.replace("rest", "graphql")

    def get_next_page_token(self, response, previous_token):
        if self.current_visibility in [1, 3]:
            return None

        data = response.json()
        page_info = data["data"]["products"]["page_info"]
        if page_info["current_page"] >= page_info["total_pages"]:
            return None
        return page_info["current_page"] + 1

    GRAPHQL_FIELDS = """
            id
            uid
            name
            sku
            url_key
            type_id
            price_range {
                minimum_price {
                    regular_price { value currency }
                    final_price { value currency }
                    discount { amount_off percent_off }
                }
                maximum_price {
                    regular_price { value currency }
                    final_price { value currency }
                }
            }
            price_tiers {
                quantity
                final_price { value currency }
                discount { amount_off percent_off }
            }
            ... on ConfigurableProduct {
                variants {
                    product {
                        id
                        uid
                        sku
                        name
                        url_key
                        type_id
                        price_range {
                            minimum_price {
                                regular_price { value currency }
                                final_price { value currency }
                            }
                        }
                    }
                    attributes {
                        label
                        code
                        value_index
                    }
                }
            }
            ... on BundleProduct {
                bundle_items: items {
                    sku
                    title
                    options {
                        label
                        quantity
                        product {
                            id
                            uid
                            sku
                            name
                            url_key
                            type_id
                            price_range {
                                minimum_price {
                                    final_price { value currency }
                                }
                            }
                        }
                    }
                }
            }
    """

    def prepare_request_payload(self, context, next_page_token):
        if self.current_visibility == 1:
            return super().prepare_request_payload(context, next_page_token)
        elif self.current_visibility == 3:
            # Query the graphql endpoint for each SKU in the current batch.
            fields = "items { " + self.GRAPHQL_FIELDS.replace('\n', ' ').replace('  ', ' ').strip() + " }"

            many_sku_query = "\n".join(
                f'product_{i}: products(search: "{sku}", filter: {{ sku: {{ eq: "{sku}" }} }}) {{ {fields} }}'
                for i, sku in enumerate(self.current_batch_context_dict.keys())
            )
            
            many_sku_query = f"{{ {many_sku_query} }}"
            payload = {
                "query": many_sku_query
            }
            return payload
        elif self.current_visibility in [2, 4]:
            # Query the graphql endpoint for all products with visibility 2 and 4 on current store id.
            return {
                "query": f"""
                    query getProducts($current_page: Int, $page_size: Int) {{
                        products(
                            pageSize: $page_size
                            currentPage: $current_page
                            filter: {{}}
                        ) {{
                            total_count
                            page_info {{
                                page_size
                                current_page
                                total_pages
                            }}
                            items {{
                                {self.GRAPHQL_FIELDS}
                            }}
                        }}
                    }}""",
                "variables": {
                    "page_size": max(self.default_page_size//2, 1),
                    "current_page": next_page_token or 1,
                },
            }
        else:
            return {}

    def get_additional_headers_with_context(self, context: {}) -> dict:
        headers = super().http_headers
        headers["store"] = context["store_code"]
        return headers

    def get_url_params(self, context, next_page_token):
        return {}

    def post_process(self, row, context):
        row["hg_fetched_at"] = self.current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        if self.current_visibility in [2, 4]:
            if "status" in row and row["status"] == 2:
                # Status = 2 means the product is disabled, so we can't get any price information.
                row["price_null_deactivated_status"] = True
            else:
                row["price_null_deactivated_status"] = False
            row["store_id"] = context["store_id"]
            row["store_code"] = context["store_code"]
            row["currency_code"] = context["base_currency_code"]
            if row["sku"] not in self.processed_skus_this_store:
                self.processed_skus_this_store.append(row["sku"])
            return row
        elif self.current_visibility == 3:

            # Make sure we are using the right store id for variants, bundles, and main products.
            if row["sku"] in self.current_batch_context_dict:
                current_context = self.current_batch_context_dict[row["sku"]]
            elif "variant_parent_sku" in row and row["variant_parent_sku"] in self.current_batch_context_dict:
                current_context = self.current_batch_context_dict[row["variant_parent_sku"]]
            elif "bundle_parent_sku" in row and row["bundle_parent_sku"] in self.current_batch_context_dict:
                current_context = self.current_batch_context_dict[row["bundle_parent_sku"]]
            else:
                current_context = list(self.current_batch_context_dict.values())[-1]
            row["store_id"] = current_context["store_id"]
            row["store_code"] = current_context["store_code"]
            row["currency_code"] = current_context["base_currency_code"]
            return row
        elif self.current_visibility == 1:
            if context["product_status"] == 2:
                row["price_null_deactivated_status"] = True
            else:
                row["price_null_deactivated_status"] = False
            row["invisible_in_catalog_and_search"] = True
            row["price_no_special"] = context["product_price"]
            row["tier_prices"] = context["tier_prices"]
            return row
        else:
            return row


    def deal_with_bundle_and_variants(self, product: dict):
        # Yield the main product
        product["is_variant"] = False
        product["is_part_of_bundle"] = False
        product["bundle_parent_sku"] = None
        product["variant_parent_sku"] = None

        # this happens for bundle parent products
        if "title" in product:
            product["name"] = product["title"]
        yield product

        parent_sku = product["sku"]

        # Go through each variant and yield it.
        variants_list = product.get("variants", [])

        # this is a shallow copy, just makes it easier to read
        current_variant = product
        if "variants" in current_variant:
            current_variant.pop("variants")

        for variant in variants_list:
            current_variant["is_variant"] = True
            current_variant["variant_parent_sku"] = parent_sku
            current_variant["id"] = variant["product"]["id"]
            current_variant["uid"] = variant["product"].get("uid")
            current_variant["url_key"] = variant["product"].get("url_key")
            current_variant["sku"] = variant["product"]["sku"]
            current_variant["name"] = variant["product"]["name"]
            current_variant["price_range"] = variant["product"]["price_range"]
            current_variant["attributes"] = variant.get("attributes", [])
            current_variant["created_at"] = variant["product"].get("created_at")
            current_variant["updated_at"] = variant["product"].get("updated_at")

            if variant["product"]["sku"] not in self.skus_variants_bundles_this_store_dict:
                self.skus_variants_bundles_this_store_dict[variant["product"]["sku"]] = ""
                yield current_variant

        # Handle bundle children
        product["is_variant"] = False
        product["is_part_of_bundle"] = False
        product["bundle_parent_sku"] = None
        product["variant_parent_sku"] = None

        bundle_items = product.get("bundle_items", [])
        current_bundle_child = product
        if "bundle_items" in current_bundle_child:
            current_bundle_child.pop("bundle_items")

        for bundle_item in bundle_items:
            for option in bundle_item.get("options", []):
                bundle_product = option.get("product")
                if bundle_product:
                    current_bundle_child["is_variant"] = True
                    current_bundle_child["bundle_parent_sku"] = parent_sku
                    current_bundle_child["id"] = bundle_product["id"]
                    current_bundle_child["uid"] = bundle_product.get("uid")
                    current_bundle_child["url_key"] = bundle_product.get("url_key")
                    current_bundle_child["sku"] = bundle_product["sku"]
                    current_bundle_child["name"] = bundle_product["name"]
                    current_bundle_child["price_range"] = bundle_product["price_range"]
                    current_bundle_child["created_at"] = bundle_product.get("created_at")
                    current_bundle_child["updated_at"] = bundle_product.get("updated_at")

                    if bundle_product["sku"] not in self.skus_variants_bundles_this_store_dict:
                        self.skus_variants_bundles_this_store_dict[variant["product"]["sku"]] = ""
                        yield current_bundle_child

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if self.current_visibility == 3:
            for product_batch in super().parse_response(response):
                for product_batch_item in product_batch:
                    if len(product_batch[product_batch_item]["items"]) == 0:
                        product = {}
                    else: 
                        product = product_batch[product_batch_item]["items"][0]
                    
                    yield from self.deal_with_bundle_and_variants(product)
            return
        elif self.current_visibility in [2, 4]:
            for product in super().parse_response(response):
                yield from self.deal_with_bundle_and_variants(product)
            return
        else:
            yield from super().parse_response(response)

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("store_id", th.StringType),
        th.Property("store_code", th.StringType),
        th.Property("currency_code", th.StringType),
        th.Property("uid", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("name", th.StringType),
        th.Property("url_key", th.StringType),
        th.Property("type_id", th.StringType),
        th.Property("is_variant", th.BooleanType),
        th.Property("is_part_of_bundle", th.BooleanType),
        th.Property("variant_parent_sku", th.StringType),
        th.Property("bundle_parent_sku", th.StringType),
        th.Property("invisible_in_catalog_and_search", th.BooleanType),
        th.Property("price_null_deactivated_status", th.BooleanType),
        th.Property("price_no_special", th.NumberType),
        th.Property("hg_fetched_at", th.DateTimeType),
        th.Property("attributes", th.ArrayType(th.CustomType({"type": ["object", "string"]})),),
        th.Property(
            "price_range", th.CustomType({"type": ["object", "string"]}),
        ),
        th.Property(
            "price_tiers", th.ArrayType(th.CustomType({"type": ["object", "string"]})),
        ),
        # yes, there are price_tiers (graphql) and tier_prices (rest)
        th.Property(
            "tier_prices",
            th.ArrayType(th.CustomType({"type": ["string", "object"]})),
        ),
        th.Property(
            "variants", th.ArrayType(th.CustomType({"type": ["object", "string"]})),
        ),
        th.Property(
            "bundle_items", th.ArrayType(th.CustomType({"type": ["object", "string"]})),
        ),
    ).to_dict()

    def row_from_context(self, context: dict) -> dict:
        return {
            "sku": context["product_sku"],
            "store_id": context["store_id"],
            "store_code": context["store_code"],
            "id": context["product_id"],
            "visibility": context["visibility"],
            "status": context["product_status"],
            "currency_code": context["base_currency_code"],
            "name": context["product_name"]
        }

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:

        # At the end of each store, we go through the skus with visibility 1 and yield if not gotten yet.
        if self.clearing_visibility_1_skus and context.get("visibility", 0) == 1:
            row = self.post_process(self.row_from_context(context), context)
            if row["sku"] not in self.processed_skus_this_store:
                yield row
                self.processed_skus_this_store.append(row["sku"])
            return

        # If haven't fetched this store yet, fetch the products with visibility 2 or 4, then continue.
        if context["store_id"] not in self.fetched_store_ids_visibility_2_4:
            self.rest_method = "POST"
            self.fetched_store_ids_visibility_2_4.append(context["store_id"])
            big_context = {}
            big_context["store_id"] = context["store_id"]
            big_context["store_code"] = context["store_code"]
            big_context["base_currency_code"] = context["base_currency_code"]
            self.current_visibility = 2
            big_context["action"] = "Querying products w/ visibility 2 and 4"
            yield from super().get_records(big_context)
        
        # Set the current visibility of the product. A lot of logic depends on this.
        self.current_visibility = context["visibility"]
        
        # We already got these products.
        if context["visibility"] == 2 or context["visibility"] == 4:
            return

        # Build batches for products with visibility 3, as they need individual queries.
        elif context["visibility"] == 3:
            self.rest_method = "POST"
            if not context or not context.get("product_sku"):
                return

            # If the product is disabled (status 2), yield it immediately.
            if context["product_status"] == 2:
                row = {}
                row = self.row_from_context(context)
                row["price_null_deactivated_status"] = True
                row["hg_fetched_at"] = self.current_datetime.strftime("%Y-%m-%d %H:%M:%S")
                if row["sku"] not in self.processed_skus_this_store:
                    yield row
                    self.processed_skus_this_store.append(row["sku"])
                return

            # If the batch is not empty, check if the current batch is from a different store. If yes, then query that batch and start a new one with current context.
            if len(list(self.current_batch_context_dict.keys())) > 0:
                previous_key = list(self.current_batch_context_dict.keys())[-1]
                previous_store_id = self.current_batch_context_dict.get(previous_key).get("store_id")
                if (context["store_id"] != previous_store_id):
                    yield from super().get_records(context)
                    for sku in self.current_batch_context_dict.keys():
                        if sku not in self.processed_skus_this_store:
                            self.processed_skus_this_store.append(sku)
                    self.current_batch_context_dict = {}
                    self.current_batch_context_dict[context["product_sku"]] = context
                    return

            # If we haven't process this sku yet (possible because could be variant), add to batch.
            if context["product_sku"] not in self.processed_skus_this_store:
                self.current_batch_context_dict[context["product_sku"]] = context

            # If the batch is not full, return.
            if len(self.current_batch_context_dict.keys()) < self.BATCH_SIZE:
                return

            # Query the batch and wipe the cache.
            yield from super().get_records(context)

            for sku in self.current_batch_context_dict.keys():
                if sku not in self.processed_skus_this_store:
                    self.processed_skus_this_store.append(sku)

            self.current_batch_context_dict = {}
        elif context["visibility"] == 1:
            self.skus_with_visibility_1_this_store_dict[context["product_sku"]] = context
        else:
            self.logger.warning(f"Invalid visibility for prices stream: {context['visibility']}")
            return

class ProductAttributesStream(MagentoStream):
    name = "product_attributes"
    path = "/V1/products/attributes"
    primary_keys = ["attribute_id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("attribute_id", th.NumberType),
        th.Property("attribute_code", th.StringType),
        th.Property("frontend_input", th.StringType),
        th.Property("entity_type_id", th.StringType),
        th.Property("is_required", th.BooleanType),
        th.Property("backend_type", th.StringType),
        th.Property("backend_model", th.StringType),
        th.Property("is_unique", th.StringType),
        th.Property("is_wysiwyg_enabled", th.BooleanType),
        th.Property("is_html_allowed_on_front", th.BooleanType),
        th.Property("used_for_sort_by", th.BooleanType),
        th.Property("is_filterable", th.BooleanType),
        th.Property("is_filterable_in_search", th.BooleanType),
        th.Property("is_used_in_grid", th.BooleanType),
        th.Property("is_visible_in_grid", th.BooleanType),
        th.Property("is_filterable_in_grid", th.BooleanType),
        th.Property("position", th.NumberType),
        th.Property("apply_to", th.CustomType({"type": ["array", "string"]})),
        th.Property("is_searchable", th.StringType),
        th.Property("is_visible_in_advanced_search", th.StringType),
        th.Property("is_comparable", th.StringType),
        th.Property("is_used_for_promo_rules", th.StringType),
        th.Property("is_visible_on_front", th.StringType),
        th.Property("used_in_product_listing", th.StringType),
        th.Property("is_visible", th.BooleanType),
        th.Property("scope", th.StringType),
        th.Property("options", th.CustomType({"type": ["array", "string"]})),
        th.Property("is_user_defined", th.BooleanType),
        th.Property("default_frontend_label", th.StringType),
        th.Property("frontend_labels", th.CustomType({"type": ["array", "string"]})),
        th.Property("validation_rules", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "attribute_code": record["attribute_code"],
        }


class ProductAttributeDetailsStream(MagentoStream):
    name = "product_attribute_details"
    path = "/V1/products/attributes/{attribute_code}"
    primary_keys = ["attribute_id"]
    records_jsonpath: str = "$[*]"
    replication_key = None
    parent_stream_type = ProductAttributesStream

    schema = th.PropertiesList(
        th.Property("attribute_id", th.NumberType),
        th.Property("is_wysiwyg_enabled", th.BooleanType),
        th.Property("is_html_allowed_on_front", th.BooleanType),
        th.Property("used_for_sort_by", th.BooleanType),
        th.Property("is_filterable", th.BooleanType),
        th.Property("is_filterable_in_search", th.BooleanType),
        th.Property("is_used_in_grid", th.BooleanType),
        th.Property("is_visible_in_grid", th.BooleanType),
        th.Property("is_filterable_in_grid", th.BooleanType),
        th.Property("position", th.NumberType),
        th.Property("apply_to", th.CustomType({"type": ["array", "string"]})),
        th.Property("is_searchable", th.StringType),
        th.Property("is_visible_in_advanced_search", th.StringType),
        th.Property("is_comparable", th.StringType),
        th.Property("is_used_for_promo_rules", th.StringType),
        th.Property("is_visible_on_front", th.StringType),
        th.Property("used_in_product_listing", th.StringType),
        th.Property("is_visible", th.BooleanType),
        th.Property("scope", th.StringType),
        th.Property("attribute_code", th.StringType),
        th.Property("frontend_input", th.StringType),
        th.Property("entity_type_id", th.StringType),
        th.Property("is_required", th.BooleanType),
        th.Property("options", th.CustomType({"type": ["array", "string"]})),
        th.Property("is_user_defined", th.BooleanType),
        th.Property("default_frontend_label", th.StringType),
        th.Property("frontend_labels", th.CustomType({"type": ["array", "string"]})),
        th.Property("backend_type", th.StringType),
        th.Property("backend_model", th.StringType),
        th.Property("is_unique", th.StringType),
        th.Property("validation_rules", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class ProductItemStocksStream(MagentoStream):
    name = "product_item_stocks"
    path = "/{store_code}/V1/stockItems/lowStock"
    primary_keys = ["store_id", "item_id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = None
    _page_size = 10000
    parent_stream_type = StoresStream

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("item_id", th.NumberType),
        th.Property("product_id", th.NumberType),
        th.Property("stock_id", th.NumberType),
        th.Property("store_id", th.StringType),
        th.Property("qty", th.NumberType),
        th.Property("is_in_stock", th.BooleanType),
        th.Property("is_qty_decimal", th.BooleanType),
        th.Property("show_default_notification_message", th.BooleanType),
        th.Property("use_config_min_qty", th.BooleanType),
        th.Property("min_qty", th.NumberType),
        th.Property("use_config_min_sale_qty", th.NumberType),
        th.Property("min_sale_qty", th.NumberType),
        th.Property("use_config_max_sale_qty", th.BooleanType),
        th.Property("max_sale_qty", th.NumberType),
        th.Property("use_config_backorders", th.BooleanType),
        th.Property("backorders", th.NumberType),
        th.Property("use_config_notify_stock_qty", th.BooleanType),
        th.Property("notify_stock_qty", th.NumberType),
        th.Property("use_config_qty_increments", th.BooleanType),
        th.Property("qty_increments", th.NumberType),
        th.Property("use_config_enable_qty_inc", th.BooleanType),
        th.Property("enable_qty_increments", th.BooleanType),
        th.Property("use_config_manage_stock", th.BooleanType),
        th.Property("manage_stock", th.BooleanType),
        th.Property("low_stock_date", th.DateTimeType),
        th.Property("is_decimal_divided", th.BooleanType),
        th.Property("stock_status_changed_auto", th.NumberType),
    ).to_dict()

    def post_process(self, row, context):
        row["store_id"] = context.get("store_id")
        return row

class ProductStockStatusesStream(MagentoStream):
    name = "product_stock_statuses"
    path = "/{store_code}/V1/stockStatuses/{product_sku}"
    primary_keys = ["stock_id", "product_id"]
    records_jsonpath: str = "$.[*]"
    replication_key = None
    parent_stream_type = ProductsStream

    schema = th.PropertiesList(
        th.Property("product_id", th.NumberType),
        th.Property("store_id", th.StringType),
        th.Property("stock_id", th.NumberType),
        th.Property("qty", th.NumberType),
        th.Property("stock_status", th.NumberType),
        th.Property("stock_item",
            th.ObjectType(
                th.Property("item_id", th.NumberType),
                th.Property("product_id", th.NumberType),
                th.Property("stock_id", th.NumberType),
                th.Property("qty", th.NumberType),
                th.Property("is_in_stock", th.BooleanType),
                th.Property("is_qty_decimal", th.BooleanType),
                th.Property("show_default_notification_message", th.BooleanType),
                th.Property("use_config_min_qty", th.BooleanType),
                th.Property("min_qty", th.NumberType),
                th.Property("use_config_min_sale_qty", th.NumberType),
                th.Property("min_sale_qty", th.NumberType),
                th.Property("use_config_max_sale_qty", th.BooleanType),
                th.Property("max_sale_qty", th.NumberType),
                th.Property("use_config_backorders", th.BooleanType),
                th.Property("backorders", th.NumberType),
                th.Property("use_config_notify_stock_qty", th.BooleanType),
                th.Property("notify_stock_qty", th.NumberType),
                th.Property("use_config_qty_increments", th.BooleanType),
                th.Property("qty_increments", th.NumberType),
                th.Property("use_config_enable_qty_inc", th.BooleanType),
                th.Property("enable_qty_increments", th.BooleanType),
                th.Property("use_config_manage_stock", th.BooleanType),
                th.Property("manage_stock", th.BooleanType),
                th.Property("low_stock_date", th.DateTimeType),
                th.Property("is_decimal_divided", th.BooleanType),
                th.Property("stock_status_changed_auto", th.NumberType),
            )
        ),
    ).to_dict()

    def post_process(self, row, context):
        # Ignore disabled products
        if context.get("product_status") == 2:
            return None

        row["store_id"] = context.get("store_id")
        return row


class CategoryStream(MagentoStream):
    name = "categories"
    path = "/V1/categories/list"
    primary_keys = ["id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("parent_id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("position", th.NumberType),
        th.Property("level", th.NumberType),
        th.Property("children", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("path", th.StringType),
        th.Property("include_in_menu", th.BooleanType),
        th.Property("available_sort_by", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_attributes", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()


class SaleRulesStream(MagentoStream):
    name = "salerules"
    path = "/V1/salesRules/search"
    primary_keys = ["rule_id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("rule_id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("store_labels", th.CustomType({"type": ["array", "string"]})),
        th.Property("description", th.StringType),
        th.Property("website_ids", th.CustomType({"type": ["array", "string"]})),
        th.Property("customer_group_ids", th.CustomType({"type": ["array", "string"]})),
        th.Property("from_date", th.DateTimeType),
        th.Property("uses_per_customer", th.NumberType),
        th.Property("is_active", th.BooleanType),
        th.Property("condition", th.CustomType({"type": ["object", "string"]})),
        th.Property("action_condition", th.CustomType({"type": ["object", "string"]})),
        th.Property("stop_rules_processing", th.BooleanType),
        th.Property("is_advanced", th.BooleanType),
        th.Property("sort_order", th.NumberType),
        th.Property("simple_action", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_step", th.NumberType),
        th.Property("apply_to_shipping", th.BooleanType),
        th.Property("times_used", th.NumberType),
        th.Property("is_rss", th.BooleanType),
        th.Property("coupon_type", th.StringType),
        th.Property("use_auto_generation", th.BooleanType),
        th.Property("uses_per_coupon", th.NumberType),
        th.Property("simple_free_shipping", th.StringType),
        th.Property(
            "extension_attributes", th.CustomType({"type": ["object", "string"]})
        ),
    ).to_dict()


class CouponsStream(MagentoStream):
    name = "coupons"
    path = "/V1/coupons/search"
    primary_keys = ["coupon_id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("coupon_id", th.NumberType),
        th.Property("rule_id", th.NumberType),
        th.Property("code", th.StringType),
        th.Property("usage_limit", th.NumberType),
        th.Property("times_used", th.NumberType),
        th.Property("is_primary", th.BooleanType),
        th.Property("type", th.NumberType),
    ).to_dict()


class InvoicesStream(MagentoStream):
    name = "invoices"
    path = "/V1/invoices"
    primary_keys = ["increment_id"]
    records_jsonpath: str = "$.items[*]"
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("base_currency_code", th.StringType),
        th.Property("base_discount_amount", th.NumberType),
        th.Property("base_grand_total", th.NumberType),
        th.Property("base_discount_tax_compensation_amount", th.NumberType),
        th.Property("base_shipping_amount", th.NumberType),
        th.Property("base_shipping_discount_tax_compensation_amnt", th.NumberType),
        th.Property("base_shipping_incl_tax", th.NumberType),
        th.Property("base_shipping_tax_amount", th.NumberType),
        th.Property("base_subtotal", th.NumberType),
        th.Property("base_subtotal_incl_tax", th.NumberType),
        th.Property("base_tax_amount", th.NumberType),
        th.Property("base_total_refunded", th.NumberType),
        th.Property("base_to_global_rate", th.NumberType),
        th.Property("base_to_order_rate", th.NumberType),
        th.Property("billing_address_id", th.NumberType),
        th.Property("can_void_flag", th.NumberType),
        th.Property("created_at", th.DateTimeType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_description", th.StringType),
        th.Property("email_sent", th.NumberType),
        th.Property("entity_id", th.NumberType),
        th.Property("global_currency_code", th.StringType),
        th.Property("grand_total", th.NumberType),
        th.Property("discount_tax_compensation_amount", th.NumberType),
        th.Property("increment_id", th.StringType),
        th.Property("is_used_for_refund", th.NumberType),
        th.Property("order_currency_code", th.StringType),
        th.Property("order_id", th.NumberType),
        th.Property("shipping_address_id", th.NumberType),
        th.Property("shipping_amount", th.NumberType),
        th.Property("shipping_discount_tax_compensation_amount", th.NumberType),
        th.Property("shipping_incl_tax", th.NumberType),
        th.Property("shipping_tax_amount", th.NumberType),
        th.Property("state", th.NumberType),
        th.Property("store_currency_code", th.StringType),
        th.Property("store_id", th.NumberType),
        th.Property("store_to_base_rate", th.NumberType),
        th.Property("store_to_order_rate", th.NumberType),
        th.Property("subtotal", th.NumberType),
        th.Property("subtotal_incl_tax", th.NumberType),
        th.Property("tax_amount", th.NumberType),
        th.Property("total_qty", th.NumberType),
        th.Property("transaction_id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("items", th.CustomType({"type": ["array", "string"]})),
        th.Property("comments", th.CustomType({"type": ["array", "string"]})),
        th.Property("extension_attributes", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()


class StoreConfigsStream(MagentoStream):
    name = "store_configs"
    path = "/{store_code}/V1/store/storeConfigs"
    primary_keys = ["id"]
    records_jsonpath: str = "$.[*]"
    parent_stream_type = StoresStream
    ignore_parent_replication_key = True

    def get_next_page_token(self, response, previous_token):
        return None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("code", th.StringType),
        th.Property("website_id", th.NumberType),
        th.Property("locale", th.StringType),
        th.Property("base_currency_code", th.StringType),
        th.Property("default_display_currency_code", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("weight_unit", th.StringType),
        th.Property("base_url", th.StringType),
        th.Property("base_link_url", th.StringType),
        th.Property("base_static_url", th.StringType),
        th.Property("base_media_url", th.StringType),
        th.Property("secure_base_url", th.StringType),
        th.Property("secure_base_link_url", th.StringType),
        th.Property("secure_base_static_url", th.StringType),
        th.Property("secure_base_media_url", th.StringType),
    ).to_dict()


class StoreWebsitesStream(MagentoStream):
    name = "store_websites"
    path = "/{store_code}/V1/store/websites"
    primary_keys = ["id"]
    records_jsonpath: str = "$.[*]"
    parent_stream_type = StoresStream
    ignore_parent_replication_key = True

    def get_next_page_token(self, response, previous_token):
        return None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("code", th.StringType),
        th.Property("name", th.StringType),
        th.Property("default_group_id", th.NumberType),
    ).to_dict()


class SourceItemsStream(MagentoStream):
    name = "source_items"
    path = "/{store_code}/V1/inventory/source-items"
    primary_keys = None
    records_jsonpath: str = "$.items[*]"
    parent_stream_type = StoresStream
    ignore_parent_replication_key = True

    schema = th.PropertiesList(
        th.Property("sku", th.StringType),
        th.Property("source_code", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("status", th.NumberType),
    ).to_dict()


class CartsStream(MagentoStream):
    name = "carts"
    path = "/V1/carts/search"
    primary_keys = ["id"]
    records_jsonpath: str = "$.items[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.StringType),
        th.Property("converted_at", th.StringType),
        th.Property("is_active", th.BooleanType),
        th.Property("is_virtual", th.BooleanType),
        th.Property("items_count", th.IntegerType),
        th.Property("items_qty", th.IntegerType),
        th.Property("customer", th.CustomType({"type": ["object"]})),
        th.Property("billing_address", th.CustomType({"type": ["object"]})),
        th.Property("reserved_order_id", th.StringType),
        th.Property("orig_order_id", th.IntegerType),
        th.Property("currency", th.CustomType({"type": ["object"]})),
        th.Property("customer_is_guest", th.BooleanType),
        th.Property("customer_note", th.StringType),
        th.Property("customer_note_notify", th.BooleanType),
        th.Property("customer_tax_class_id", th.IntegerType),
        th.Property("store_id", th.IntegerType),
        th.Property("extension_attributes", th.CustomType({"type": ["object"]})),
    ).to_dict()

    def parse_response(self, response):
        return super().parse_response(response)


class CreditMemosStream(MagentoStream):
    name = "credit_memos"
    path = "/V1/creditmemos"
    primary_keys = ["entity_id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("adjustment", th.NumberType),
        th.Property("adjustment_negative", th.NumberType),
        th.Property("adjustment_positive", th.NumberType),
        th.Property("base_adjustment", th.NumberType),
        th.Property("base_adjustment_negative", th.NumberType),
        th.Property("base_adjustment_positive", th.NumberType),
        th.Property("base_currency_code", th.StringType),
        th.Property("base_discount_amount", th.NumberType),
        th.Property("base_grand_total", th.NumberType),
        th.Property("base_discount_tax_compensation_amount", th.NumberType),
        th.Property("base_shipping_amount", th.NumberType),
        th.Property("base_shipping_discount_tax_compensation_amnt", th.NumberType),
        th.Property("base_shipping_incl_tax", th.NumberType),
        th.Property("base_shipping_tax_amount", th.NumberType),
        th.Property("base_subtotal", th.NumberType),
        th.Property("base_subtotal_incl_tax", th.NumberType),
        th.Property("base_tax_amount", th.NumberType),
        th.Property("base_to_global_rate", th.NumberType),
        th.Property("base_to_order_rate", th.NumberType),
        th.Property("billing_address_id", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("creditmemo_status", th.NumberType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_description", th.StringType),
        th.Property("email_sent", th.NumberType),
        th.Property("entity_id", th.IntegerType),
        th.Property("global_currency_code", th.StringType),
        th.Property("grand_total", th.NumberType),
        th.Property("discount_tax_compensation_amount", th.NumberType),
        th.Property("increment_id", th.StringType),
        th.Property("invoice_id", th.IntegerType),
        th.Property("order_currency_code", th.StringType),
        th.Property("order_id", th.IntegerType),
        th.Property("shipping_address_id", th.IntegerType),
        th.Property("shipping_amount", th.NumberType),
        th.Property("shipping_discount_tax_compensation_amount", th.NumberType),
        th.Property("shipping_incl_tax", th.NumberType),
        th.Property("shipping_tax_amount", th.NumberType),
        th.Property("state", th.NumberType),
        th.Property("store_currency_code", th.StringType),
        th.Property("store_id", th.IntegerType),
        th.Property("store_to_base_rate", th.NumberType),
        th.Property("store_to_order_rate", th.NumberType),
        th.Property("subtotal", th.NumberType),
        th.Property("subtotal_incl_tax", th.NumberType),
        th.Property("tax_amount", th.NumberType),
        th.Property("transaction_id", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("items", th.ArrayType(
            th.ObjectType(
                th.Property("additional_data", th.StringType),
                th.Property("base_cost", th.NumberType),
                th.Property("base_discount_amount", th.NumberType),
                th.Property("base_discount_tax_compensation_amount", th.NumberType),
                th.Property("base_price", th.NumberType),
                th.Property("base_price_incl_tax", th.NumberType),
                th.Property("base_row_total", th.NumberType),
                th.Property("base_row_total_incl_tax", th.NumberType),
                th.Property("base_tax_amount", th.NumberType),
                th.Property("base_weee_tax_applied_amount", th.NumberType),
                th.Property("base_weee_tax_applied_row_amnt", th.NumberType),
                th.Property("base_weee_tax_disposition", th.NumberType),
                th.Property("base_weee_tax_row_disposition", th.NumberType),
                th.Property("description", th.StringType),
                th.Property("discount_amount", th.NumberType),
                th.Property("entity_id", th.IntegerType),
                th.Property("discount_tax_compensation_amount", th.NumberType),
                th.Property("name", th.StringType),
                th.Property("order_item_id", th.IntegerType),
                th.Property("parent_id", th.IntegerType),
                th.Property("price", th.NumberType),
                th.Property("price_incl_tax", th.NumberType),
                th.Property("product_id", th.IntegerType),
                th.Property("qty", th.NumberType),
                th.Property("row_total", th.NumberType),
                th.Property("row_total_incl_tax", th.NumberType),
                th.Property("sku", th.StringType),
                th.Property("tax_amount", th.NumberType),
                th.Property("weee_tax_applied", th.StringType),
                th.Property("weee_tax_applied_amount", th.NumberType),
                th.Property("weee_tax_applied_row_amount", th.NumberType),
                th.Property("weee_tax_disposition", th.NumberType),
                th.Property("weee_tax_row_disposition", th.NumberType),
                th.Property("extension_attributes", th.CustomType({"type": ["string", "object"]})),
            )
        )),
        th.Property("comments", th.ArrayType(
            th.ObjectType(
                th.Property("comment", th.StringType),
                th.Property("created_at", th.DateTimeType),
                th.Property("entity_id", th.IntegerType),
                th.Property("is_customer_notified", th.NumberType),
                th.Property("is_visible_on_front", th.NumberType),
                th.Property("parent_id", th.IntegerType),
                th.Property("extension_attributes", th.CustomType({"type": ["string", "object"]})),
            )
        )),
        th.Property("extension_attributes", th.ObjectType(
            th.Property("base_customer_balance_amount", th.NumberType),
            th.Property("customer_balance_amount", th.NumberType),
            th.Property("base_gift_cards_amount", th.NumberType),
            th.Property("gift_cards_amount", th.NumberType),
            th.Property("gw_base_price", th.StringType),
            th.Property("gw_price", th.StringType),
            th.Property("gw_items_base_price", th.StringType),
            th.Property("gw_items_price", th.StringType),
            th.Property("gw_card_base_price", th.StringType),
            th.Property("gw_card_price", th.StringType),
            th.Property("gw_base_tax_amount", th.StringType),
            th.Property("gw_tax_amount", th.StringType),
            th.Property("gw_items_base_tax_amount", th.StringType),
            th.Property("gw_items_tax_amount", th.StringType),
            th.Property("gw_card_base_tax_amount", th.StringType),
            th.Property("gw_card_tax_amount", th.StringType),
        )),
    ).to_dict()
    
class CustomersStream(MagentoStream):
    name = "customers"
    path = "/V1/customers/search"
    primary_keys = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.items[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("group_id", th.IntegerType),
        th.Property("default_billing", th.StringType),
        th.Property("default_shipping", th.StringType),
        th.Property("confirmation", th.StringType),
        th.Property("created_at", th.StringType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("created_in", th.StringType),
        th.Property("dob", th.StringType),
        th.Property("email", th.StringType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("middlename", th.StringType),
        th.Property("prefix", th.StringType),
        th.Property("suffix", th.StringType),
        th.Property("gender", th.IntegerType),
        th.Property("store_id", th.IntegerType),
        th.Property("taxvat", th.StringType),
        th.Property("website_id", th.IntegerType),
        th.Property("disable_auto_group_change", th.IntegerType),

        th.Property("addresses", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("customer_id", th.IntegerType),
                th.Property("region", th.ObjectType()),  # region is an object
                th.Property("region_id", th.IntegerType),
                th.Property("country_id", th.StringType),
                th.Property("street", th.ArrayType(th.StringType)),
                th.Property("company", th.StringType),
                th.Property("telephone", th.StringType),
                th.Property("fax", th.StringType),
                th.Property("postcode", th.StringType),
                th.Property("city", th.StringType),
                th.Property("firstname", th.StringType),
                th.Property("lastname", th.StringType),
                th.Property("middlename", th.StringType),
                th.Property("prefix", th.StringType),
                th.Property("suffix", th.StringType),
                th.Property("vat_id", th.StringType),
                th.Property("default_shipping", th.BooleanType),
                th.Property("default_billing", th.BooleanType),
                th.Property("extension_attributes", th.ObjectType()),
                th.Property("custom_attributes", th.ArrayType(th.ObjectType()))
            )
        )),

        th.Property("extension_attributes", th.ObjectType(
            th.Property("company_attributes", th.ObjectType()),
            th.Property("is_subscribed", th.BooleanType),
            th.Property("assistance_allowed", th.IntegerType)
        )),

        th.Property("custom_attributes", th.ArrayType(
            th.ObjectType(
                th.Property("attribute_code", th.StringType),
                th.Property("value", th.StringType)
            )
        ))
    ).to_dict()
