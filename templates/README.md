# tap-magento Configuration

This document describes the configuration options for the tap-magento Singer tap, which extracts data from Magento 2 REST APIs.

---

## Store & Connection

#### `store_url` (string, required)
The base URL of your Magento store. Used to build the REST API base URL.
- **Example**: `"https://your-store.magento.cloud"` or `"https://your-store.com"`

---

## Authentication

You can authenticate either with **username/password** (admin token), **access token**, or **OAuth 1.0**.

### Username / password (admin token)

#### `username` (string, optional)
Admin API username. When set together with `password`, the tap obtains an admin token for each request.
- **Example**: `"api_user"`

#### `password` (string, optional)
Admin API password.
- **Example**: `"xxxxxxxxxxxxxxxxx"`

### Bearer / access token

#### `access_token` (string, optional)
Static bearer token to authenticate against the Magento REST API. Used when `username`/`password` are not provided.
- **Example**: `"xxxxxxxxxxxxxxxxx"`

### OAuth 1.0

#### `use_oauth` (boolean, optional)
When `true`, authentication uses OAuth 1.0 (consumer key/secret and token/secret) instead of bearer or admin token.
- **Default**: `false`
- **Example**: `true`

#### `consumer_key` (string, optional)
OAuth 1.0 consumer key. Required when `use_oauth` is `true`.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `consumer_secret` (string, optional)
OAuth 1.0 consumer secret. Required when `use_oauth` is `true`.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `oauth_token` (string, optional)
OAuth 1.0 access token. Used with `use_oauth`; falls back to `access_token` if not set.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `oauth_token_secret` (string, optional)
OAuth 1.0 access token secret. Falls back to `access_token_secret` if not set.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `access_token_secret` (string, optional)
Alternative to `oauth_token_secret` for OAuth 1.0.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

---

## Date range

#### `start_date` (string, optional)
The earliest record date to sync. ISO 8601 datetime; used for incremental syncs on streams with a replication key.
- **Example**: `"2020-01-01T00:00:00Z"`

#### `end_date` (string, optional)
The latest record date to sync. ISO 8601 datetime; records after this date are excluded.
- **Example**: `"2025-12-31T23:59:59Z"`

---

## Store filtering

#### `store_id` (string or array, optional)
Comma-separated store IDs or store codes to sync. When empty or omitted, all stores are synced. Can be numeric IDs or store codes.
- **Example**: `"1,2,3"` or `"default,en"`

#### `fetch_all_stores` (boolean, optional)
When `true`, syncs data for all stores and applies store-level filtering where supported.
- **Default**: `false`
- **Example**: `true`

---

## Pagination & performance

#### `page_size` (number, optional)
Number of records per API page for most streams.
- **Default**: `300`
- **Example**: `300` or `500`

#### `source_items_page_size` (number, optional)
Page size for the source_items (inventory) stream.
- **Default**: `2000`
- **Example**: `2000`

#### `crawl_delay` (number, optional)
Delay in seconds to wait after each API response. Can help avoid rate limits.
- **Default**: `0`
- **Example**: `1`

---

## Stream filtering & behavior

#### `order_ids` (array, optional)
If set, only orders with these entity IDs are synced. Empty or omitted means all orders (subject to date/store filters).
- **Example**: `[10001, 10002]` or `[]`

#### `use_inventory_source_items` (boolean, optional)
When `false`, the source_items stream is not synced.
- **Default**: `true`
- **Example**: `true`

#### `use_item_stock` (boolean, optional)
When `false`, the product_item_stocks stream is not synced.
- **Default**: `true`
- **Example**: `true`

---

## HTTP & custom options

#### `user_agent` (string, optional)
Custom User-Agent string sent in HTTP requests.
- **Example**: `"tap-magento/1.0.0"`

#### `custom_cookies` (object, optional)
Key-value map of cookie names and values to send with every request.
- **Example**: `{"cookie_name": "cookie_value"}`

---

## Example: Minimal config (required only)

Uses store URL and static access token:

```json
{
  "store_url": "https://your-store.magento.cloud",
  "access_token": "xxxxxxxxxxxxxxxxx"
}
```

Or with admin credentials:

```json
{
  "store_url": "https://your-store.magento.cloud",
  "username": "api_user",
  "password": "xxxxxxxxxxxxxxxxx"
}
```

---

## Example: More complete config

Includes date range, store filtering, pagination, and optional HTTP settings:

```json
{
  "store_url": "https://your-store.magento.cloud",
  "username": "api_user",
  "password": "xxxxxxxxxxxxxxxxx",
  "start_date": "2020-01-01T00:00:00Z",
  "end_date": "2025-12-31T23:59:59Z",
  "store_id": "1,2",
  "fetch_all_stores": false,
  "page_size": 300,
  "source_items_page_size": 2000,
  "crawl_delay": 0,
  "user_agent": "tap-magento/1.0.0",
  "use_inventory_source_items": true,
  "use_item_stock": true
}
```
