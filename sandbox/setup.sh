#!/bin/sh
set -eu

if [ ! -f composer.json ]; then
  composer create-project --repository-url=https://mirror.mage-os.org/ \
    --no-install --no-interaction magento/project-community-edition:2.4.8-p5 /app
fi

# Also finishes dependency installation if the first run was interrupted.
composer install --no-dev --no-interaction

if [ ! -f app/etc/sandbox-password ]; then
  php -r 'file_put_contents("app/etc/sandbox-password", bin2hex(random_bytes(16)) . "Aa1!");'
fi
ADMIN_PASSWORD=$(cat app/etc/sandbox-password)

# PHP handles these variables; prevent shell expansion.
# shellcheck disable=SC2016
if ! php -r '
  $config = is_file("app/etc/env.php") ? require "app/etc/env.php" : [];
  exit(empty($config["install"]["date"]) ? 1 : 0);
'; then
  # env.php is written before schema/data/admin setup. Without install/date,
  # rerun the installer to finish those steps; do not use --cleanup-database.
  # Local sandbox only: allow setup to request an admin token.
  php bin/magento setup:install \
    --base-url=http://localhost:8081/ \
    --db-host=db --db-name=magento --db-user=magento --db-password=magento \
    --backend-frontname=admin \
    --admin-firstname=Sandbox --admin-lastname=Admin \
    --admin-email=admin@example.com --admin-user=admin \
    --admin-password="$ADMIN_PASSWORD" \
    --language=en_US --currency=USD --timezone=UTC --use-rewrites=1 \
    --disable-modules=Magento_AdminAdobeImsTwoFactorAuth,Magento_TwoFactorAuth \
    --search-engine=opensearch --opensearch-host=opensearch --opensearch-port=9200
fi

php bin/magento deploy:mode:set developer

php <<'PHP'
<?php
require '/app/app/bootstrap.php';

$bootstrap = \Magento\Framework\App\Bootstrap::create(BP, $_SERVER);
$objects = $bootstrap->getObjectManager();
$objects->get(\Magento\Framework\App\State::class)->setAreaCode('adminhtml');
$store = $objects->get(\Magento\Store\Model\StoreManagerInterface::class)->getStore('default');
$config = $objects->get(\Magento\Framework\App\Config\Storage\WriterInterface::class);
$config->save('payment/checkmo/active', 1);
$config->save('carriers/flatrate/active', 1);
$objects->get(\Magento\Framework\App\Config\ReinitableConfigInterface::class)->reinit();

// Stable SKUs, emails, and order numbers prevent duplicates on setup reruns.
$products = $objects->get(\Magento\Catalog\Api\ProductRepositoryInterface::class);
$productFixtures = [
    ['sandbox-shirt', 'Sandbox T-Shirt', 25.00],
    ['sandbox-mug', 'Sandbox Mug', 12.50],
    ['sandbox-notebook', 'Sandbox Notebook', 8.00],
];
foreach ($productFixtures as [$sku, $name, $price]) {
    try {
        $products->get($sku);
    } catch (\Magento\Framework\Exception\NoSuchEntityException $e) {
        $product = $objects->create(\Magento\Catalog\Model\Product::class);
        $product->setSku($sku)->setName($name)->setTypeId('simple')
            ->setAttributeSetId($product->getDefaultAttributeSetId())
            ->setPrice($price)->setWeight(1)->setStatus(1)->setVisibility(4)
            ->setWebsiteIds([(int) $store->getWebsiteId()])
            ->setStockData(['use_config_manage_stock' => 1, 'qty' => 100, 'is_in_stock' => 1]);
        $products->save($product);
    }
}

// Checkout needs the new products indexed before creating orders.
$indexers = $objects->get(\Magento\Indexer\Model\Indexer\CollectionFactory::class)->create();
foreach ($indexers as $indexer) {
    $indexer->reindexAll();
}

$customers = $objects->get(\Magento\Customer\Api\CustomerRepositoryInterface::class);
$customerFixtures = [
    ['alex.sandbox@example.com', 'Alex', 'Sandbox'],
    ['sam.sandbox@example.com', 'Sam', 'Sandbox'],
];
foreach ($customerFixtures as $index => [$email, $first, $last]) {
    $addressData = [
        'firstname' => $first,
        'lastname' => $last,
        'street' => ['10 Sandbox Street'],
        'city' => 'London',
        'country_id' => 'GB',
        'postcode' => 'SW1A 1AA',
        'telephone' => '02079460000',
    ];
    try {
        $customer = $customers->get($email, (int) $store->getWebsiteId());
    } catch (\Magento\Framework\Exception\NoSuchEntityException $e) {
        $customer = $objects->create(\Magento\Customer\Api\Data\CustomerInterface::class);
        $customer->setEmail($email)->setFirstname($first)->setLastname($last)
            ->setWebsiteId((int) $store->getWebsiteId())->setStoreId((int) $store->getId());
        $address = $objects->create(\Magento\Customer\Api\Data\AddressInterface::class, [
            'data' => $addressData,
        ]);
        $address->setIsDefaultBilling(true)->setIsDefaultShipping(true);
        $customer->setAddresses([$address]);
        $customer = $customers->save($customer);
    }

    $orders = $objects->create(\Magento\Sales\Model\ResourceModel\Order\Collection::class)
        ->addFieldToFilter('increment_id', 'sandbox-' . ($index + 1));
    $order = $orders->getFirstItem();
    if (!$order->getId()) {
        $quote = $objects->create(\Magento\Quote\Model\Quote::class);
        $quote->setStore($store)->assignCustomer($customer);
        $quote->addProduct($products->get($productFixtures[$index][0]), $index + 1);
        $quote->getBillingAddress()->addData($addressData);
        $quote->getShippingAddress()->addData($addressData)
            ->setCollectShippingRates(true)->collectShippingRates()
            ->setShippingMethod('flatrate_flatrate');
        $quote->getPayment()->setQuote($quote)->importData(['method' => 'checkmo']);
        $quote->setReservedOrderId('sandbox-' . ($index + 1))->collectTotals();
        $objects->get(\Magento\Quote\Api\CartRepositoryInterface::class)->save($quote);
        $order = $objects->get(\Magento\Quote\Model\QuoteManagement::class)->submit($quote);
    }
    if ($index === 0 && $order->canInvoice()) {
        $objects->get(\Magento\Sales\Api\InvoiceOrderInterface::class)->execute((int) $order->getId());
    }
}

file_put_contents(
    '/setup/local-config.json',
    json_encode([
        'store_url' => 'http://localhost:8081',
        'username' => 'admin',
        'password' => file_get_contents('/app/app/etc/sandbox-password'),
        'start_date' => '2020-01-01T00:00:00Z',
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n"
);
chmod('/setup/local-config.json', 0600);
echo "Sample data ready: 3 products, 2 customers, 2 orders, 1 invoice.\n";
PHP

chown "$(stat -c '%u:%g' /setup)" /setup/local-config.json

php bin/magento cache:flush
chown -R application:application /app
