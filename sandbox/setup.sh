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

if [ ! -f app/etc/env.php ]; then
  # Local sandbox only: allow the tap's username/password authentication.
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
php bin/magento indexer:reindex
php bin/magento cache:flush
chown -R application:application /app

php <<'PHP'
<?php
file_put_contents(
    '/setup/local-config.json',
    json_encode([
        'store_url' => 'http://localhost:8081',
        'username' => 'admin',
        'password' => file_get_contents('/app/app/etc/sandbox-password'),
        'start_date' => '2020-01-01T00:00:00Z',
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n"
);
PHP
