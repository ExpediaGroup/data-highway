#! /bin/sh

set -e

BASE_URL=/swagger
SERVICE_URLS='[{"name":"onramp","url":"/swagger/onramp"},{"name":"paver","url":"/swagger/paver"}]'
NGINX_ROOT=/usr/share/nginx/html
INDEX_FILE=$NGINX_ROOT/index.html

sed -i "s|location / {|location $BASE_URL {|g" /etc/nginx/nginx.conf
sed -i "s|url: .*,|urls: $SERVICE_URLS,\nvalidatorUrl: null,|g" $INDEX_FILE

exec nginx -g 'daemon off;'
