#! /bin/sh

set -e

apk add --update jq

BASE_URL=${BASE_URL:-/}
NGINX_ROOT=/usr/share/nginx/html
INDEX_FILE=$NGINX_ROOT/index.html
SWAGGER_CONF=/etc/config/swagger-conf.json

sed -i "s|location / {|location $BASE_URL {|g" /etc/nginx/nginx.conf

if [ -r $SWAGGER_CONF ]; then
  API_URLS=$(jq -c '[ .[] | { name, url } ]' < $SWAGGER_CONF)
  sed -i "s|url: .*,|urls: $API_URLS,\nvalidatorUrl: null,|g" $INDEX_FILE

  jq -r 'map("location \(.url) {\n  proxy_pass \(.proxy);\n}") | .[]' \
    < $SWAGGER_CONF \
    > /etc/nginx/proxy.conf
fi

exec nginx -g 'daemon off;'
