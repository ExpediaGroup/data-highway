FROM swaggerapi/swagger-ui:v3.13.6

LABEL maintainer="data-highway@expedia.com"

RUN apk add --update jq
RUN mkdir -p /etc/swagger

COPY nginx.conf /etc/nginx/
COPY docker-run.sh /usr/share/nginx/

CMD ["sh", "/usr/share/nginx/docker-run.sh"]

EXPOSE 8080
