# CLI tools for Data Highway

## Build
```bash
mvn clean package -pl com.hotels.road:road-tools-cli -am -Djib.skip -TC1
```

## Run

```bash
./tools/cli/bin/data-highway-console-offramp.sh \
    --dataHighwayHost="dh_cluster" \
    --username="dh_user_name" \
    --password="dh_user_password" \
    --roadName="dh_road_name" \
    --streamName="dh_road_stream" \
    --defaultOffset="LATEST" \
    --tlsTrust="ALL"
```
