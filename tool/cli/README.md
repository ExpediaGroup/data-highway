# CLI tools for Data Highway

## Build
```bash
mvn clean package -pl com.hotels.road:road-tool-cli -am -Djib.skip -TC1
```

## Run

```bash
./cli/bin/data-highway-console-offramp.sh \
    --host="dh_cluster" \
    --username="dh_user_name" \
    --password="dh_user_password" \
    --roadName="dh_road_name" \
    --streamName="dh_road_stream" \
    --defaultOffset="LATEST" \
    --tlsTrustAll="true"
```

Print help:
```bash
./cli/bin/data-highway-console-offramp.sh --help
```

## Shell autocompletion
Auto completion script is created in maven's package phase in build directory.
User can install auto completion in shell by sourcing the script as described in
[picocli.info docs](https://picocli.info/autocomplete.html#_install_completion_script),
or install it permanently in 
[bash](https://picocli.info/autocomplete.html#_installing_completion_scripts_permanently_in_bash)
or [zsh](https://picocli.info/autocomplete.html#_installing_completion_scripts_permanently_in_zsh).
