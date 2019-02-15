# Data Highway

## Start using

![Maven Central](https://img.shields.io/maven-central/v/com.hotels.road/road-parent.svg)
![GitHub license](https://img.shields.io/github/license/HotelsDotCom/data-highway.svg)
![Build](https://travis-ci.org/HotelsDotCom/data-highway.svg?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/data-highway/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/data-highway?branch=master)

## Overview
### What is Data Highway?
The Data Highway is a service that allows data to be easily produced and consumed via JSON messages over HTTPS/WSS. Data is first defined using a schema and a "road" is created which will accept messages that conform to this schema. Producers of data sets thus only need to define the structure of their data and are then able to send their data to a REST endpoint and not be concerned with what happens next. Data Highway will ensure that this data is made available for streaming consumption and also stored reliably in a "data lake" in the cloud for access by end users.

#### Architecture
![Data Highway Architecture](data-highway-architecture.svg)

##### Paver
Paver is Data Highway's administration endpoint. It provides the following features:

* Road (Synonymous with Kafka topic) creation.
* Schema registration and (soft) deletion.
* Data-at-rest to Hive/S3 configuration.
* Road-level producer and consumer authorisation.

##### Onramp
Onramp is Data Highway's producer endpoint. It allows users to submit messages to roads in JSON format over HTTPS.

##### Offramp
Offramp is Data Highway's consumer endpoint. It allows users to consume message from roads in JSON format over  WSS.

##### Tollbooth
Tollbooth is the core of Data Highway. It provides the mechanism by which mutations to a road's model are persisted. Mutations can come from users (Paver) or internal agents. Anything wishing to make a mutation submit's a JSON Patch onto a deltas Kafka topic. Tollbooth consumes this topic, continuously applying patches to models and persisting them back onto the main Model (compact) topic.

##### Traffic Control
Traffic Control is the Kafka Agent. It is primarily responsible for managing Kafka topics in response to changes in models.

##### Loading Bay / Truck Park
Loading Bay is responsible for orchestrating the landing of data to S3 on a configured interval and managing Hive tables - creation, schema mutation and the addition of partitions.

## Try it out

Try [Test Drive](https://hub.docker.com/r/hotelsdotcom/road-test-drive/tags), an in-memory version of Data Highway that exposes all the public facing endpoints in a single Spring Boot application or Docker container.

```bash
docker run -p 8080:8080 hotelsdotcom/road-test-drive:<tag>
```

## Examples
Using a local instance of Test Drive, try creating road, registering a schema and producing and consuming messages using the build in user account `user:pass`.

Note: For the example below, cURL will prompt for a password which is `pass`.

### Create a road

```bash
curl -sk \
  -u user \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
  "name": "my_road", 
  "description": "My Road",
  "teamName": "TEAM", 
  "contactEmail": "team@company.com",
  "partitionPath": "$.foo",
  "enabled": true,
  "authorisation": {
    "onramp": {
      "cidrBlocks": ["0.0.0.0/0"],
      "authorities": ["*"]
    },
    "offramp": {
      "authorities": {
        "*": ["PUBLIC"]
      }
    }
  }
}' https://localhost:8080/paver/v1/roads
```

### Register a schema

```bash
curl -sk \
  -u user\
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
  "type" : "record",
  "name" : "my_record",
  "fields" : [
    {"name":"foo","type":"string"},
    {"name":"bar","type":"string"}
  ]
}' https://localhost:8080/paver/v1/roads/my_road/schemas
```

### Produce messages

```bash
curl -sk \
  -u user\
  -H "Content-Type: application/json" \
  -d '[{"foo":"foo1","bar":"bar1"}]' \
  https://localhost:8080/onramp/v1/roads/my_road/messages
```

### Consume messages

```bash
echo '{"type":"REQUEST","count":1}' |\
  websocat -nk wss://localhost:8080/offramp/v2/roads/my_road/streams/my_stream/messages?defaultOffset=EARLIEST
```

See: [websocat](https://github.com/vi/websocat)

## Building
Build and load docker images to the local docker daemon:

```bash
mvn clean package -Djib.goal=dockerBuild
```

Build without docker images:

```bash
mvn clean package -Djib.skip
```

Build and push docker images to a repo:

```bash
mvn clean package -Ddocker.repo=my.docker.repo
```

## Contributors
Special thanks to the following for making data-highway possible!

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore -->
| [<img src="https://avatars.githubusercontent.com/nahguam?s=100" width="100" alt="Dave Maughan" /><br /><sub><b>Dave Maughan</b></sub>](https://github.com/nahguam)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=nahguam "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation") | [<img src="https://avatars.githubusercontent.com/noddy76?s=100" width="100" alt="James Grant" /><br /><sub><b>James Grant</b></sub>](https://github.com/noddy76)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=noddy76 "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation") [📢](#talks "Talks") | [<img src="https://avatars.githubusercontent.com/teabot?s=100" width="100" alt="Elliot West" /><br /><sub><b>Elliot West</b></sub>](https://github.com/teabot)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=teabot "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation") [📢](#talks "Talks") | [<img src="https://avatars.githubusercontent.com/massdosage?s=100" width="100" alt="Adrian Woodhead" /><br /><sub><b>Adrian Woodhead</b></sub>](https://github.com/massdosage)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=massdosage "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation") | [<img src="https://avatars.githubusercontent.com/konrad7d?s=100" width="100" alt="Konrad Dowgird" /><br /><sub><b>Konrad Dowgird</b></sub>](https://github.com/konrad7d)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=konrad7d "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation")  | [<img src="https://avatars.githubusercontent.com/riccardofreixo?s=100" width="100" alt="Riccardo Freixo" /><br /><sub><b>Riccardo Freixo</b></sub>](https://github.com/riccardofreixo)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=riccardofreixo "Code") [🎨](#design "Design") [👀](#review "Reviewed Pull Requests") [📖](#documentation "Documentation") [🚇](#infrastructure "Infrastructure") | [<img src="https://avatars.githubusercontent.com/MonicaacinoM?s=100" width="100" alt="Monica Nicoara" /><br /><sub><b>Monica Nicoara</b></sub>](https://github.com/MonicaacinoM)<br />[🤔](#planning "Planning") [📋](#events "Events") |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |

| [<img src="https://avatars.githubusercontent.com/teivah?s=100" width="100" alt="Teiva Harsanyi" /><br /><sub><b>Teiva Harsanyi</b></sub>](https://github.com/teivah)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=teivah "Code") | [<img src="https://avatars.githubusercontent.com/kyrsideris?s=100" width="100" alt="Kryiakos Sideris" /><br /><sub><b>Kryiakos Sideris</b></sub>](https://github.com/kyrsideris)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=kyrsideris "Code") | [<img src="https://avatars.githubusercontent.com/SandeepSolanki?s=100" width="100" alt="Sandeep Solanki" /><br /><sub><b>Sandeep Solanki</b></sub>](https://github.com/SandeepSolanki)<br />[💻](https://github.com/HotelsDotCom/data-highway/commits?author=SandeepSolanki "Code") |
| :---: | :---: | :---: |
<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/kentcdodds/all-contributors) specification.

## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia Inc.
