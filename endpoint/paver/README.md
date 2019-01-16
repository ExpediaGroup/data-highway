# ROAD Paver

Paver exposes a REST API for managing roads and their schemas.

Road configuration is stored in a Kafka topic defined by the `kafka.road.topic` option. This topic must be a [compacted
topic](https://kafka.apache.org/documentation/#compaction).

For all read operations like retrieving road configuration (`GET /roads/{name}`) Paver queries the road model directly
and renders a JSON response to the calling client.

All write operations like adding a road (`POST /roads`) that require a change to the model are sent onto the road
modification topic defined by the `kafka.road.modification.topic` option as [json-patch](http://jsonpatch.com/)
updates. Every patch consists of a list of patch operations and it only ever affects one road.

The patches are applied to the internal road model and actual changes to the system like creating a topic for a new
road are handled by road agents.

Once a patch has been processed, the success or failure of an operation and the current state of the road can be
determined by querying Paver's REST API.

Please note: Paver write operations are asynchronous. For example, once a request to create a road is issued, the road
should be created within seconds but it is not guaranteed to happen before the response is returned to the client.


