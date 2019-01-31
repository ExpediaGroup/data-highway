# Test Drive

`test-drive` is a memory implementation of Data Highway. It exposes all the endpoints of
`paver`, `onramp` and `offramp`. It also exposes a few extra convenience endpoints for viewing all the messages for any given messages and deleting roads, messages and commits.

## Swagger

When `test-drive` is running, the Swagger documentation for all endpoints will be available at
`http://test-drive/swagger-ui.html`.

## Test Drive Endpoints

| Endpoint                                                              | Description
|---                                                                    |---
| `GET /testdrive/v1/roads/{roadName}/messages`                         | Get all messages for the given road.
| `DELETE /testdrive/v1/roads`                                          | Delete everything for all roads.
| `DELETE /testdrive/v1/roads/{roadName}`                               | Delete everything for the given road.
| `DELETE /testdrive/v1/roads/{roadName}/messages`                      | Delete all messages (and all commits for all streams) associated with the given road.
| `DELETE /testdrive/v1/roads/{roadName}/streams/{streamName}/messages` | Delete all commits for all streams associated with the given road.

## Running Test Drive

### Docker

```
docker run -p 8080:8080 ${repository}/road/test-drive:latest
```

### In a unit test
```
    private URI baseUri;
    private ConfigurableApplicationContext context;

    @Before
    public void before() throws Exception {
      int port;
      try (ServerSocket socket = new ServerSocket(0)) {
        port = socket.getLocalPort();
      }
      baseUri = URI.create("http://localhost:" + port);
      String[] args = new String[] { "--server.port=" + port };
      context = SpringApplication.run(TestDriveApp.class, args);
    }
  
    @After
    public void after() throws Exception {
      if (context != null) {
        context.close();
      }
    }
```

### TLS Configuration

`test-drive` exposes an HTTPS port using the following certificate: [server.crt](src/main/resources/server.crt).

In order to consume the HTTPS services, you can trust this certificate or directly import the corresponding [truststore](src/main/resources/truststore.jks).

This certificate is configured on two different DNS names:
* `localhost`
* `test-drive`

The latter allows a developer to consume the HTTPS services if the project is deployed remotely without having to configure a custom hostname verifier.
In this case, you can simply add a DNS entry to map `test-drive` to the remote endpoint.