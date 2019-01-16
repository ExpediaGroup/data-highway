# Road Schema

This project provides functionality related to the management of [Avro](https://avro.apache.org/) schemas.

--

`SchemaCompatibility` is an `enum` that encapsulates all the compatibility modes that Avro supports:

| Mode                    | Description
|---                      |---
| `CAN_READ_ALL`          | The new schema can read Avro data that was written with all previous schemas in the chronology.
| `CAN_READ_LATEST`       | The new schema can read Avro data that was written with the latest schema in the chronology.
| `CAN_BE_READ_BY_ALL`    | All previous schemas in the chronology can read Avro data that was written by the new schema.
| `CAN_BE_READ_BY_LATEST` | The latest previous schema in the chronology can read Avro data that was written by the new schema.
| `MUTUAL_READ_ALL`       | A combination of `CAN_READ_ALL` and `CAN_BE_READ_BY_ALL`.
| `MUTUAL_READ_LATEST`    | A combination of `CAN_READ_LATEST` and `CAN_BE_READ_BY_LATEST`.

--

`SchemaChronology` is POJO for holding a `SchemaCompatibility` mode and the full chronology of schema versions.

--

`DataHighwaySchemaValidator` contains methods for validating schemas against criteria for use in Data Highway:

* The schema must be a `RECORD`.
* The schema is validated against [Jasvorno's](https://github.com/HotelsDotCom/jasvorno) `SchemaValidator` which does
  not allow variants of `union[bytes, string[, ...]]]`. This is because, when parsing a JSON document, it is impossible
  to distinguish between `bytes` and `string` values.

--

`SchemaSerializationModule`, `SchemaSerializer` and `SchemaDeserializer` for use with
[Jackson](https://github.com/FasterXML/jackson).
