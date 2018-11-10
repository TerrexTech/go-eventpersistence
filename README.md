EventPersistence Package
-----

This service handles persisting/storing the system-generated events into Cassandra event-store.

### How this Works:

* The event is consumed via Kafka topic (`event.rns_eventstore.events`).

* This event is then processed, which includes Marshalling it into [Event][0] model and inserting it into Cassandra Event-Store.

* The post-processing result is published to a Kafka Topic (`event.persistence.response.<event-action>.<aggregate-id>`), which is based on [Kafka-Response][1] model. The event-producer must consume this result (by listening on the respective Kafka-Topic), check for errors, and proceed accordingly.

Check [.env][2] and [docker-compose.yaml][3] (docker-compose is only used in tests as of yet) files for default configurations (including the Cassandra Keyspace/Table used).

  [0]: https://github.com/TerrexTech/go-eventstore-models/blob/master/models/event.go
  [1]: https://github.com/TerrexTech/go-eventstore-models/blob/master/models/kafka_response.go
  [2]: https://github.com/TerrexTech/go-eventpersistence/blob/master/.env
  [3]: https://github.com/TerrexTech/go-eventpersistence/blob/master/test/docker-compose.yaml
