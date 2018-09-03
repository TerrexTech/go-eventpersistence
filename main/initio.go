package main

import (
	"log"
	"os"
	"time"

	"github.com/pkg/errors"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"

	"github.com/TerrexTech/go-commonutils/utils"
	"github.com/TerrexTech/go-eventpersistence/persistence"
)

// Creates a KafkaIO from KafkaAdapter based on set environment variables.
func initKafkaIO() (*persistence.KafkaIO, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	consumerGroupName := os.Getenv("KAFKA_CONSUMER_GROUP")
	consumerTopics := os.Getenv("KAFKA_CONSUMER_TOPICS")
	offsetRetentionDuration := os.Getenv("KAFKA_OFFSET_RETENTION_HOURS")
	responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

	dur, err := time.ParseDuration(offsetRetentionDuration)
	if err != nil {
		err = errors.Wrap(err, "Error Parsing Duration for Kafka Offset-Retention")
		log.Fatalln(err)
	}

	kafkaAdapter := &persistence.KafkaAdapter{
		Brokers:                 *utils.ParseHosts(brokers),
		ConsumerGroupName:       consumerGroupName,
		ConsumerTopics:          *utils.ParseHosts(consumerTopics),
		OffsetRetentionDuration: dur,
		ResponseTopic:           responseTopic,
	}

	return kafkaAdapter.InitIO()
}

// Creates new CassandraIO using CassandraAdapter.
func initCassandraIO() (*persistence.CassandraIO, error) {
	hosts := os.Getenv("CASSANDRA_HOSTS")
	dataCenters := os.Getenv("CASSANDRA_DATA_CENTERS")
	username := os.Getenv("CASSANDRA_USERNAME")
	password := os.Getenv("CASSANDRA_PASSWORD")
	keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
	tableName := os.Getenv("CASSANDRA_TABLE")

	tableDef := map[string]csndra.TableColumn{
		"action": csndra.TableColumn{
			Name:            "action",
			DataType:        "text",
			PrimaryKeyIndex: "3",
		},
		"aggregateID": csndra.TableColumn{
			Name:            "aggregate_id",
			DataType:        "int",
			PrimaryKeyIndex: "1",
		},
		"data": csndra.TableColumn{
			Name:     "data",
			DataType: "text",
		},
		"timestamp": csndra.TableColumn{
			Name:            "timestamp",
			DataType:        "timestamp",
			PrimaryKeyIndex: "4",
			PrimaryKeyOrder: "DESC",
		},
		"userID": csndra.TableColumn{
			Name:     "user_id",
			DataType: "int",
		},
		"uuid": csndra.TableColumn{
			Name:            "uuid",
			DataType:        "uuid",
			PrimaryKeyIndex: "5",
		},
		"version": csndra.TableColumn{
			Name:            "version",
			DataType:        "int",
			PrimaryKeyIndex: "2",
			PrimaryKeyOrder: "DESC",
		},
		"yearBucket": csndra.TableColumn{
			Name:            "year_bucket",
			DataType:        "smallint",
			PrimaryKeyIndex: "0",
		},
	}

	cassandraAdapter := &persistence.CassandraAdapter{
		DataCenters: *utils.ParseHosts(dataCenters),
		Hosts:       *utils.ParseHosts(hosts),
		Keyspace:    keyspaceName,
		Password:    password,
		Table:       tableName,
		Username:    username,
		TableDef:    tableDef,
	}

	return cassandraAdapter.InitIO()
}
