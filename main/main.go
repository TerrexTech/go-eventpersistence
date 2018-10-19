package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-kafkautils/kafka"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

// validateEnv checks if all required environment-variables are set.
func validateEnv() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"CASSANDRA_HOSTS",
		"CASSANDRA_DATA_CENTERS",
		"CASSANDRA_KEYSPACE",
		"CASSANDRA_EVENT_TABLE",
		"CASSANDRA_EVENT_META_TABLE",
		"CASSANDRA_EVENT_META_PARTITION_KEY",

		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
		"KAFKA_CONSUMER_TOPICS",
		"KAFKA_RESPONSE_TOPIC",

		"VALID_EVENT_ACTIONS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}

}

func main() {
	validateEnv()

	// ======> Setup Kafka
	brokersStr := os.Getenv("KAFKA_BROKERS")
	brokers := *commonutil.ParseHosts(brokersStr)

	consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP")
	eventTopicStr := os.Getenv("KAFKA_CONSUMER_TOPICS")
	eventTopic := *commonutil.ParseHosts(eventTopicStr)
	// Event Consumer
	eventConsumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    consumerGroup,
		KafkaBrokers: brokers,
		Topics:       eventTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Event-Consumer")
		log.Fatalln(err)
	}

	// Handle ConsumerErrors
	go func() {
		for err := range eventConsumer.Errors() {
			err = errors.Wrap(err, "Kafka Consumer Error")
			log.Println(
				"Error in event consumer. " +
					"The events cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			)
			log.Fatalln(err)
		}
	}()

	// Response Producer
	responseProducer, err := kafka.NewProducer(&kafka.ProducerConfig{
		KafkaBrokers: brokers,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Response-Producer")
		log.Fatalln(err)
	}

	// Handle ProducerErrors
	go func() {
		for prodErr := range responseProducer.Errors() {
			err = errors.Wrap(prodErr.Err, "Kafka Producer Error")
			log.Println(
				"Error in response producer. " +
					"The responses cannot be produced without a working Kafka Producer. " +
					"The service will now exit.",
			)
			log.Fatalln(err)
		}
	}()

	// ======> Create/Load Cassandra Keyspace/Tables
	log.Println("Bootstrapping Event table")
	eventTable, err := bootstrap.Event()
	if err != nil {
		err = errors.Wrap(err, "EventTable: Error Creating Table in Cassandra")
		log.Fatalln(err)
	}
	log.Println("Bootstrapping EventMeta table")
	eventMetaTable, err := bootstrap.EventMeta()
	if err != nil {
		err = errors.Wrap(err, "EventMetaTable: Error Creating Table in Cassandra")
		log.Fatalln(err)
	}

	// ======> Setup EventStore
	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	metaPartitionKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		log.Fatalln(err)
	}

	eventStore, err := NewEventStore(eventTable, eventMetaTable, int8(metaPartitionKey))
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventStore")
		log.Fatalln(err)
	}

	// ======> Setup EventHandler
	validActionsStr := os.Getenv("VALID_EVENT_ACTIONS")
	validActions := *commonutil.ParseHosts(validActionsStr)
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		log.Fatalln(err)
	}
	responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

	handler, err := NewEventHandler(EventHandlerConfig{
		EventStore:       eventStore,
		ResponseProducer: responseProducer,
		ResponseTopic:    responseTopic,
		ValidActions:     validActions,
	})
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventHandler")
		log.Fatalln(err)
	}

	log.Println("Event-Persistence Service Initialized")

	err = eventConsumer.Consume(context.Background(), handler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to consume Events")
		log.Fatalln(err)
	}
}
