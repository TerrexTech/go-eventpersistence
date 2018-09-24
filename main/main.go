package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

var metaParitionKey int8

func initService() {
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
		"KAFKA_OFFSET_RETENTION_HOURS",
		"KAFKA_RESPONSE_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}

	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	pKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		log.Fatalln(err)
	}
	metaParitionKey = int8(pKey)
}

// Creates a KafkaIO from KafkaAdapter based on set environment variables.
func initKafkaIO() (*KafkaIO, error) {
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

	kafkaAdapter := &KafkaAdapter{
		Brokers:                 *commonutil.ParseHosts(brokers),
		ConsumerGroupName:       consumerGroupName,
		ConsumerTopics:          *commonutil.ParseHosts(consumerTopics),
		OffsetRetentionDuration: dur,
		ResponseTopic:           responseTopic,
	}

	return kafkaAdapter.InitIO()
}

func main() {
	initService()
	kafkaIO, err := initKafkaIO()
	if err != nil {
		err = errors.Wrap(err, "Error in KafkaIO Init")
		log.Fatalln(err)
	}

	// Handle ConsumerErrors
	go func() {
		for consumerErr := range kafkaIO.ConsumerErrors() {
			err := errors.Wrap(consumerErr, "Kafka Consumer Error")
			log.Println(
				"Error in event consumer. " +
					"The events cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			)
			log.Panicln(err)
		}
	}()

	// Handle ProducerErrors
	go func() {
		for producerErr := range kafkaIO.ProducerErrors() {
			err := errors.Wrap(producerErr, "Kafka Producer Error")
			log.Println(
				"Error in response producer. " +
					"The responses cannot be produced without a working Kafka Producer. " +
					"The service will now exit.",
			)
			// Healthy Response producer is essential for this service,
			// else there will be no responses for Kafka events
			log.Panicln(err)
		}
	}()

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

	log.Println("Event-Persistence Service Initialized")

	// Process Events
	for eventMsg := range kafkaIO.ConsumerMessages() {
		go processEvent(metaParitionKey, kafkaIO, eventTable, eventMetaTable, eventMsg)
	}
}
