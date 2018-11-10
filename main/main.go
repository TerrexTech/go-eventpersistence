package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-kafkautils/kafka"
	tlog "github.com/TerrexTech/go-logtransport/log"
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
		"SERVICE_NAME",
		"KAFKA_LOG_PRODUCER_TOPIC",

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

	logTopic := os.Getenv("KAFKA_LOG_PRODUCER_TOPIC")
	serviceName := os.Getenv("SERVICE_NAME")
	prodConfig := &kafka.ProducerConfig{
		KafkaBrokers: brokers,
	}
	logger, err := tlog.Init(nil, serviceName, prodConfig, logTopic)
	if err != nil {
		err = errors.Wrap(err, "WARNING: Error creating log-producer")
		log.Fatalln(err)
	}

	// Event Consumer
	eventConsumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		GroupName:    consumerGroup,
		KafkaBrokers: brokers,
		Topics:       eventTopic,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating Kafka Event-Consumer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// Handle ConsumerErrors
	go func() {
		for err := range eventConsumer.Errors() {
			err = errors.Wrap(err, "Kafka Consumer Error")
			logger.I(tlog.Entry{
				Description: "Error in event consumer. " +
					"The events cannot be consumed without a working Kafka Consumer. " +
					"The service will now exit.",
			})
			logger.F(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   1,
			})
		}
	}()

	// Response Producer
	responseProducer, err := kafka.NewProducer(prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating Response-Producer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// Handle ProducerErrors
	go func() {
		for prodErr := range responseProducer.Errors() {
			err = errors.Wrap(prodErr.Err, "Response-Producer Error")
			logger.E(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   1,
			})
		}
	}()

	// ======> Create/Load Cassandra Keyspace/Tables
	log.Println("Bootstrapping Event table")
	eventTable, err := bootstrap.Event()
	if err != nil {
		err = errors.Wrap(err, "EventTable: Error getting Table from Cassandra")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}
	log.Println("Bootstrapping EventMeta table")
	eventMetaTable, err := bootstrap.EventMeta()
	if err != nil {
		err = errors.Wrap(err, "EventMetaTable: Error getting Table from Cassandra")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// ======> Setup EventStore
	pKeyVar := "CASSANDRA_EVENT_META_PARTITION_KEY"
	metaPartitionKey, err := strconv.Atoi(os.Getenv(pKeyVar))
	if err != nil {
		err = errors.Wrap(err, pKeyVar+" must be a valid integer")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	eventStore, err := NewEventStore(eventTable, eventMetaTable, int8(metaPartitionKey))
	if err != nil {
		err = errors.Wrap(err, "EventStore: Error while initializing EventStore")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	// ======> Setup EventHandler
	validActionsStr := os.Getenv("VALID_EVENT_ACTIONS")
	validActions := *commonutil.ParseHosts(validActionsStr)
	responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

	handler, err := NewEventHandler(EventHandlerConfig{
		EventStore:       eventStore,
		Logger:           logger,
		ResponseProducer: responseProducer,
		ResponseTopic:    responseTopic,
		ValidActions:     validActions,
	})
	if err != nil {
		err = errors.Wrap(err, "Error while initializing EventHandler")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	logger.I(tlog.Entry{
		Description: "Event-Persistence Service Initialized",
	})

	err = eventConsumer.Consume(context.Background(), handler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to consume Events")
		logger.F(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}
}
