package main

import (
	"log"

	"github.com/TerrexTech/go-eventstore-models/models"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func main() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	kafkaIO, err := initKafkaIO()
	if err != nil {
		err = errors.Wrap(err, "Error in KafkaIO Init")
		log.Fatalln(err)
	}

	cassandraIO, err := initCassandraIO()
	if err != nil {
		err = errors.Wrap(err, "Error Creating Cassandra Session")
		log.Fatalln(err)
	}

	// Handle ConsumerErrors
	go func() {
		for consumerErr := range kafkaIO.ConsumerErrors() {
			err := errors.Wrap(consumerErr, "Kafka Consumer Error")
			kr := &models.KafkaResponse{
				Error: err.Error(),
			}
			kafkaIO.ProducerInput() <- kr
			log.Fatalln(err)
		}
	}()

	// Handle ProducerErrors
	go func() {
		for producerErr := range kafkaIO.ProducerErrors() {
			err := errors.Wrap(producerErr, "Kafka Producer Error")
			log.Println(
				"The responses cannot be produced without a working Kafka Producer. " +
					"The service will now exit.",
			)
			// Healthy Response producer is essential for this service,
			// else there will be no responses for Kafka events
			log.Panicln(err)
		}
	}()

	log.Println("Event-Persistence Service Initialized")

	// Process Events
	for eventMsg := range kafkaIO.ConsumerMessages() {
		go processEvent(kafkaIO, cassandraIO, eventMsg)
	}
}
