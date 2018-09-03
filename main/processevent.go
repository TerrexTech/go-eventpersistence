package main

import (
	"bytes"
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventpersistence/persistence"
	"github.com/TerrexTech/go-eventstore-models/models"
	"github.com/pkg/errors"
)

func decodeEvent(eventMsg *sarama.ConsumerMessage) (*models.Event, error) {
	// Unmarshal Event to Event-model struct.
	// We Disallow unknown fields to prevent unexpected data/behaviour
	reader := bytes.NewReader(eventMsg.Value)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	event := &models.Event{}
	err := decoder.Decode(event)
	return event, err
}

func processEvent(
	kafkaIO *persistence.KafkaIO,
	cassandraIO *persistence.CassandraIO,
	eventMsg *sarama.ConsumerMessage,
) {
	// Decode Event and Insert to Event-Store
	event, err := decodeEvent(eventMsg)
	if err != nil {
		// Kafka-Response informing that the Unmarshalling was unsuccessful.
		err = errors.Wrap(err, "Error Unmarshalling Event-string to Event-struct")
		kr := &models.KafkaResponse{
			Input: string(eventMsg.Value),
			Error: err.Error(),
		}
		log.Println(err)

		kafkaIO.MarkOffset() <- eventMsg
		kafkaIO.ProducerInput() <- kr
		return
	}

	err = <-cassandraIO.Insert(event)

	// Create KafkaResponse from processing of consumed event
	errStr := ""
	if err != nil {
		err = errors.Wrap(err, "Error Inserting Event into Cassandra")
		log.Println(err)
		log.Printf("%+v", event)
		errStr = err.Error()
	} else {
		// MarkOffset to be committed if the insert operation is successful
		kafkaIO.MarkOffset() <- eventMsg
	}
	kr := &models.KafkaResponse{
		Input: string(eventMsg.Value),
		Error: errStr,
	}
	kafkaIO.ProducerInput() <- kr
}
