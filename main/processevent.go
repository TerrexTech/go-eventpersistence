package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/pkg/errors"
)

func decodeEvent(eventMsg *sarama.ConsumerMessage) (*model.Event, error) {
	// Unmarshal Event to Event-model struct.
	// We Disallow unknown fields to prevent unexpected data/behaviour
	reader := bytes.NewReader(eventMsg.Value)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	event := &model.Event{}
	err := decoder.Decode(event)
	return event, err
}

func getAggVersion(
	eventMetaTable *csndra.Table,
	yearBucket int16,
	aggregateID int8,
) (int64, error) {
	resultsBind := []model.EventMeta{}
	sp := csndra.SelectParams{
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator("year_bucket", yearBucket).Eq(),
			csndra.Comparator("aggregate_id", aggregateID).Eq(),
		},
		ResultsBind:   &resultsBind,
		SelectColumns: []string{"aggregate_version"},
	}
	_, err := eventMetaTable.Select(sp)
	if err != nil {
		err = errors.Wrap(err, "Error Fetching Latest Event-Version from EventMeta")
		return -1, err
	}

	if len(resultsBind) > 1 {
		return -1, errors.New(
			"EventsMeta: Received > 1 entries while fetching aggregate-version",
		)
	}
	if len(resultsBind) == 0 {
		return -1, fmt.Errorf("EventsMeta: No versions found for AggregateID: %d", aggregateID)
	}
	return resultsBind[0].AggregateVersion, nil
}

func processEvent(
	kafkaIO *KafkaIO,
	eventTable *csndra.Table,
	eventMetaTable *csndra.Table,
	eventMsg *sarama.ConsumerMessage,
) {
	// Decode Event and Insert to Event-Store
	event, err := decodeEvent(eventMsg)
	if err != nil {
		// Kafka-Response informing that the Unmarshalling was unsuccessful.
		err = errors.Wrap(err, "Error Unmarshalling Event-string to Event-struct")
		kr := &model.KafkaResponse{
			Input: string(eventMsg.Value),
			Error: err.Error(),
		}
		log.Println(err)

		kafkaIO.MarkOffset() <- eventMsg
		kafkaIO.ProducerInput() <- kr
		return
	}

	aggVersion, err := getAggVersion(eventMetaTable, 2018, event.AggregateID)
	if err != nil {
		err = errors.Wrapf(
			err,
			"EventMeta: Error Getting Event-Version to use for Aggregate ID: %d",
			event.AggregateID,
		)
		log.Println(err)
		return
	}
	event.Version = aggVersion
	err = <-eventTable.AsyncInsert(event)

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
	kr := &model.KafkaResponse{
		Input: string(eventMsg.Value),
		Error: errStr,
	}
	kafkaIO.ProducerInput() <- kr
}
