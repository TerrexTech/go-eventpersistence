package main

import (
	"bytes"
	"encoding/json"
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
	partitionKey int8,
	aggregateID int8,
) (int64, error) {
	resultsBind := []model.EventMeta{}
	partitionKeyCol, err := eventMetaTable.Column("partitionKey")
	if err != nil {
		return -1, errors.New(`Expected column "partitionKey" not found`)
	}
	aggregateIDCol, err := eventMetaTable.Column("aggregateID")
	if err != nil {
		return -1, errors.New(`Expected column "aggregateID" not found`)
	}
	sp := csndra.SelectParams{
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(partitionKeyCol, partitionKey).Eq(),
			csndra.Comparator(aggregateIDCol, aggregateID).Eq(),
		},
		ResultsBind:   &resultsBind,
		SelectColumns: []string{"aggregate_version"},
	}
	_, err = eventMetaTable.Select(sp)
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
		meta := model.EventMeta{
			AggregateVersion: 0,
			AggregateID:      aggregateID,
			PartitionKey:     partitionKey,
		}
		err = <-eventMetaTable.AsyncInsert(&meta)
		if err != nil {
			return -1, err
		}
		return meta.AggregateVersion, nil
	}
	return resultsBind[0].AggregateVersion, nil
}

func processEvent(
	metaPartitionKey int8,
	kafkaIO *KafkaIO,
	eventTable *csndra.Table,
	eventMetaTable *csndra.Table,
	eventMsg *sarama.ConsumerMessage,
) {
	// Decode Event and Insert to Event-Store
	event, err := decodeEvent(eventMsg)
	if err != nil {
		// Kafka-Response informing that the Unmarshalling was unsuccessful.
		err = errors.Wrap(err, "Error Unmarshalling Event-data to Event-struct")
		log.Println(err)

		// Commit message so this bugged message doesn't appear again
		kafkaIO.MarkOffset() <- eventMsg
		return
	}

	aggVersion, err := getAggVersion(eventMetaTable, metaPartitionKey, event.AggregateID)
	if err != nil {
		err = errors.Wrapf(
			err,
			"EventMeta: Error Getting Event-Version to use for Aggregate ID: %d",
			event.AggregateID,
		)
		log.Println(err)

		kr := &model.KafkaResponse{
			AggregateID: event.AggregateID,
			Input:       eventMsg.Value,
			Error:       err.Error(),
		}
		kafkaIO.ProducerInput() <- kr
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
		AggregateID: event.AggregateID,
		Input:       eventMsg.Value,
		Error:       errStr,
	}
	kafkaIO.ProducerInput() <- kr
}
