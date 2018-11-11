package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"

	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	cql "github.com/gocql/gocql"
	cqlx "github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

// EventTestUtil creates convenient Kafka consumer/producer for testing event-persistence.
type EventTestUtil struct {
	KafkaBrokers      []string
	ConsumerGroupName string
	ConsumerTopic     string
	EventsTopic       string

	ValidActionsCmd       []string
	CmdEventTopicSuffix   string
	QueryEventTopicSuffix string

	EventTableName string
	CQLSession     *cql.Session

	Writer func(s string, args ...interface{})
}

// Produce produces the provided mockEvent on EventsTopic.
func (t *EventTestUtil) Produce(mockEvent model.Event, errorChan chan<- error) {
	t.Writer("Creating Kafka mock-event Producer")
	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		KafkaBrokers: t.KafkaBrokers,
	})
	errorChan <- err

	go func() {
		for prodErr := range producer.Errors() {
			errorChan <- prodErr.Err
		}
	}()

	// Produce event on Kafka topic
	t.Writer("Marshalling mock-event to json")
	mockEventMsg, err := json.Marshal(mockEvent)
	errorChan <- err

	mockEventInput := producer.Input()
	t.Writer("Producing mock-event on event-consumer topic")
	mockEventInput <- kafka.CreateMessage(t.EventsTopic, mockEventMsg)

	log.Printf("Produced Event with ID: %s", mockEvent.UUID)
	producer.Close()
}

// DidConsume creates a consumer on ConsumerTopic which
// checks if the topic receives the provided MockEvent.
func (t *EventTestUtil) DidConsume(
	mockEvent model.Event,
	timeoutSec int,
	responseChan chan<- *model.KafkaResponse,
	errorChan chan<- error,
) {
	t.Writer(
		"Checking if the Kafka response-topic received the event, " +
			"with timeout of 20 seconds",
	)

	validCmdAction := commonutil.IsElementInSlice(t.ValidActionsCmd, mockEvent.EventAction)

	var topicSuffix string
	if validCmdAction {
		topicSuffix = t.CmdEventTopicSuffix
	} else {
		topicSuffix = t.QueryEventTopicSuffix
	}
	consumerTopic := fmt.Sprintf(
		"%s.%d.%s",
		t.ConsumerTopic,
		mockEvent.AggregateID,
		topicSuffix,
	)

	t.Writer("Consuming on Topic: %s", consumerTopic)
	responseConsumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		KafkaBrokers: t.KafkaBrokers,
		GroupName:    t.ConsumerGroupName,
		Topics:       []string{consumerTopic},
	})
	errorChan <- err

	go func() {
		for consumerErr := range responseConsumer.Errors() {
			errorChan <- consumerErr
		}
	}()

	msgCallback := func(msg *sarama.ConsumerMessage) bool {
		// Unmarshal the Kafka-Response
		t.Writer("Verifying received response")
		response := &model.KafkaResponse{}
		err := json.Unmarshal(msg.Value, response)
		errorChan <- err

		log.Printf("Response UUID: %s", response.UUID)
		log.Printf("Response CorrelationID: %s", response.CorrelationID)

		// Check if the event is the one we are looking for
		cidMatch := response.CorrelationID == mockEvent.CorrelationID
		uuidMatch := response.UUID == mockEvent.UUID
		if cidMatch && uuidMatch {
			responseChan <- response
			responseConsumer.Close()
			return true
		}
		return false
	}

	handler := &msgHandler{msgCallback}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(timeoutSec)*time.Second,
	)
	defer cancel()
	responseConsumer.Consume(ctx, handler)
}

// DidStore checks if the provided MockEvent is stored in Cassandra.
func (t *EventTestUtil) DidStore(mockEvent model.Event, aggVersion int64) error {
	// Try fetching the MockEvent from Database, we should have a matching event
	stmt, columns := qb.Select(t.EventTableName).Where(
		qb.Eq("year_bucket"),
		qb.Eq("aggregate_id"),
		qb.Eq("version"),
		qb.Eq("nano_time"),
		qb.Eq("uuid"),
	).ToCql()

	mockEvent.Version = aggVersion
	q := t.CQLSession.Query(stmt)
	q = cqlx.Query(q, columns).BindStruct(mockEvent).Query

	iter := cqlx.Iter(q)
	events := make([]model.Event, 0)
	err := iter.Select(&events)
	if err != nil {
		err = errors.Wrap(err, "Error in Select")
		return err
	}

	if len(events) == 0 {
		return errors.New("no events found")
	}

	actualEvent := events[0]
	if actualEvent.YearBucket != mockEvent.YearBucket {
		return errors.New("YearBucket mismatch")
	}
	if actualEvent.AggregateID != mockEvent.AggregateID {
		return errors.New("AggregateID mismatch")
	}
	if actualEvent.EventAction != mockEvent.EventAction {
		return errors.New("EventAction mismatch")
	}
	if actualEvent.NanoTime != mockEvent.NanoTime {
		return errors.New("NanoTime mismatch")
	}
	if actualEvent.UUID != mockEvent.UUID {
		return errors.New("UUID mismatch")
	}
	if actualEvent.Version != aggVersion {
		return errors.New("Version mismatch")
	}

	return nil
}

// DidNotStore checks if the provided (invalid) MockEvent is not stored in Cassandra.
func (t *EventTestUtil) DidNotStore(mockEvent model.Event, aggVersion int64) error {
	// Try fetching the MockEvent from Database, we should have a matching event
	stmt, columns := qb.Select(t.EventTableName).Where(
		qb.Eq("year_bucket"),
		qb.Eq("aggregate_id"),
		qb.Eq("version"),
		qb.Eq("nano_time"),
		qb.Eq("uuid"),
	).ToCql()

	mockEvent.Version = aggVersion
	q := t.CQLSession.Query(stmt)
	q = cqlx.Query(q, columns).BindStruct(mockEvent).Query

	iter := cqlx.Iter(q)
	events := make([]model.Event, 0)
	err := iter.Select(&events)
	if err != nil {
		err = errors.Wrap(err, "Error in Select")
		return err
	}

	if len(events) != 0 {
		return errors.New("no events found")
	}

	return nil
}
