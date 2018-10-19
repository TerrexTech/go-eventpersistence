package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// EventHandlerConfig is the configuration for EventsConsumer.
type EventHandlerConfig struct {
	EventStore       EventStore
	ResponseProducer *kafka.Producer
	ResponseTopic    string
	ValidActions     []string
}

// eventHandler handler for Consumer Messages
type eventHandler struct {
	config EventHandlerConfig
}

// NewEventHandler creates a new handler for ConsumerEvents.
func NewEventHandler(config EventHandlerConfig) (sarama.ConsumerGroupHandler, error) {
	if config.EventStore == nil {
		return nil, errors.New("invalid config: EventStore cannot be nil")
	}
	if config.ResponseProducer == nil {
		return nil, errors.New("invalid config: ResponseProducer cannot be nil")
	}
	if config.ResponseTopic == "" {
		return nil, errors.New("invalid config: ResponseTopic cannot be blank")
	}
	if config.ValidActions == nil || len(config.ValidActions) == 0 {
		return nil, errors.New("invalid config: ValidActions cannot be blank")
	}

	return &eventHandler{config}, nil
}

func (*eventHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka EventHandler")
	return nil
}

func (*eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka EventHandler")
	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for new Events...")
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			event := &model.Event{}
			err := json.Unmarshal(msg.Value, event)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}
			log.Printf("Received Event with ID: %s", event.TimeUUID)

			// =====> Validate Event
			if event.AggregateID == 0 {
				err = errors.New("received an Event with missing AggregateID")
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}
			if event.TimeUUID.String() == (uuuid.UUID{}).String() {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing UUID")
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.TimeUUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "EmptyUUID Error: Error Marshalling KafkaResponse")
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}
			_, err = uuuid.TimestampFromV1(event.TimeUUID)
			if err != nil {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with non-V1 UUID as TimeUUID")
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.TimeUUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "V1-UUID Error: Error Marshalling KafkaResponse")
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}

			isValidAction := commonutil.IsElementInSlice(e.config.ValidActions, event.Action)
			if !isValidAction {
				session.MarkMessage(msg, "")
				err = fmt.Errorf(
					`Error: Event with ID "%s" and AggregateID "%d "`+
						`has invalid action "%s"`,
					event.TimeUUID,
					event.AggregateID,
					event.Action,
				)
				err = errors.Wrap(err, "Error processing Event")
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.TimeUUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "InvalidAction Error: Error Marshalling KafkaResponse")
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}

			// =====> Process Event
			aggVersion, err := e.config.EventStore.GetAggVersion(event.AggregateID)
			if err != nil {
				err = errors.Wrapf(
					err,
					"GetAggVersion: Error Getting Event-Version to use for Aggregate ID: %d",
					event.AggregateID,
				)
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.TimeUUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "GetAggVersion: Error Marshalling KafkaResponse")
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}
			event.Version = aggVersion
			err = e.config.EventStore.CommitEvent(event)

			// =====> Send KafkaResponse from Event-Processing
			errStr := ""
			if err != nil {
				err = errors.Wrap(err, "Error Inserting Event into Cassandra")
				log.Println(err)
				errStr = err.Error()
			} else {
				// MarkOffset to be committed if the insert operation is successful
				session.MarkMessage(msg, "")
			}

			kr := &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         errStr,
				UUID:          event.TimeUUID,
			}
			resp, err := json.Marshal(kr)
			if err != nil {
				err = errors.Wrap(err, "EventHandler: Error Marshalling KafkaResponse")
				log.Println(err)
			} else {
				responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
				respMsg := kafka.CreateMessage(responseTopic, resp)
				e.config.ResponseProducer.Input() <- respMsg
			}
		}(session, msg)
	}
	return nil
}
