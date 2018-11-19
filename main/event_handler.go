package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// EventHandlerConfig is the configuration for EventsConsumer.
type EventHandlerConfig struct {
	EventStore       EventStore
	Logger           tlog.Logger
	ResponseProducer *kafka.Producer
	ResponseTopic    string
	ServiceName      string
}

// eventHandler handler for Consumer Messages
type eventHandler struct {
	EventHandlerConfig
}

// NewEventHandler creates a new handler for ConsumerEvents.
func NewEventHandler(config EventHandlerConfig) (sarama.ConsumerGroupHandler, error) {
	if config.EventStore == nil {
		return nil, errors.New("invalid config: EventStore cannot be nil")
	}
	if config.Logger == nil {
		return nil, errors.New("invalid config: Logger cannot be nil")
	}
	if config.ResponseProducer == nil {
		return nil, errors.New("invalid config: ResponseProducer cannot be nil")
	}
	if config.ResponseTopic == "" {
		return nil, errors.New("invalid config: ResponseTopic cannot be blank")
	}
	if config.ServiceName == "" {
		return nil, errors.New("invalid config: ServiceName cannot be blank")
	}

	return &eventHandler{config}, nil
}

func (e *eventHandler) Setup(sarama.ConsumerGroupSession) error {
	e.Logger.I(tlog.Entry{
		Description: "Initializing Kafka EventHandler",
	})
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	e.Logger.I(tlog.Entry{
		Description: "Closing Kafka EventHandler",
	})

	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	e.Logger.I(tlog.Entry{
		Description: "Listening for new Events...",
	})
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			event := &model.Event{}
			err := json.Unmarshal(msg.Value, event)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, msg.Value)

				session.MarkMessage(msg, "")
				return
			}

			// =====> Validate Event
			responseTopic := fmt.Sprintf("%s.%d", e.ResponseTopic, event.AggregateID)
			e.Logger.D(tlog.Entry{
				Description: "Using response topic: " + responseTopic,
			}, event)

			if event.AggregateID == 0 {
				err = errors.New("received an Event with missing AggregateID")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				session.MarkMessage(msg, "")
				return
			}
			docUUID, err := uuuid.NewV4()
			if err != nil {
				err = errors.Wrap(err, "Error generating UUID for Document")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				})
				docUUID = uuuid.UUID{}
			}

			if event.NanoTime == 0 {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing NanoTime")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				doc := &model.Document{
					CorrelationID: event.UUID,
					Data:          msg.Value,
					Error:         err.Error(),
					ErrorCode:     1,
					Source:        e.ServiceName,
					Topic:         responseTopic,
					UUID:          docUUID,
				}
				resp, err := json.Marshal(doc)
				if err != nil {
					err = errors.Wrap(err, "MissingNanoTime Error: Error Marshalling Document")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, doc)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, doc)
				}
				return
			}
			if event.UUID == (uuuid.UUID{}) {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing UUID")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				doc := &model.Document{
					CorrelationID: event.UUID,
					Data:          msg.Value,
					Error:         err.Error(),
					ErrorCode:     1,
					Source:        e.ServiceName,
					Topic:         responseTopic,
					UUID:          docUUID,
				}
				resp, err := json.Marshal(doc)
				if err != nil {
					err = errors.Wrap(err, "MissingUUID Error: Error Marshalling Document")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, doc)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, doc)
				}
				return
			}

			// =====> Process Event
			aggVersion, err := e.EventStore.GetAggVersion(event.AggregateID)
			if err != nil {
				err = errors.Wrapf(
					err,
					"GetAggVersion: Error Getting Event-Version to use for Aggregate ID: %d",
					event.AggregateID,
				)
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				doc := &model.Document{
					CorrelationID: event.UUID,
					Data:          msg.Value,
					Error:         err.Error(),
					ErrorCode:     1,
					Source:        e.ServiceName,
					Topic:         responseTopic,
					UUID:          docUUID,
				}
				resp, err := json.Marshal(doc)
				if err != nil {
					err = errors.Wrap(err, "GetAggVersion: Error Marshalling Document")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, doc)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, doc)
				}
				return
			}
			event.Version = aggVersion
			err = e.EventStore.CommitEvent(event)

			e.Logger.D(tlog.Entry{
				Description: "Event committed.",
			}, event)

			// =====> Send Document from Event-Processing
			errStr := ""
			if err != nil {
				err = errors.Wrap(err, "Error Inserting Event into Cassandra")
				errStr = err.Error()
				e.Logger.E(tlog.Entry{
					Description: errStr,
				}, event)
			} else {
				// MarkOffset to be committed if the insert operation is successful
				session.MarkMessage(msg, "")
			}

			doc := &model.Document{
				CorrelationID: event.UUID,
				Data:          msg.Value,
				Source:        e.ServiceName,
				Topic:         responseTopic,
				UUID:          docUUID,
			}
			resp, err := json.Marshal(doc)
			if err != nil {
				err = errors.Wrap(err, "EventHandler: Error Marshalling Document")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event, doc)
			} else {
				respMsg := kafka.CreateMessage(responseTopic, resp)
				e.ResponseProducer.Input() <- respMsg

				e.Logger.I(tlog.Entry{
					Description: fmt.Sprintf(
						`Produced Document with ID: "%s" on Topic: "%s"`,
						event.UUID.String(),
						responseTopic,
					),
				}, doc)
			}
		}(session, msg)
	}
	return nil
}
