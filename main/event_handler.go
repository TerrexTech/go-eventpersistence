package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
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

	ValidActionsCmd       []string
	ValidActionsQuery     []string
	CmdEventTopicSuffix   string
	QueryEventTopicSuffix string
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
	if config.ValidActionsCmd == nil || len(config.ValidActionsCmd) == 0 {
		return nil, errors.New("invalid config: ValidActionsCmd cannot be blank")
	}
	if config.ValidActionsQuery == nil || len(config.ValidActionsQuery) == 0 {
		return nil, errors.New("invalid config: ValidActionsQuery cannot be blank")
	}
	if config.CmdEventTopicSuffix == "" {
		return nil, errors.New("invalid config: CmdEventTopicSuffix cannot be blank")
	}
	if config.QueryEventTopicSuffix == "" {
		return nil, errors.New("invalid config: QueryEventTopicSuffix cannot be blank")
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
			validCmdAction := commonutil.IsElementInSlice(e.ValidActionsCmd, event.EventAction)
			validQueryAction := commonutil.IsElementInSlice(e.ValidActionsQuery, event.EventAction)
			if !validCmdAction && !validQueryAction {
				session.MarkMessage(msg, "")
				err = fmt.Errorf(
					`Error: Event with ID "%s" and AggregateID "%d" `+
						`has invalid EventAction "%s"`,
					event.UUID,
					event.AggregateID,
					event.EventAction,
				)
				err = errors.Wrap(err, "Error processing Event")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)
				return
			}

			var topicSuffix string
			if validCmdAction {
				topicSuffix = e.CmdEventTopicSuffix
			} else {
				topicSuffix = e.QueryEventTopicSuffix
			}
			responseTopic := fmt.Sprintf(
				"%s.%d.%s",
				e.ResponseTopic,
				event.AggregateID,
				topicSuffix,
			)
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
			if event.NanoTime == 0 {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing NanoTime")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.UUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "MissingNanoTime Error: Error Marshalling KafkaResponse")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, kr)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, kr)
				}
				return
			}
			if event.UUID == (uuuid.UUID{}) {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing UUID")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.UUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "MissingUUID Error: Error Marshalling KafkaResponse")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, kr)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, kr)
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

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.UUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "GetAggVersion: Error Marshalling KafkaResponse")
					e.Logger.E(tlog.Entry{
						Description: err.Error(),
					}, event, kr)
				} else {
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.ResponseProducer.Input() <- respMsg
					e.Logger.D(tlog.Entry{
						Description: "Produced error-response on topic: " + responseTopic,
					}, kr)
				}
				return
			}
			event.Version = aggVersion
			err = e.EventStore.CommitEvent(event)

			e.Logger.D(tlog.Entry{
				Description: "Event committed.",
			}, event)

			// =====> Send KafkaResponse from Event-Processing
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

			kr := &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         errStr,
				UUID:          event.UUID,
			}
			resp, err := json.Marshal(kr)
			if err != nil {
				err = errors.Wrap(err, "EventHandler: Error Marshalling KafkaResponse")
				e.Logger.E(tlog.Entry{
					Description: err.Error(),
				}, event, kr)
			} else {
				respMsg := kafka.CreateMessage(responseTopic, resp)
				e.ResponseProducer.Input() <- respMsg

				e.Logger.I(tlog.Entry{
					Description: fmt.Sprintf(
						`Produced KafkaResponse with ID: "%s" on Topic: "%s"`,
						event.UUID.String(),
						responseTopic,
					),
				})
			}
		}(session, msg)
	}
	return nil
}
