package main

import (
	"encoding/json"
	"fmt"
	"log"

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
	Logger           *tlog.Logger
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
	if config.Logger == nil {
		return nil, errors.New("invalid config: Logger cannot be nil")
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

func (e *eventHandler) Setup(sarama.ConsumerGroupSession) error {
	logEntry := "Initializing Kafka EventHandler"
	e.config.Logger.Log(model.LogEntry{
		Description: logEntry,
		ErrorCode:   0,
		Level:       "INFO",
	})
	log.Println(logEntry)
	return nil
}

func (e *eventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	logEntry := "Closing Kafka EventHandler"
	e.config.Logger.Log(model.LogEntry{
		Description: logEntry,
		ErrorCode:   0,
		Level:       "INFO",
	})
	log.Println(logEntry)

	return nil
}

func (e *eventHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	tLog := e.config.Logger

	logEntry := "Listening for new Events..."
	tLog.Log(model.LogEntry{
		Description: logEntry,
		ErrorCode:   0,
		Level:       "INFO",
	})
	log.Println(logEntry)
	for msg := range claim.Messages() {
		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			event := &model.Event{}
			err := json.Unmarshal(msg.Value, event)
			if err != nil {
				err = errors.Wrap(err, "Error: unable to Unmarshal Event")
				tLog.Log(errToLogEntry(err), string(msg.Value))
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}

			logEntry := fmt.Sprintf("Received Event with ID: %s", event.UUID)
			tLog.Log(model.LogEntry{
				Description: logEntry,
				ErrorCode:   0,
				Level:       "INFO",
			}, event)
			log.Printf(logEntry)

			// =====> Validate Event
			if event.AggregateID == 0 {
				err = errors.New("received an Event with missing AggregateID")
				tLog.Log(errToLogEntry(err), event)
				log.Println(err)

				session.MarkMessage(msg, "")
				return
			}
			if event.NanoTime == 0 {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing NanoTime")
				tLog.Log(errToLogEntry(err), event)
				log.Println(err)

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
					tLog.Log(errToLogEntry(err), event)
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}
			if event.UUID == (uuuid.UUID{}) {
				session.MarkMessage(msg, "")
				err = errors.New("received an Event with missing UUID")
				tLog.Log(errToLogEntry(err), event)
				log.Println(err)

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
					tLog.Log(errToLogEntry(err), event)
					log.Println(err)
				} else {
					responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
					respMsg := kafka.CreateMessage(responseTopic, resp)
					e.config.ResponseProducer.Input() <- respMsg
				}
				return
			}

			isValidAction := commonutil.IsElementInSlice(e.config.ValidActions, event.EventAction)
			if !isValidAction {
				session.MarkMessage(msg, "")
				err = fmt.Errorf(
					`Error: Event with ID "%s" and AggregateID "%d" `+
						`has invalid EventAction "%s"`,
					event.UUID,
					event.AggregateID,
					event.EventAction,
				)
				err = errors.Wrap(err, "Error processing Event")
				tLog.Log(errToLogEntry(err), event)
				log.Println(err)

				kr := &model.KafkaResponse{
					AggregateID:   event.AggregateID,
					CorrelationID: event.CorrelationID,
					Input:         msg.Value,
					Error:         err.Error(),
					UUID:          event.UUID,
				}
				resp, err := json.Marshal(kr)
				if err != nil {
					err = errors.Wrap(err, "InvalidAction Error: Error Marshalling KafkaResponse")
					tLog.Log(errToLogEntry(err), event)
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
				tLog.Log(errToLogEntry(err), event)
				log.Println(err)

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
					tLog.Log(errToLogEntry(err))
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
				tLog.Log(errToLogEntry(err))
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
				UUID:          event.UUID,
			}
			resp, err := json.Marshal(kr)
			if err != nil {
				err = errors.Wrap(err, "EventHandler: Error Marshalling KafkaResponse")
				tLog.Log(errToLogEntry(err))
				log.Println(err)
			} else {
				responseTopic := fmt.Sprintf("%s.%d", e.config.ResponseTopic, event.AggregateID)
				respMsg := kafka.CreateMessage(responseTopic, resp)
				e.config.ResponseProducer.Input() <- respMsg

				logEntry := fmt.Sprintf(
					`Produced KafkaResponse with ID: "%s" on Topic: "%s"`,
					event.UUID.String(),
					responseTopic,
				)
				tLog.Log(model.LogEntry{
					Description: logEntry,
					ErrorCode:   0,
					Level:       "INFO",
				}, event)
				log.Printf(logEntry, kr)
			}
		}(session, msg)
	}
	return nil
}
