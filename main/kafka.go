package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/bsm/sarama-cluster"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	"github.com/pkg/errors"
)

// KafkaIO provides channels for interacting with Kafka.
// Note: All receive-channels must be read from to prevent deadlock.
type KafkaIO struct {
	Adapter            *KafkaAdapter
	consumerErrChan    <-chan error
	consumerMsgChan    <-chan *sarama.ConsumerMessage
	consumerOffsetChan chan<- *sarama.ConsumerMessage
	producerErrChan    <-chan *sarama.ProducerError
	producerInputChan  chan<- *model.KafkaResponse
}

// ConsumerErrors returns send-channel where consumer errors are published.
func (kio *KafkaIO) ConsumerErrors() <-chan error {
	return kio.consumerErrChan
}

// ConsumerMessages returns send-channel where consumer messages are published.
func (kio *KafkaIO) ConsumerMessages() <-chan *sarama.ConsumerMessage {
	return kio.consumerMsgChan
}

// MarkOffset marks the consumer message-offset to be committed.
// This should be used once a message has done its job.
func (kio *KafkaIO) MarkOffset() chan<- *sarama.ConsumerMessage {
	return kio.consumerOffsetChan
}

// ProducerErrors returns send-channel where producer errors are published.
func (kio *KafkaIO) ProducerErrors() <-chan *sarama.ProducerError {
	return kio.producerErrChan
}

// ProducerInput returns receive-channel where kafka-responses can be produced.
func (kio *KafkaIO) ProducerInput() chan<- *model.KafkaResponse {
	return kio.producerInputChan
}

// KafkaAdapter allows conveniently connecting to Kafka, and creates required
// Topics and channels for Kafka-communication.
type KafkaAdapter struct {
	Brokers                 []string
	ConsumerGroupName       string
	ConsumerTopics          []string
	OffsetRetentionDuration time.Duration
	ResponseTopic           string
}

// responseProducer creates a new Kafka-Producer used for producing the
// responses after processing consumed Kafka-messages.
func (ka *KafkaAdapter) responseProducer(
	brokers []string,
) (*producer.Producer, error) {
	config := producer.Config{
		KafkaBrokers: brokers,
	}
	resProducer, err := producer.New(&config)
	if err != nil {
		return nil, err
	}
	return resProducer, nil
}

// Consumer creates a new Kafka-Consumer which listens for the events.
func (ka *KafkaAdapter) consumer() (*consumer.Consumer, error) {
	saramaCfg := cluster.NewConfig()
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.MaxProcessingTime = 10 * time.Second
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Retention = ka.OffsetRetentionDuration

	config := &consumer.Config{
		ConsumerGroup: ka.ConsumerGroupName,
		KafkaBrokers:  ka.Brokers,
		SaramaConfig:  saramaCfg,
		Topics:        ka.ConsumerTopics,
	}

	return consumer.New(config)
}

// InitIO initializes KafkaIO from the configuration provided to KafkaAdapter.
// It is necessary that both consumer and producer are properly setup, to enable
// response for every request. Else this operation will be marked as failed,
// and the service won't run.
func (ka *KafkaAdapter) InitIO() (*KafkaIO, error) {
	log.Println("Initializing KafkaIO")

	// Create Kafka Response-Producer
	resProducer, err := ka.responseProducer(ka.Brokers)
	if err != nil {
		err = errors.Wrap(err, "Error Creating Response Producer")
		return nil, err
	}
	log.Println("Create Kafka Response-Producer")
	resProducerInput, err := resProducer.Input()
	if err != nil {
		err = errors.Wrap(err, "Error Getting Input-Channel from Producer")
		return nil, err
	}

	// Setup Producer I/O channels
	producerInputChan := make(chan *model.KafkaResponse)
	kio := &KafkaIO{
		Adapter:           ka,
		producerInputChan: (chan<- *model.KafkaResponse)(producerInputChan),
		producerErrChan:   resProducer.Errors(),
	}

	// The Kafka-Response post-processing the consumed events
	go func() {
		for msg := range producerInputChan {
			if msg.Input == "" && msg.Error == "" {
				err = errors.New(
					"KafkaResponse: Both Input and Error are empty, " +
						"atleast one must be non-empty",
				)
				log.Fatalln(err)
			}
			msgJSON, err := json.Marshal(msg)
			if err != nil {
				err = errors.Wrapf(err, "Error Marshalling KafkaResponse: %s", msg)
				log.Fatalln(err)
			}

			producerMsg := producer.CreateMessage(ka.ResponseTopic, msgJSON)
			resProducerInput <- producerMsg
		}
	}()
	log.Println("Created Kafka Response-Channel")

	// Create Kafka Event-Consumer
	eventConsumer, err := ka.consumer()
	if err != nil {
		err = errors.Wrap(err, "Error Creating ConsumerGroup for Events")
		return nil, err
	}
	log.Println("Created Kafka Event-Consumer Group")

	// A channel which receives consumer-messages to be committed
	consumerOffsetChan := make(chan *sarama.ConsumerMessage)
	kio.consumerOffsetChan = (chan<- *sarama.ConsumerMessage)(consumerOffsetChan)
	go func() {
		for msg := range consumerOffsetChan {
			eventConsumer.MarkOffset(msg, "")
		}
	}()
	log.Println("Created Kafka Event Offset-Commit Channel")

	// Setup Consumer I/O channels
	kio.consumerErrChan = eventConsumer.Errors()
	kio.consumerMsgChan = eventConsumer.Messages()
	log.Println("KafkaIO Ready")

	return kio, nil
}
