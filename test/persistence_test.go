package test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"

	"github.com/joho/godotenv"
	cqlx "github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"

	"github.com/TerrexTech/go-commonutils/utils"

	cql "github.com/gocql/gocql"

	"github.com/TerrexTech/go-eventstore-models/models"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// This suite tests the following:
// * That the generated event is consumed by consumer-topic
// * That the consumed event is processed, and an adequate response is generated
// on Kafka response-topic
// * That the processed event gets stored in Cassandra event-store
func TestEventPersistence(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EventPersistence Suite")
}

var _ = Describe("EventPersistence", func() {
	var (
		brokers           *[]string
		consumerGroupName string
		consumerTopic     string
		responseTopic     string

		mockEvent *models.Event
	)

	BeforeSuite(func() {
		Describe("An event is produced", func() {
			log.Println("Reading environment file")
			err := godotenv.Load("../.env")
			Expect(err).ToNot(HaveOccurred())

			brokers = utils.ParseHosts(os.Getenv("KAFKA_BROKERS"))
			consumerGroupName = os.Getenv("KAFKA_CONSUMER_GROUP")
			consumerTopic = os.Getenv("KAFKA_CONSUMER_TOPICS")
			responseTopic = os.Getenv("KAFKA_RESPONSE_TOPIC")

			config := &producer.Config{
				KafkaBrokers: *brokers,
			}

			log.Println("Creating Kafka mock-event Producer")
			kafkaProducer, err := producer.New(config)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Generating random uuid")
			uuid, err := cql.RandomUUID()
			Expect(err).ToNot(HaveOccurred())

			mockEvent = &models.Event{
				Action:      "insert",
				AggregateID: 1,
				Data:        "test-data",
				Timestamp:   time.Now(),
				UserID:      1,
				UUID:        uuid,
				Version:     1,
				YearBucket:  2018,
			}

			log.Println("Marshalling mock-event to json")
			testEventMsg, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Fetching input-channel from mock-event producer")
			input, err := kafkaProducer.Input()
			Expect(err).ToNot(HaveOccurred())

			input <- kafkaProducer.CreateMessage(consumerTopic, string(testEventMsg))
			log.Println("Produced mock-event on consumer-topic")
		})
	})

	Context("An event is produced", func() {
		var responseConsumer *consumer.Consumer

		BeforeEach(func() {
			if responseConsumer == nil {
				config := cluster.NewConfig()
				config.Consumer.Offsets.Initial = sarama.OffsetOldest
				config.Consumer.MaxProcessingTime = 10 * time.Second
				config.Consumer.Return.Errors = true

				consumerConfig := &consumer.Config{
					ConsumerGroup: consumerGroupName,
					KafkaBrokers:  *brokers,
					Topics:        []string{responseTopic},
					SaramaConfig:  config,
				}

				var err error
				responseConsumer, err = consumer.New(consumerConfig)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		Specify("no errors should occur on response-consumer", func() {
			go func() {
				for consumerErr := range responseConsumer.Errors() {
					Expect(consumerErr).ToNot(HaveOccurred())
				}
			}()
		})

		// This test will run in a go-routine, and must succeed within 5 seconds
		Specify(
			"the mock-event processing-result should appear on Kafka response-topic",
			func(done Done) {
				log.Println(
					"Checking if the Kafka response-topic received the event, " +
						"with timeout of 5 seconds",
				)

				for msg := range responseConsumer.Messages() {
					// Mark the message-offset since we do not want the
					// same message to appear again in later tests.
					responseConsumer.MarkOffset(msg, "")
					// Context is added to this error (using errors.Wrap) later below
					err := responseConsumer.SaramaConsumerGroup().CommitOffsets()
					err = errors.Wrap(err, "Error Committing Offsets for message")
					Expect(err).ToNot(HaveOccurred())

					// Unmarshal the Kafka-Response
					log.Println("An Event was received, now verifying")
					response := &models.KafkaResponse{}
					err = json.Unmarshal(msg.Value, response)

					Expect(err).ToNot(HaveOccurred())
					Expect(response.Error).To(BeEmpty())

					// Unmarshal the Input from Kafka-Response
					msgEvent := &models.Event{}
					err = json.Unmarshal([]byte(response.Input), msgEvent)
					Expect(err).ToNot(HaveOccurred())

					// Check if the event is the one we are looking for
					if msgEvent.UUID == mockEvent.UUID {
						log.Println("The event matched the expectations")
						close(done)
					}
				}
			}, 5)

		Specify("The event should be stored in Cassandra event-store", func() {
			log.Println("Checking if the event was correctly stored in event-store")

			hosts := os.Getenv("CASSANDRA_HOSTS")
			username := os.Getenv("CASSANDRA_USERNAME")
			password := os.Getenv("CASSANDRA_PASSWORD")
			keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
			tableName := os.Getenv("CASSANDRA_TABLE")

			// Create Cassandra Session
			cluster := cql.NewCluster(*utils.ParseHosts(hosts)...)
			cluster.ConnectTimeout = time.Millisecond * 1000
			cluster.Timeout = time.Millisecond * 1000
			cluster.Keyspace = keyspaceName
			cluster.ProtoVersion = 4

			if username != "" && password != "" {
				cluster.Authenticator = cql.PasswordAuthenticator{
					Username: username,
					Password: password,
				}
			}

			session, err := cluster.CreateSession()
			Expect(err).ToNot(HaveOccurred())

			// Try fetching the MockEvent from Database, we should have a matching event
			stmt, columns := qb.Select(tableName).Where(
				qb.Eq("year_bucket"),
				qb.Eq("aggregate_id"),
				qb.Eq("version"),
				qb.Eq("action"),
				qb.Eq("timestamp"),
				qb.Eq("uuid"),
			).ToCql()

			q := session.Query(stmt)
			q = cqlx.Query(q, columns).BindStruct(mockEvent).Query

			iter := cqlx.Iter(q)
			event := make([]models.Event, 1)
			err = iter.Select(&event)
			Expect(err).ToNot(HaveOccurred())

			// Although just getting the event proves that the event got
			// saved to Database, but lets just still compare it for
			// satisfactory purposes.
			actualEvent := event[0]
			Expect(actualEvent.YearBucket).To(Equal(mockEvent.YearBucket))
			Expect(actualEvent.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(actualEvent.Version).To(Equal(mockEvent.Version))
			Expect(actualEvent.Action).To(Equal(mockEvent.Action))
			Expect(actualEvent.Timestamp.Unix()).To(Equal(mockEvent.Timestamp.Unix()))
			Expect(actualEvent.UUID).To(Equal(mockEvent.UUID))
		})
	})
})
