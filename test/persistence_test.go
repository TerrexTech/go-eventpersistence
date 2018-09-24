package test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/uuuid"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
	cql "github.com/gocql/gocql"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	cqlx "github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

// This suite tests the following:
// * That the generated event is consumed by consumer-topic
// * That the consumed event is processed, and an adequate response is generated
// on Kafka response-topic
// * That the aggregate-version is read and applied from event-meta table
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

		mockEvent      *model.Event
		mockEventInput chan<- *sarama.ProducerMessage
		metaAggVersion int64
	)

	BeforeSuite(func() {
		Describe("An event is produced", func() {
			log.Println("Reading environment file")
			err := godotenv.Load("../.env")
			Expect(err).ToNot(HaveOccurred())

			brokers = commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS"))
			consumerGroupName = os.Getenv("KAFKA_CONSUMER_GROUP")
			consumerTopic = os.Getenv("KAFKA_CONSUMER_TOPICS")

			config := &producer.Config{
				KafkaBrokers: *brokers,
			}

			log.Println("Creating Kafka mock-event Producer")
			kafkaProducer, err := producer.New(config)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Generating random uuid")
			eventUUID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			userUUID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent = &model.Event{
				Action:      "insert",
				AggregateID: 1,
				Data:        []byte("test-data"),
				Timestamp:   time.Now(),
				UserUUID:    userUUID,
				UUID:        eventUUID,
				Version:     1,
				YearBucket:  2018,
			}
			responseTopic = fmt.Sprintf(
				"%s.%d",
				os.Getenv("KAFKA_RESPONSE_TOPIC"),
				mockEvent.AggregateID,
			)
			metaAggVersion = 42

			// Create event-meta table for controlling event-versions
			metaTable, err := bootstrap.EventMeta()
			Expect(err).ToNot(HaveOccurred())
			metaData := &model.EventMeta{
				AggregateVersion: metaAggVersion,
				AggregateID:      mockEvent.AggregateID,
				PartitionKey:     0,
			}
			err = <-metaTable.AsyncInsert(metaData)
			Expect(err).ToNot(HaveOccurred())

			// Produce event on Kafka topic
			log.Println("Marshalling mock-event to json")
			testEventMsg, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())

			log.Println("Fetching input-channel from mock-event producer")
			mockEventInput, err = kafkaProducer.Input()
			Expect(err).ToNot(HaveOccurred())

			mockEventInput <- producer.CreateMessage(consumerTopic, testEventMsg)
			log.Println("Produced mock-event on consumer-topic")
		})
	})

	Context("An event is produced", func() {
		var (
			responseConsumer   *consumer.Consumer
			session            *cql.Session
			eventTableName     string
			eventMetaTableName string
		)
		BeforeEach(func() {
			var err error

			// =====> Setup Kafka
			if responseConsumer == nil {
				consumerConfig := &consumer.Config{
					ConsumerGroup: consumerGroupName,
					KafkaBrokers:  *brokers,
					Topics:        []string{responseTopic},
				}
				responseConsumer, err = consumer.New(consumerConfig)
				Expect(err).ToNot(HaveOccurred())
			}

			// =====> Setup Cassandra
			hosts := os.Getenv("CASSANDRA_HOSTS")
			username := os.Getenv("CASSANDRA_USERNAME")
			password := os.Getenv("CASSANDRA_PASSWORD")
			keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
			eventTableName = os.Getenv("CASSANDRA_EVENT_TABLE")
			eventMetaTableName = os.Getenv("CASSANDRA_EVENT_META_TABLE")

			// Create Cassandra Session
			cluster := cql.NewCluster(*commonutil.ParseHosts(hosts)...)
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

			session, err = cluster.CreateSession()
			Expect(err).ToNot(HaveOccurred())
		})

		Specify("no errors should appear on response-consumer", func() {
			go func() {
				defer GinkgoRecover()
				for consumerErr := range responseConsumer.Errors() {
					Expect(consumerErr).ToNot(HaveOccurred())
				}
			}()
		})

		// This test will run in a go-routine, and must succeed within 10 seconds
		Specify(
			"the mock-event processing-result should appear on Kafka response-topic",
			func(done Done) {
				log.Println(
					"Checking if the Kafka response-topic received the event, " +
						"with timeout of 10 seconds",
				)

				for msg := range responseConsumer.Messages() {
					// Mark the message-offset since we do not want the
					// same message to appear again in later tests.
					responseConsumer.MarkOffset(msg, "")
					err := responseConsumer.SaramaConsumerGroup().CommitOffsets()
					err = errors.Wrap(err, "Error Committing Offsets for message")
					Expect(err).ToNot(HaveOccurred())

					// Unmarshal the Kafka-Response
					log.Println("An Event was received, now verifying")
					response := &model.KafkaResponse{}
					log.Println(string(msg.Value))
					err = json.Unmarshal(msg.Value, response)

					Expect(err).ToNot(HaveOccurred())
					Expect(response.Error).To(BeEmpty())

					// Unmarshal the Input from Kafka-Response
					msgEvent := &model.Event{}
					err = json.Unmarshal([]byte(response.Input), msgEvent)
					Expect(err).ToNot(HaveOccurred())

					// Check if the event is the one we are looking for
					if msgEvent.UUID == mockEvent.UUID {
						log.Println("The event matched the expectations")
						close(done)
					}
				}
			}, 10)

		Specify("the event should be stored in Cassandra event-store", func() {

			log.Println("Checking if the event was correctly stored in event-store")

			// Try fetching the MockEvent from Database, we should have a matching event
			stmt, columns := qb.Select(eventTableName).Where(
				qb.Eq("year_bucket"),
				qb.Eq("aggregate_id"),
				qb.Eq("version"),
				qb.Eq("action"),
				qb.Eq("timestamp"),
				qb.Eq("uuid"),
			).ToCql()

			q := session.Query(stmt)
			mockEvent.Version = metaAggVersion
			q = cqlx.Query(q, columns).BindStruct(mockEvent).Query

			iter := cqlx.Iter(q)
			event := make([]model.Event, 1)
			err := iter.Select(&event)
			Expect(err).ToNot(HaveOccurred())

			// Although just getting the event proves that the event got
			// saved to Database, but lets just still compare it for
			// satisfactory purposes.
			actualEvent := event[0]
			Expect(actualEvent.YearBucket).To(Equal(mockEvent.YearBucket))
			Expect(actualEvent.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(actualEvent.Version).To(Equal(metaAggVersion))
			Expect(actualEvent.Action).To(Equal(mockEvent.Action))
			Expect(actualEvent.Timestamp.Unix()).To(Equal(mockEvent.Timestamp.Unix()))
			Expect(actualEvent.UUID).To(Equal(mockEvent.UUID))
		})

		It("should create entry in EventMeta table if AggregareID is absent", func() {
			log.Println(
				"Ensure that it adds AggregateID to EventMeta table if it doesn't exist.",
			)
			// Just a random ID that's most likely not present in event-meta table
			// at time of this testing
			mockEvent.AggregateID = 79
			event, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			mockEventInput <- producer.CreateMessage(consumerTopic, event)

			log.Println("Waiting for dummy event to be processed")
			time.Sleep(5 * time.Second)
			stmt, columns := qb.Select(eventMetaTableName).Where(
				qb.Eq("partition_key"),
				qb.Eq("aggregate_id"),
			).ToCql()

			eventMetaValues := &model.EventMeta{
				AggregateID:  mockEvent.AggregateID,
				PartitionKey: 0,
			}

			q := session.Query(stmt)
			q = cqlx.Query(q, columns).BindStruct(eventMetaValues).Query

			iter := cqlx.Iter(q)
			bind := make([]model.EventMeta, 1)
			err = iter.Select(&bind)
			Expect(err).ToNot(HaveOccurred())

			Expect(bind[0].AggregateID).To(Equal(mockEvent.AggregateID))
		})
	})
})
