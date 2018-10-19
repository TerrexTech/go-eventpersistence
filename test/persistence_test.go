package test

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/bootstrap"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	cql "github.com/gocql/gocql"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	cqlx "github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

// This suite tests the following:
// * That the generated event is consumed by consumer-topic
// * That the consumed event is processed, and an adequate response is generated
// on Kafka response-topic
// * That the aggregate-version is read and applied from event-meta table
// * That the processed event gets stored in Cassandra event-store
func TestEventPersistence(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"CASSANDRA_HOSTS",
		"CASSANDRA_KEYSPACE",
		"CASSANDRA_EVENT_TABLE",
		"CASSANDRA_EVENT_META_TABLE",

		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP",
		"KAFKA_CONSUMER_TOPICS",
	)
	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventPersistence Suite")
}

var _ = Describe("EventPersistence", func() {
	var (
		testUtil  *EventTestUtil
		mockEvent model.Event

		eventMetaTableName string
		metaAggVersion     int64
	)

	BeforeSuite(func() {
		var err error
		// =============> Cassandra Configuration
		hosts := os.Getenv("CASSANDRA_HOSTS")
		username := os.Getenv("CASSANDRA_USERNAME")
		password := os.Getenv("CASSANDRA_PASSWORD")
		keyspaceName := os.Getenv("CASSANDRA_KEYSPACE")
		eventTableName := os.Getenv("CASSANDRA_EVENT_TABLE")
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

		session, err := cluster.CreateSession()
		Expect(err).ToNot(HaveOccurred())

		// =============> Kafka Configuration
		brokers := *commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS"))
		consumerGroupName := os.Getenv("KAFKA_CONSUMER_GROUP") + "-test"
		consumerTopicStr := os.Getenv("KAFKA_CONSUMER_TOPICS")
		consumerTopics := *commonutil.ParseHosts(consumerTopicStr)
		responseTopic := os.Getenv("KAFKA_RESPONSE_TOPIC")

		eventUUID, err := uuuid.NewV1()
		Expect(err).ToNot(HaveOccurred())
		userUUID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = model.Event{
			Action:        "insert",
			AggregateID:   1,
			CorrelationID: cid,
			Data:          []byte("test-data"),
			Timestamp:     time.Now(),
			UserUUID:      userUUID,
			TimeUUID:      eventUUID,
			Version:       1,
			YearBucket:    2018,
		}

		testUtil = &EventTestUtil{
			KafkaBrokers:      brokers,
			ConsumerGroupName: consumerGroupName,
			ConsumerTopic:     responseTopic,
			EventsTopic:       consumerTopics[0],

			EventTableName: eventTableName,
			CQLSession:     session,

			Writer: Byf,
		}

		// Create event-meta table for controlling event-versions
		metaAggVersion = 42
		metaTable, err := bootstrap.EventMeta()
		Expect(err).ToNot(HaveOccurred())
		metaData := &model.EventMeta{
			AggregateVersion: metaAggVersion,
			AggregateID:      mockEvent.AggregateID,
			PartitionKey:     0,
		}
		err = <-metaTable.AsyncInsert(metaData)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("A valid event is produced", func() {
		Context("Event-Action is insert", func() {
			Specify(
				"result should appear on Kafka response-topic and event should be persisted",
				func(done Done) {
					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					Byf("Producing Event")
					testUtil.Produce(mockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							mockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Persisting Event")
					err := testUtil.DidStore(mockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("Event-Action is delete", func() {
			Specify(
				"result should appear on Kafka response-topic and event should be persisted",
				func(done Done) {
					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					mockEvent.Action = "delete"

					Byf("Producing Event")
					testUtil.Produce(mockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							mockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Persisting Event")
					err := testUtil.DidStore(mockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("Event-Action is query", func() {
			Specify(
				"result should appear on Kafka response-topic and event should be persisted",
				func(done Done) {
					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					mockEvent.Action = "query"

					Byf("Producing Event")
					testUtil.Produce(mockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							mockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Persisting Event")
					err := testUtil.DidStore(mockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("Event-Action is update", func() {
			Specify(
				"result should appear on Kafka response-topic and event should be persisted",
				func(done Done) {
					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					mockEvent.Action = "update"

					Byf("Producing Event")
					testUtil.Produce(mockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							mockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Persisting Event")
					err := testUtil.DidStore(mockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})
	})

	It("should create entry in EventMeta table if AggregareID is absent", func() {
		Byf(
			"Ensuring that it adds AggregateID to EventMeta table if it doesn't exist.",
		)

		// Test with three random values for more surity
		for i := 0; i < 3; i++ {
			// Just a random ID that's most likely not present in event-meta table
			// at time of this testing
			minID := 10
			maxID := 100
			s1 := rand.NewSource(time.Now().UnixNano())
			r1 := rand.New(s1)
			aggID := int8(r1.Intn(maxID-minID) + minID)
			mockEvent.AggregateID = aggID

			timeuid, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.TimeUUID = timeuid

			Byf("inserting AggregateID %d", aggID)

			errorChan := make(chan error)
			go func() {
				defer GinkgoRecover()
				for err := range errorChan {
					Expect(err).ToNot(HaveOccurred())
				}
			}()
			testUtil.Produce(mockEvent, errorChan)

			Byf("Processing MockEvent")
			time.Sleep(5 * time.Second)
			stmt, columns := qb.Select(eventMetaTableName).Where(
				qb.Eq("partition_key"),
				qb.Eq("aggregate_id"),
			).ToCql()

			Byf("Checking if Entry was created")
			eventMetaValues := &model.EventMeta{
				AggregateID:      mockEvent.AggregateID,
				AggregateVersion: 1,
				PartitionKey:     0,
			}
			q := testUtil.CQLSession.Query(stmt)
			q = cqlx.Query(q, columns).BindStruct(eventMetaValues).Query

			iter := cqlx.Iter(q)
			bind := make([]model.EventMeta, 1)
			err = iter.Select(&bind)
			Expect(err).ToNot(HaveOccurred())

			Expect(bind[0].AggregateID).To(Equal(mockEvent.AggregateID))
		}
	})

	Describe("An invalid event is produced", func() {
		Context("an event with invalid action is generated", func() {
			Specify(
				"result should appear on Kafka response-topic and event should not be persisted",
				func(done Done) {
					cid, err := uuuid.NewV4()
					Expect(err).ToNot(HaveOccurred())
					timeuid, err := uuuid.NewV1()
					Expect(err).ToNot(HaveOccurred())

					invalidMockEvent := mockEvent
					invalidMockEvent.AggregateID = 1
					invalidMockEvent.Action = "InvalidAction"
					invalidMockEvent.CorrelationID = cid
					invalidMockEvent.TimeUUID = timeuid

					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					Byf("Producing Event")
					testUtil.Produce(invalidMockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							invalidMockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Not Persisting Event")
					err = testUtil.DidNotStore(invalidMockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("an event with missing UUID is generated", func() {
			Specify(
				"result should appear on Kafka response-topic and event should not be persisted",
				func(done Done) {
					invalidMockEvent := mockEvent
					invalidMockEvent.TimeUUID = uuuid.UUID{}

					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					Byf("Producing Event")
					testUtil.Produce(invalidMockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							invalidMockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Not Persisting Event")
					err := testUtil.DidNotStore(invalidMockEvent, metaAggVersion)
					Expect(err).To(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("an event with TimeUUID V1 is generated", func() {
			Specify(
				"result should appear on Kafka response-topic and event should not be persisted",
				func(done Done) {
					timeuid, err := uuuid.NewV4()
					Expect(err).ToNot(HaveOccurred())

					invalidMockEvent := mockEvent
					invalidMockEvent.TimeUUID = timeuid

					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						defer GinkgoRecover()
						for err := range errorChan {
							Expect(err).ToNot(HaveOccurred())
						}
					}()

					Byf("Producing Event")
					testUtil.Produce(invalidMockEvent, errorChan)

					Byf("Processing Event")
					go func() {
						testUtil.DidConsume(
							invalidMockEvent,
							20,
							(chan<- *model.KafkaResponse)(responseChan),
							errorChan,
						)
					}()
					<-responseChan

					Byf("Not Persisting Event")
					err = testUtil.DidNotStore(invalidMockEvent, metaAggVersion)
					Expect(err).To(HaveOccurred())
					close(done)
				}, 30,
			)
		})

		Context("an event with missing AggregateID is generated", func() {
			Specify(
				"result should appear on Kafka response-topic and event should not be persisted",
				func(done Done) {
					timeuid, err := uuuid.NewV1()
					Expect(err).ToNot(HaveOccurred())

					invalidMockEvent := mockEvent
					invalidMockEvent.AggregateID = 0
					invalidMockEvent.TimeUUID = timeuid

					responseChan := make(chan *model.KafkaResponse)
					errorChan := make(chan error)

					go func() {
						for _ = range errorChan {
							// Ignore errors because we dont care about timeouts here
						}
					}()

					Byf("Producing Event")
					testUtil.Produce(invalidMockEvent, errorChan)

					Byf("Processing Event")
					testUtil.DidConsume(
						invalidMockEvent,
						20,
						(chan<- *model.KafkaResponse)(responseChan),
						errorChan,
					)

					Byf("Not Persisting Event")
					err = testUtil.DidNotStore(invalidMockEvent, metaAggVersion)
					Expect(err).ToNot(HaveOccurred())
					close(done)
				}, 30,
			)
		})
	})
})
