package persistence

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-commonutils/utils"
	"github.com/TerrexTech/go-eventstore-models/models"
	cql "github.com/gocql/gocql"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

var _ = Describe("CassansraAdapter", func() {
	var (
		adapter *CassandraAdapter
		// This refers to the system_schema table in cassandra containing information
		// about the keyspace, tables, and other configuration.
		systemSchemaTable string
	)

	BeforeEach(func() {
		// Setup environment variables
		err := godotenv.Load("../.env")
		Expect(err).ToNot(HaveOccurred())

		hosts := os.Getenv("CASSANDRA_HOSTS")
		dataCenters := os.Getenv("CASSANDRA_DATA_CENTERS")
		username := os.Getenv("CASSANDRA_USERNAME")
		password := os.Getenv("CASSANDRA_PASSWORD")
		keyspaceName := "lib_test_db"
		tableName := os.Getenv("CASSANDRA_TABLE")

		systemSchemaTable = "system_schema.tables"

		tableDef := map[string]csndra.TableColumn{
			"action": csndra.TableColumn{
				Name:            "action",
				DataType:        "text",
				PrimaryKeyIndex: "3",
			},
			"aggregateID": csndra.TableColumn{
				Name:            "aggregate_id",
				DataType:        "int",
				PrimaryKeyIndex: "1",
			},
			"data": csndra.TableColumn{
				Name:     "data",
				DataType: "text",
			},
			"timestamp": csndra.TableColumn{
				Name:            "timestamp",
				DataType:        "timestamp",
				PrimaryKeyIndex: "4",
				PrimaryKeyOrder: "DESC",
			},
			"userID": csndra.TableColumn{
				Name:     "user_id",
				DataType: "int",
			},
			"uuid": csndra.TableColumn{
				Name:            "uuid",
				DataType:        "uuid",
				PrimaryKeyIndex: "5",
			},
			"version": csndra.TableColumn{
				Name:            "version",
				DataType:        "int",
				PrimaryKeyIndex: "2",
				PrimaryKeyOrder: "DESC",
			},
			"yearBucket": csndra.TableColumn{
				Name:            "year_bucket",
				DataType:        "smallint",
				PrimaryKeyIndex: "0",
			},
		}

		adapter = &CassandraAdapter{
			DataCenters: *utils.ParseHosts(dataCenters),
			Hosts:       *utils.ParseHosts(hosts),
			Keyspace:    keyspaceName,
			Password:    password,
			Table:       tableName,
			Username:    username,
			TableDef:    tableDef,
			session:     nil,
		}
	})

	AfterEach(func() {
		log.Println("Closing Cassandra Session")
		if adapter.session != nil {
			adapter.session.Close()
		}
	})

	Describe("CassandraAdapter", func() {
		It("should create the specified keyspace", func() {
			_, err := adapter.InitIO()
			Expect(err).ToNot(HaveOccurred())

			cluster := cql.NewCluster(adapter.Hosts...)
			cluster.ConnectTimeout = time.Millisecond * 1000
			cluster.Timeout = time.Millisecond * 1000
			cluster.ProtoVersion = 4
			if adapter.Username != "" && adapter.Password != "" {
				cluster.Authenticator = cql.PasswordAuthenticator{
					Username: adapter.Username,
					Password: adapter.Password,
				}
			}

			session, err := csndra.GetSession(cluster)
			Expect(err).ToNot(HaveOccurred())

			stmt, columns := qb.Select(systemSchemaTable).
				Where(
					qb.Eq("keyspace_name"),
					qb.Eq("table_name"),
				).
				Count("table_name").
				ToCql()

			q := session.Query(stmt).GoCqlQuery()
			q = gocqlx.Query(q, columns).BindMap(qb.M{
				"keyspace_name": adapter.Keyspace,
				"table_name":    adapter.Table,
			}).Query
			iter := gocqlx.Iter(q)

			// Count is just a single result
			count := make([]int, 1)
			err = iter.Select(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count[0]).To(Equal(1))

			session.Close()
		})

		It("should return error when data-centers are missing", func() {
			adapter.DataCenters = nil
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		It("should return error when hosts are missing", func() {
			adapter.Hosts = nil
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		It("should return error when keyspace is missing", func() {
			adapter.Keyspace = ""
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		It("should return error when table-name is missing", func() {
			adapter.Table = ""
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		It("should return error when table-definition is missing", func() {
			adapter.TableDef = nil
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		It("should return error if invalid credentials are provided", func() {
			adapter.Password = ""
			_, err := adapter.InitIO()
			Expect(err).To(HaveOccurred())
		})

		Context("session is requested to be closed", func() {
			It("should close the session if session is set", func() {
				cio, err := adapter.InitIO()
				Expect(err).ToNot(HaveOccurred())

				isClosed := adapter.session.GoCqlSession().Closed()
				Expect(isClosed).To(BeFalse())

				cio.CloseSession()
				isClosed = adapter.session.GoCqlSession().Closed()
				Expect(isClosed).To(BeTrue())
			})
		})

		It("should insert the event in Cassandra event-store", func() {
			cio, err := adapter.InitIO()
			Expect(err).ToNot(HaveOccurred())

			uuid, err := gocql.RandomUUID()
			Expect(err).ToNot(HaveOccurred())
			timestamp := time.Now()

			event := &models.Event{
				Action:      "insert",
				AggregateID: 1,
				Data:        "test-data",
				Timestamp:   timestamp,
				UserID:      1,
				UUID:        uuid,
				Version:     1,
				YearBucket:  2018,
			}
			err = <-cio.Insert(event)
			Expect(err).ToNot(HaveOccurred())

			cluster := cql.NewCluster(adapter.Hosts...)
			cluster.ConnectTimeout = time.Millisecond * 1000
			cluster.Timeout = time.Millisecond * 1000
			cluster.ProtoVersion = 4
			if adapter.Username != "" && adapter.Password != "" {
				cluster.Authenticator = cql.PasswordAuthenticator{
					Username: adapter.Username,
					Password: adapter.Password,
				}
			}

			session, err := csndra.GetSession(cluster)
			Expect(err).ToNot(HaveOccurred())

			// Get the event from database and match its values
			tableName := fmt.Sprintf("%s.%s", adapter.Keyspace, adapter.Table)
			stmt, columns := qb.Select(tableName).
				Where(
					qb.Eq("year_bucket"),
					qb.Eq("aggregate_id"),
					qb.Eq("version"),
					qb.Eq("action"),
					qb.Eq("timestamp"),
					qb.Eq("uuid"),
				).
				ToCql()

			q := session.Query(stmt).GoCqlQuery()
			q = gocqlx.Query(q, columns).BindStruct(event).Query
			iter := gocqlx.Iter(q)

			// Count is just a single result
			count := []models.Event{}
			err = iter.Select(&count)
			Expect(err).ToNot(HaveOccurred())

			// Cassandra's Timestamp is in different timezone which
			// will cause the test to fail. We still got uuid and
			// other fields for adequate comparison.
			count[0].Timestamp = timestamp
			Expect(count[0]).To(Equal(*event))

			session.Close()
		})
	})
})
