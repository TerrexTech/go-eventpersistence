package persistence

import (
	"log"
	"strconv"
	"strings"
	"time"

	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	"github.com/TerrexTech/go-eventstore-models/models"
	cql "github.com/gocql/gocql"
	"github.com/pkg/errors"
)

// CassandraIO allows conveniently inserting data into database
type CassandraIO struct {
	table *csndra.Table
}

// Insert inserts the data into database
func (cio *CassandraIO) Insert(event *models.Event) <-chan error {
	return cio.table.AsyncInsert(event)
}

// CloseSession closses the Cassandra session used by CassandraIO
func (cio *CassandraIO) CloseSession() {
	session := cio.table.Session().GoCqlSession()
	session.Close()
}

// CassandraAdapter allows ceonvniently connecting to Cassandra
// and creates the required Keyspace and Table.
type CassandraAdapter struct {
	DataCenters []string
	Hosts       []string
	Keyspace    string
	Password    string
	Table       string
	Username    string
	TableDef    map[string]csndra.TableColumn
	// Currently used for closing session in tests only.
	session *driver.Session
}

// evenStoreSession creates a Cassandra session, used for storing events.
func (ca *CassandraAdapter) eventStoreSession(
	clusterHosts *[]string,
	username string,
	password string,
) (*driver.Session, error) {
	cluster := cql.NewCluster(*clusterHosts...)
	cluster.ConnectTimeout = time.Millisecond * 3000
	cluster.Timeout = time.Millisecond * 3000
	cluster.ProtoVersion = 4

	if username != "" && password != "" {
		cluster.Authenticator = cql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	return csndra.GetSession(cluster)
}

// createKeyspaceTable creates the required Keyspace and Table
// as specified in CassandraAdapter.
func (ca *CassandraAdapter) createKeyspaceTable(
	session driver.SessionI,
	keyspaceName string,
	tableName string,
	datacenters *[]string,
) (*csndra.Table, error) {
	datacenterMap := map[string]int{}
	for _, dcStr := range *datacenters {
		dc := strings.Split(dcStr, ":")
		centerID, err := strconv.Atoi(dc[1])
		if err != nil {
			return nil, errors.Wrap(
				err,
				"Cassandra Keyspace Create Error (CASSANDRA_DATA_CENTERS format mismatch)"+
					"CASSANDRA_DATA_CENTERS must be of format \"<ID>:<replication_factor>\"",
			)
		}
		datacenterMap[dc[0]] = centerID
	}

	keyspaceConfig := csndra.KeyspaceConfig{
		Name:                    keyspaceName,
		ReplicationStrategy:     "NetworkTopologyStrategy",
		ReplicationStrategyArgs: datacenterMap,
	}

	keyspace, err := csndra.NewKeyspace(session, keyspaceConfig)
	if err != nil {
		return nil, errors.Wrap(err, "Error Creating Keyspace:")
	}

	tc := &csndra.TableConfig{
		Keyspace: keyspace,
		Name:     tableName,
	}

	t, err := csndra.NewTable(session, tc, &ca.TableDef)
	if err != nil {
		return nil, errors.Wrap(err, "Error Creating Table:")
	}
	return t, nil
}

// InitIO creates CassandraIO from CassandraAdapter.
// CassandraIO can be used for inserting events into database.
func (ca *CassandraAdapter) InitIO() (*CassandraIO, error) {
	if ca.Hosts == nil || len(ca.Hosts) == 0 {
		return nil, errors.New(
			"Error connecting to Cassandra: Empty or Nil Hosts provided",
		)
	}
	log.Println("Initializing CassandraIO")

	// Create Cassandra session
	session, err := ca.eventStoreSession(&ca.Hosts, ca.Username, ca.Password)
	if err != nil {
		err = errors.Wrap(err, "Cassandra Session Creation Error")
		return nil, err
	}
	ca.session = session
	log.Println("Created Cassandra Session")

	// Create Keyspace and Table
	t, err := ca.createKeyspaceTable(
		ca.session,
		ca.Keyspace,
		ca.Table,
		&ca.DataCenters,
	)
	if err != nil {
		err = errors.Wrap(err, "Keyspace Creation Error:")
		return nil, err
	}
	log.Println("Created Cassandra Keyspace")

	cio := &CassandraIO{
		table: t,
	}
	log.Println("CassandraIO Ready")
	return cio, nil
}
