package main

import (
	csndra "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/TerrexTech/go-common-models/model"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/pkg/errors"
)

// EventStore represents the core functions required
// to interact with EventStore for persisting events.
type EventStore interface {
	GetAggVersion(aggregateID int8) (int64, error)
	CommitEvent(event *model.Event) error
}

type eventStore struct {
	eventTable       *csndra.Table
	eventMetaTable   *csndra.Table
	logger           tlog.Logger
	metaPartitionKey int8
}

// NewEventStore creates a new EventStore implementation from provided configuration.
func NewEventStore(
	eventTable *csndra.Table,
	eventMetaTable *csndra.Table,
	logger tlog.Logger,
	metaPartitionKey int8,
) (EventStore, error) {
	if eventTable == nil {
		return nil, errors.New("eventTable cannot be nil")
	}
	if eventMetaTable == nil {
		return nil, errors.New("eventMetaTable cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	return &eventStore{
		eventTable:       eventTable,
		eventMetaTable:   eventMetaTable,
		logger:           logger,
		metaPartitionKey: metaPartitionKey,
	}, nil
}

func (es *eventStore) CommitEvent(event *model.Event) error {
	return <-es.eventTable.AsyncInsert(event)
}

func (es *eventStore) GetAggVersion(aggregateID int8) (int64, error) {
	partitionKeyCol, err := es.eventMetaTable.Column("partitionKey")
	if err != nil {
		return -1, errors.New(`expected column "PartitionKey" not found`)
	}
	aggregateIDCol, err := es.eventMetaTable.Column("aggregateID")
	if err != nil {
		return -1, errors.New(`expected column "AggregateID" not found`)
	}
	aggVerCol, err := es.eventMetaTable.Column("aggregateVersion")
	if err != nil {
		return -1, errors.New(`expected Column "AggregateVersion" not found`)
	}

	resultsBind := []model.EventMeta{}
	sp := csndra.SelectParams{
		ColumnValues: []csndra.ColumnComparator{
			csndra.Comparator(partitionKeyCol, es.metaPartitionKey).Eq(),
			csndra.Comparator(aggregateIDCol, aggregateID).Eq(),
		},
		ResultsBind:   &resultsBind,
		SelectColumns: []string{aggVerCol},
	}
	es.logger.D(tlog.Entry{
		Description: "GetAggVersion, query created",
	}, sp)
	_, err = es.eventMetaTable.Select(sp)
	if err != nil {
		err = errors.Wrap(err, "error Fetching Latest Event-Version from EventMeta")
		return -1, err
	}

	es.logger.D(tlog.Entry{
		Description: "GetAggVersion, results received",
	}, resultsBind)

	if len(resultsBind) > 1 {
		return -1, errors.New(
			"received > 1 entries while fetching aggregate-version",
		)
	}
	if len(resultsBind) == 0 {
		meta := model.EventMeta{
			AggregateVersion: 2,
			AggregateID:      aggregateID,
			PartitionKey:     es.metaPartitionKey,
		}
		err = <-es.eventMetaTable.AsyncInsert(&meta)
		if err != nil {
			err = errors.Wrap(err, "Error writing initial EventMeta to table")
			es.logger.E(tlog.Entry{
				Description: "GetAggVersion, results received",
			}, resultsBind)
			return -1, err
		}
		return meta.AggregateVersion, nil
	}
	return resultsBind[0].AggregateVersion, nil
}
