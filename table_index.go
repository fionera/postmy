package main

import "github.com/dolthub/go-mysql-server/sql"

func (table *postgresTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	//TODO implement me
	panic("implement me")
}

func (table *postgresTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

var _ sql.IndexedTable = &postgresTable{}
