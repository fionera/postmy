package main

import (
	"context"
	dsql "database/sql"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
)

type tableInserter struct {
	*postgresTable
	ctx context.Context
	tx  *dsql.Tx
}

func (t *tableInserter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	//TODO implement me
	panic("implement me")
}

func (t *tableInserter) StatementBegin(ctx *sql.Context) {
	tx, err := t.p.conn.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	t.tx = tx
}

func (t *tableInserter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if t.tx == nil {
		return fmt.Errorf("invalid transaction")
	}
	return t.tx.Rollback()
}

func (t *tableInserter) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (t *tableInserter) Insert(context *sql.Context, row sql.Row) error {
	_, err := t.tx.ExecContext(context, context.Query())
	if err != nil {
		return err
	}

	return nil
}

func (t *tableInserter) Close(context *sql.Context) error {
	if t.tx == nil {
		return fmt.Errorf("invalid transaction")
	}
	return t.tx.Commit()
}

func (table *postgresTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return &tableInserter{
		postgresTable: table,
		ctx:           ctx,
	}
}

func (table *postgresTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &tableInserter{
		postgresTable: table,
		ctx:           ctx,
	}
}

var _ sql.InsertableTable = &postgresTable{}
var _ sql.UpdatableTable = &postgresTable{}
