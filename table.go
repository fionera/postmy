package main

import (
	dsql "database/sql"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"log"
	"strings"
)

type postgresTable struct {
	p    *postgresProvider
	db   *postgresDatabase
	name string
}

func (table *postgresTable) Name() string {
	return table.name
}

func (table *postgresTable) String() string {
	return fmt.Sprintf("%s.%s", table.db.name, table.name)
}

func (table *postgresTable) Schema() sql.Schema {
	rows, err := table.p.conn.Query("SELECT column_name, data_type, character_maximum_length, column_default, is_nullable FROM information_schema.columns WHERE table_name = $1", table.name)
	if err != nil {
		log.Println(err)
		return nil
	}

	var s sql.Schema
	for rows.Next() {
		var columnName, dataType, columnDefault string
		var characterMaximumLength *int
		var isNullable dsql.NullString

		err := rows.Scan(&columnName, &dataType, &characterMaximumLength, &columnDefault, &isNullable)
		if err != nil {
			log.Println(err)
			return nil
		}

		var t sql.Type
		switch dataType {
		case "character varying":
			if characterMaximumLength == nil {
				log.Println("invalid int definition")
				return nil
			}

			t = sql.MustCreateStringWithDefaults(sqltypes.VarChar, int64(*characterMaximumLength))
		case "integer":
			t = sql.Int32
		case "bigint":
			t = sql.Int64
		case "text":
			t = sql.Text
		case "timestamp with time zone":
			fallthrough
		case "timestamp without time zone":
			t = sql.Datetime
		default:
			log.Println(dataType)
			panic("not implemented")
		}

		//TODO(fionera): Add correct column info
		s = append(s, &sql.Column{
			Name:          columnName,
			Type:          t,
			Default:       nil,
			AutoIncrement: strings.Contains(columnDefault, "nextval"),
			Nullable:      isNullable.Valid && isNullable.String == "YES",
			Source:        table.name,
			PrimaryKey:    false,
			Comment:       "",
			Extra:         "",
		})

	}

	return s
}

func (table *postgresTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	//TODO implement me
	return sql.PartitionsToPartitionIter(), nil
}

func (table *postgresTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

var _ sql.Table = &postgresTable{}
