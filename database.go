package main

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"log"
	"strings"
)

type postgresDatabase struct {
	p      *postgresProvider
	name   string
	tables map[string]sql.Table
}

func (db *postgresDatabase) DropTable(ctx *sql.Context, name string) error {
	//TODO(fionera): probably a dirty flag?
	defer db.fetchTables()

	// TODO(fionera): Remove sprintf
	_, err := db.p.conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", name))
	return err
}

func convertType(c *sql.Column, writePK bool) string {
	result := c.Type.String()

	switch c.Type.Type() {
	case sqltypes.Uint8:
		fallthrough
	case sqltypes.Uint16:
		fallthrough
	case sqltypes.Uint24:
		fallthrough
	case sqltypes.Uint32:
		fallthrough
	case sqltypes.Uint64:
		result = strings.ReplaceAll(result, "UNSIGNED", fmt.Sprintf("CHECK (%s >= 0)", c.Name))
	case sqltypes.Text:
		result = "TEXT"
	case sqltypes.Datetime:
		result = "timestamp with time zone"
	case sqltypes.Float64:
		result = "double precision"
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		result = "bytea"
	}

	if c.AutoIncrement {
		result = "SERIAL"
	}

	if !c.Nullable {
		result += " NOT NULL"
	}

	if c.PrimaryKey && writePK {
		result += " PRIMARY KEY"
	}

	log.Println(c.Type)
	if c.Default != nil {
		s := c.Default.Expression.String()
		s = strings.ReplaceAll(s, "\"", "'")

		switch c.Default.Type().Type() {
		case sqltypes.Uint8:
			fallthrough
		case sqltypes.Uint16:
			fallthrough
		case sqltypes.Uint24:
			fallthrough
		case sqltypes.Uint32:
			fallthrough
		case sqltypes.Uint64:
			fallthrough
		case sqltypes.Int8:
			fallthrough
		case sqltypes.Int16:
			fallthrough
		case sqltypes.Int24:
			fallthrough
		case sqltypes.Int32:
			fallthrough
		case sqltypes.Int64:
			s = strings.ReplaceAll(s, "'", "")
		case sqltypes.Datetime:
			s = strings.ReplaceAll(s, "0000-00-00", "0001-01-01")
		}

		result += " DEFAULT " + s
	}
	log.Println(result)

	return result
}

func (db *postgresDatabase) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema) error {
	//TODO(fionera): probably a dirty flag?
	defer db.fetchTables()

	var query string
	// TODO(fionera): Remove sprintf
	query += fmt.Sprintf("CREATE TABLE %s (", name)

	var columns []string
	for _, column := range schema.Schema {
		// TODO(fionera): Remove sprintf
		columns = append(columns, fmt.Sprintf("%s %s", column.Name, convertType(column, len(schema.PkOrdinals) == 1)))
	}
	if len(schema.PkOrdinals) > 1 {
		var pks []string
		for _, ordinal := range schema.PkOrdinals {
			pks = append(pks, schema.Schema[ordinal].Name)
		}
		// TODO(fionera): Remove sprintf
		columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)\n", strings.Join(pks, ", ")))
	}

	query += strings.Join(columns, ",\n")
	query += ");"

	fmt.Println(schema.PkOrdinals)
	fmt.Println(ctx.Query())
	fmt.Println(query)

	// TODO(fionera): Remove sprintf
	_, err := db.p.conn.ExecContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (db *postgresDatabase) Name() string {
	return db.name
}

func (db *postgresDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, db.tables)
	return tbl, ok, nil
}

func (db *postgresDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(db.tables))
	for k := range db.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

func (db *postgresDatabase) fetchTables() error {
	res, err := db.p.conn.Query("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
	if err != nil {
		return err
	}

	tables := make(map[string]sql.Table)
	for res.Next() {
		var tableName string
		if err := res.Scan(&tableName); err != nil {
			return err
		}
		tables[tableName] = &postgresTable{
			p:    db.p,
			db:   db,
			name: tableName,
		}
	}
	db.tables = tables
	return nil
}

var _ sql.Database = &postgresDatabase{}
var _ sql.TableCreator = &postgresDatabase{}
var _ sql.TableDropper = &postgresDatabase{}
