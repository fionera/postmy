package main

import (
	dsql "database/sql"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
)

type postgresProvider struct {
	conn *dsql.DB
	dbs  map[string]*postgresDatabase
}

func (p *postgresProvider) CreateDatabase(ctx *sql.Context, name string) error {
	//TODO(fionera): probably a dirty flag?
	defer p.fetchDBs()

	// TODO(fionera): Remove sprintf
	_, err := p.conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", name))
	return err
}

func (p *postgresProvider) DropDatabase(ctx *sql.Context, name string) error {
	//TODO(fionera): probably a dirty flag?
	defer p.fetchDBs()

	// TODO(fionera): Remove sprintf
	_, err := p.conn.ExecContext(ctx, fmt.Sprintf("DROP DATABASE %s", name))
	return err
}

func (p *postgresProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	if db, ok := p.dbs[name]; ok {
		return db, nil
	}

	return nil, sql.ErrDatabaseNotFound.New(name)
}

func (p *postgresProvider) HasDatabase(ctx *sql.Context, name string) bool {
	if _, ok := p.dbs[name]; ok {
		return true
	}
	return false
}

func (p *postgresProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	var dbs []sql.Database
	for _, database := range p.dbs {
		dbs = append(dbs, database)
	}
	return dbs
}

func (p *postgresProvider) fetchDBs() error {
	rows, err := p.conn.Query("select datname from pg_database")
	if err != nil {
		return err
	}

	dbs := make(map[string]*postgresDatabase)
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return err
		}

		pdb := &postgresDatabase{
			p:    p,
			name: dbName,
		}
		dbs[dbName] = pdb

		if err := pdb.fetchTables(); err != nil {
			return err
		}
	}
	p.dbs = dbs

	return nil
}

var _ sql.DatabaseProvider = &postgresProvider{}
var _ sql.MutableDatabaseProvider = &postgresProvider{}
