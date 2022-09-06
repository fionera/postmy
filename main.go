package main

import (
	dsql "database/sql"
	"fmt"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	_ "github.com/lib/pq"
	"log"
	//"github.com/dolthub/go-mysql-server/sql/information_schema"
)

const (
	host     = "127.0.0.1"
	port     = 5432
	user     = "postgres"
	password = "-"
	dbname   = "postgres"
)

func main() {
	//pro := sql.NewDatabaseProvider(
	//	information_schema.NewInformationSchemaDatabase(),
	//	createTestDatabase(),
	//)

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := dsql.Open("postgres", psqlconn)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	pro := &postgresProvider{
		conn: db,
		dbs:  make(map[string]*postgresDatabase),
	}

	if err := pro.fetchDBs(); err != nil {
		log.Fatal(err)
	}

	a := analyzer.NewBuilder(pro).
		//WithDebug().
		Build()

	engine := sqle.New(a, nil)

	config := server.Config{
		Protocol: "tcp",
		Address:  ":3306",
	}

	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	s.Start()
}
