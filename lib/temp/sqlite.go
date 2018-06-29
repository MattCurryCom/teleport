package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/pborman/uuid"
	"time"
	//"io/ioutil"
	"log"
	//"net/http"
	"os"
	//"strings"

	"github.com/gravitational/teleport/lib/defaults"
	"github.com/gravitational/teleport/lib/services"

	_ "github.com/mattn/go-sqlite3"
)

type item struct {
	k string
	v string
}

func main() {
	err := os.Remove("foo.db")
	if err != nil {
		log.Fatal(err)
		return
	}
	db, err := sql.Open("sqlite3", "foo.db")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE FOO (
		id TEXT PRIMARY KEY,
		name TEXT
	);`)
	if err != nil {
		log.Fatal(err)
		return
	}
	_, err = db.Exec(`
		PRAGMA synchronous = OFF;
		PRAGMA journal_mode = MEMORY;
	`)
	if err != nil {
		log.Fatal(err)
		return
	}

	er := []item{}
	for i := 0; i < 4000; i++ {
		id := uuid.NewUUID()

		server := &services.ServerV2{
			Kind:    services.KindNode,
			Version: services.V2,
			Metadata: services.Metadata{
				Name:      id.String(),
				Namespace: defaults.Namespace,
			},
			Spec: services.ServerSpecV2{
				Addr:     "127.0.0.1:3022",
				Hostname: uuid.NewUUID().String(),
			},
		}
		resource, err := services.GetServerMarshaler().MarshalServer(server)
		if err != nil {
			log.Fatal(err)
			return
		}
		encodedResource := base64.StdEncoding.EncodeToString([]byte(resource))
		er = append(er, item{k: id.String(), v: encodedResource})
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
		return
	}

	stmt, err := tx.Prepare(`
			INSERT INTO FOO (
				id, name)
			VALUES (
				?, ?
			);`)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer stmt.Close()

	for i := 0; i < 4000; i++ {
		start := time.Now()

		_, err = stmt.Exec(er[i].k, er[i].v)
		if err != nil {
			log.Fatal(err)
			return
		}
		fmt.Printf("--> Elapsed: %v\n", time.Since(start))
	}

	tx.Commit()
}
