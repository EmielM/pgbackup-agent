package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"./pg"
)

func out(s string, args ...interface{}) {
	os.Stdout.Write(([]byte)(fmt.Sprintf(s+"\n", args...)))
}

func ask(s string) string {
	os.Stdout.Write(([]byte)(s + ": "))
	var r []byte
	for {
		var b [1]byte
		_, err := os.Stdin.Read(b[:])
		if err != nil {
			os.Exit(1)
		}
		if b[0] == '\n' {
			return string(r)
		}
		r = append(r, b[0])
	}
}

func Setup() error {
	// install pgbackup.conf into ~/pgbackup.conf

	confFile := os.Getenv("HOME") + "/pgbackup.conf"
	if _, e := os.Stat(confFile); e == nil {
		return errors.New("~/pgbackup.conf already exists")
	}

	out(`
The pgbackup agent will run on your database server, it will
- connect to your postgresql database using the replication protocol
- encrypt and upload the WAL (write ahead log) as it comes in
- regularly take base backups and encrypt and upload those

This program is open source: https://github.com/emielm/pgbackup

To get started, enter your database credentials:`)

	var testConn *pg.Conn
	for {
		host := ask("host (eg 127.0.0.1 or /some/path)")
		if host == "" {
			continue
		}
		port := ask("port [5432]")
		if port == "" {
			port = "5432"
		}
		db := ask("database")
		user := ask("username")
		pass := ask("password")

		var err error
		config.PgConn = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s replication=true", host, port, db, user, pass)
		testConn, err = pg.NewConn(config.PgConn)
		if err == nil {
			break
		}
		out("Could not connect: %s\n", err)
	}

	var err error
	config.SystemId, _, _, err = testConn.IdentifySystem()
	if err != nil {
		return err
	}

	out("\nConnected to postgresql %s", testConn.ServerVersion)
	out("systemId: %d", config.SystemId)

	out("\nTo help us notify you about your backup, please enter")
	config.Email = ask("your email address")

	_, err = rand.Read(config.key[:])
	if err != nil {
		return err
	}
	config.Key = base64.RawStdEncoding.EncodeToString(config.key[:])

	conf, err := json.Marshal(&config)
	if err == nil {
		err = ioutil.WriteFile(confFile, conf, 0600)
	}
	if err != nil {
		return err
	}

	// try to connect
	backend, err := Connect()
	if err != nil {
		return err
	}
	backend.Close()

	ourBin, _ := filepath.Abs(os.Args[0])

	out("Saved config to %s", confFile)

	out("\nThere are 3 final steps for your database backup to begin")

	out("\n1) Start '%s stream' as a background task", ourBin)
	out("   To start right now: %s stream &", ourBin)
	out("   To set things up more permanently, you could use this systemd")
	out("   unit file: https://pgbackup.com/pgbackup.unit")

	out("\n2) Schedule '%s basebackup' to run 3x per day", ourBin)
	out("   For instance with the following crontab line:")
	out("   0 5,13,21 * * * %s basebackup", ourBin)

	out("\n3) Save a copy of %s", confFile)
	out("  Be sure to save it to a secure location, possibly encrypted,")
	out("  as it contains the key to decrypt and restore your database.")

	out("\nThanks for trying pgbackup")

	return nil
}
