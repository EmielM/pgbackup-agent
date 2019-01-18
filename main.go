package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"./pg"
)

var (
	aesBlock cipher.Block
)
var config struct {
	PgConn   string `json:"pgConn"`
	SystemId uint64 `json:"systemId"`
	Email    string `json:"email"`
	Key      string `json:"key"`
	key      [32]byte
}

func main() {

	if len(os.Args) == 1 {
		os.Stdout.Write(([]byte)(`usage:
  pgbackup stream: capture, encrypt & upload wal stream
  pgbackup basebackup: create, encrypt & upload basebackup
  pgbackup restore [lsn] [dir]: attempt to rebuild database in [dir] (eg db/) and restore up to [lsn] (eg 01/00004000)
  pgbackup fetch [segment] [dest]: fetch wal segment from storage (used internally by restore_command)
  pgbackup status: get status summary from server
  pgbackup setup: setup ~/pgbackup.conf
`))
		return
	}

	cmd := os.Args[1]

	var err error

	if cmd == "setup" {
		// pgbackup setup
		err = Setup()
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	d, _ := ioutil.ReadFile(os.Getenv("HOME") + "/pgbackup.conf")
	json.Unmarshal(d, &config)
	key, _ := base64.RawStdEncoding.DecodeString(config.Key)
	if config.PgConn == "" || config.SystemId == 0 || len(key) != 32 || config.Email == "" {
		log.Fatal("could not read ~/pgbackup.conf")
	}
	copy(config.key[:], key)

	aesBlock, err = aes.NewCipher(key)
	if err != nil {
		log.Fatal(err)
	}

	var restartN int
restart:
	err = errors.New("no such subcommand")
	if cmd == "stream" {
		// pgbackup stream
		err = Stream()
		log.Print(err)
		restartN++
		sleep := restartN * restartN
		if sleep > 100 {
			sleep = 100
		}
		time.Sleep(time.Duration(sleep) * time.Second)
		goto restart

	} else if cmd == "basebackup" {
		// pgbackup basebackup
		err = Basebackup()

	} else if cmd == "restore" && len(os.Args) > 3 {
		// pgbackup restore 01/00004000 my-db/
		err = Restore(os.Args[2], os.Args[3])

	} else if cmd == "fetch" && len(os.Args) > 3 {
		// pgbackup fetch 000000010000000700000009 some/dest/000000010000000700000009
		err = Fetch(os.Args[2], os.Args[3])

	} else if cmd == "status" {
		err = Status()

	} else {

	}

	if err != nil {
		log.Fatal(err)
	}
}

func Fetch(segment, target string) error {
	var timeline, lsn0, lsn1 int
	fmt.Sscanf(segment, "%08x%08x%08x", &timeline, &lsn0, &lsn1)
	if timeline == 0 || lsn0 == 0 || lsn1 == 0 {
		return errors.New("invalid segment: " + segment)
	}
	lsn := 0x1000000 * ((lsn0 * 0x100) + lsn1)

	backend, err := Connect()
	if err != nil {
		return err
	}
	defer backend.Close()

	file := fmt.Sprintf("%016x.%d.wal", (lsn >> 24), timeline)

	rep, err := backend.Request(fmt.Sprintf("pgbackup.get %s", file))
	n, err := strconv.ParseInt(rep, 16, 0) // file size in hex
	if err != nil {
		return errors.New(rep) // eg: "notFound"
	}

	r := &cipher.StreamReader{R: backend.C, S: aesStream(file)}

	f, err := os.Create(target)
	if err != nil {
		return err
	}

	_, err = io.CopyN(f, r, n)
	if err != nil {
		return err
	}

	if n < 0x1000000 {
		// fill up segment remaining with 0s
		b := make([]byte, 0x1000000-n)
		io.CopyN(f, bytes.NewReader(b), 0x1000000-n)
	}

	return err
}

var streamMissing bool

func Stream() error {
	pc, err := pg.NewConn(config.PgConn + " replication=true")
	if err != nil {
		return err
	}
	defer pc.Close()

	systemId, timeline, lsn0, err := pc.IdentifySystem()
	if err != nil {
		return err
	}

	log.Print("system ", systemId, " on server ", pc.ServerVersion, " at ", lsn0, ".", timeline)

	if systemId != config.SystemId {
		return errors.New("systemId mismatch")
	}

	lsn1, _ := ParseLSN(lsn0)

	backend, err := Connect()
	if err != nil {
		return err
	}
	defer backend.Close()

	// list .wal files, find latest one
	rep, err := backend.Request("pgbackup.list wal")
	if err != nil {
		return err
	}
	if rep != "" && !streamMissing {
		ls := strings.Split(rep, " ")
		latest := ls[len(ls)-1]
		// timeline handling here is not completely correct
		var segment, timeline uint64
		fmt.Sscanf(latest, "%016x.%d.wal", &segment, &timeline)
		lsn1 = LSN(segment << 24)
		log.Print("continue stream at ", lsn1)

	} else {
		lsn1 = lsn1 & ^LSN(0xFFFFFF)
		log.Print("restart stream at ", lsn1)
	}

	walC, err := pc.StartReplication(fmt.Sprintf("START_REPLICATION %s", lsn1.String()))
	if err != nil {
		return err
	}

	var sw *cipher.StreamWriter // segment writer

	for {
		select {
		case d, ok := <-walC:
			if !ok {
				return errors.New("server stopped")
			}
			if d.Lsn == 0 {
				streamMissing = true
				return errors.New("server missing segment")
			}
			streamMissing = false
			if d.Lsn&0xFFFFFF == 0 {
				if sw != nil {
					sw.W.(io.Closer).Close() // close previous chunk
				}

				file := fmt.Sprintf("%016x.%d.wal", (d.Lsn >> 24), timeline)
				err := backend.Send(fmt.Sprintf("pgbackup.put %s", file))
				if err != nil {
					return err
				}

				log.Print("segment ", LSN(d.Lsn))

				cw := &chunkWriter{W: backend.C}
				sw = &cipher.StreamWriter{W: cw, S: aesStream(file)}
				//sw = gzip.NewWriter(w)
			}

			if sw != nil {
				//log.Print("  @", LSN(d.Lsn), " ", len(d.Data), "b")
				_, err := sw.Write(d.Data)
				if err != nil {
					return err
				}
			}
		}
	}
}

func Restore(lsn, target string) error {

	lsn0, err := ParseLSN(lsn)
	if err != nil {
		return err
	}

	backend, err := Connect()
	if err != nil {
		return err
	}
	defer backend.Close()

	// list .base files, find suitable base
	rep, err := backend.Request("pgbackup.list base")
	if err != nil {
		return err
	}

	// they are listed in lexical (chronological) order
	ls := strings.Split(rep, " ")

	cut := fmt.Sprintf("%016x.base", (uint64(lsn0) >> 24))
	var file string
	for _, f := range ls {
		if f >= cut {
			break
		}
		file = f
	}

	if file == "" {
		return errors.New("no suitable basebackup")
	}

	log.Print("restore base ", file)

	rep, err = backend.Request("pgbackup.get " + file)
	n, err := strconv.ParseInt(rep, 16, 0) // file size in hex
	if err != nil {
		return errors.New(rep) // eg: "notFound"
	}

	r := &cipher.StreamReader{R: &io.LimitedReader{R: backend.C, N: n}, S: aesStream(file)}
	err = os.Mkdir(target, 0700)
	if err != nil {
		return err
	}

	tar := exec.Command("/bin/tar", "xf", "-", "-C", target)
	tar.Stdin = r
	tar.Stdout = os.Stdout
	tar.Stderr = os.Stderr

	err = tar.Run()
	if err != nil {
		return err
	}
	log.Print("restored base into ", target)

	ourBin, err := filepath.Abs(os.Args[0])
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(target+"/recovery.conf", ([]byte)(fmt.Sprintf(`
recovery_target_lsn='%s'
restore_command='%s fetch %%f "%%p"'`, lsn, ourBin)), 0600)
	if err != nil {
		return err
	}

	log.Print()
	log.Print("recovery.conf configured for recovery till ", lsn)
	log.Print("to start: /usr/lib/postgresql/10/bin/postgres -D ", target)

	return nil
}

func Basebackup() error {

	pc, err := pg.NewConn(config.PgConn + " replication=true")
	if err != nil {
		return err
	}
	defer pc.Close()

	systemId, timeline, lsn0, err := pc.IdentifySystem()
	if err != nil {
		return err
	}

	log.Print("system ", systemId, " on server ", pc.ServerVersion, " at ", lsn0, ".", timeline)

	if systemId != config.SystemId {
		return errors.New("systemId mismatch")
	}

	backend, err := Connect()
	if err != nil {
		return err
	}
	defer backend.Close()

	_, lsn1, bbC, err := pc.BaseBackup("BASE_BACKUP LABEL 'pgbackup' NOWAIT")
	if err != nil {
		return err
	}

	lsn2, err := ParseLSN(lsn1)
	if err != nil {
		return err
	}

	log.Print("base backup at ", lsn2)

	file := fmt.Sprintf("%016x.base", (uint64(lsn2) >> 24))
	err = backend.Send(fmt.Sprintf("pgbackup.put %s", file))
	if err != nil {
		return err
	}

	cw := &chunkWriter{W: backend.C}
	sw := &cipher.StreamWriter{W: cw, S: aesStream(file)}

	var w int
	for d := range bbC {
		sw.Write(d)
		w += len(d)
	}

	cw.Close()

	log.Print("base backup written ", w, "b")

	return nil
}

func Status() error {
	backend, err := Connect()
	if err != nil {
		return err
	}
	defer backend.Close()

	rep, err := backend.Request("pgbackup.status")
	n, _ := strconv.ParseInt(rep, 16, 0) // size in hex
	_, err = io.CopyN(os.Stdout, backend.C, n)
	return err
}

func aesStream(ivSeed string) cipher.Stream {
	iv := sha256.Sum256(([]byte)(ivSeed))
	return cipher.NewCTR(aesBlock, iv[:16])
}
