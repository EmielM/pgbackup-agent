package pg

// client to connect to

import (
	"bufio"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

var (
	errProtocol = errors.New("pg: protocol error")
	errAuth     = errors.New("pg: unsupported auth scheme")
)

type Conn struct {
	conn net.Conn
	rb   io.Reader

	ServerVersion string
}

func NewConn(connString string) (*Conn, error) {

	network, addr, opts, err := parseConnString(connString)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		conn: conn,
		rb:   bufio.NewReader(conn), // not sure this is a big perf gain
	}

	password, _ := opts["password"]
	user, _ := opts["user"]
	delete(opts, "password")

	b := WriteBuf{}
	b.Int32(196608)
	for k, v := range opts {
		b.String(k)
		b.String(v)
	}
	b.String("")
	// startup packet has no tag, don't use .send()
	d := append(make([]byte, 4), []byte(b)...)
	binary.BigEndian.PutUint32(d, uint32(len(b)+4))
	_, err = c.conn.Write(d)
	if err != nil {
		return nil, err
	}

	for {
		tag, payload, err := c.recv()
		if err != nil {
			return nil, err
		}

		switch tag {
		case 'R':
			err := c.auth(payload, user, password)
			if err != nil {
				return nil, err
			}
		case 'S': // ParameterStatus
			param := payload.String()
			switch param {
			case "server_version":
				c.ServerVersion = payload.String()
			default:
				// ignore
			}
		case 'K': // BackendKeyData
			//c.processID = r.Int32()
			//c.secretKey = r.Int32()
		case 'Z': // ReadyForQuery
			txStatus := payload.Byte()
			if txStatus != 'I' { // txStatus should be idle after connecting
				return nil, errProtocol
			}
			return c, nil
		default:
			return nil, errProtocol
		}
	}

	return c, nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) SimpleQuery(q string) ([][]interface{}, error) {

	b := WriteBuf{}
	b.String(q)
	c.send('Q', b)

	rows, err := c.processResult()
	if err != nil {
		c.processReady()
		return nil, err
	}

	err = c.processReady()
	if err != nil {
		return nil, err
	}
	return rows, err
}

func (c *Conn) processReady() error {
	for {
		tag, _, err := c.recv()
		if err != nil {
			return err
		}

		switch tag {
		case 'Z': // ReadyForQuery
			return nil
		default:
			log.Print("pg: processReady unknown tag=", string(tag))
			return errProtocol
		}
	}
}

// processes a regular result set (RowDescription, DataRow, CommandComplete)
func (c *Conn) processResult() ([][]interface{}, error) {

	var colNames []string
	var colTypes []uint32
	var colFormats []uint16

	rows := make([][]interface{}, 0)

	for {
		tag, payload, err := c.recv()
		if err != nil {
			return nil, err
		}

		//log.Print("pg: pcr tag=", string(tag))

		switch tag {
		case 'T': // RowDescription
			n := int(payload.Int16())
			colNames = make([]string, n)
			colTypes = make([]uint32, n)
			colFormats = make([]uint16, n)
			for i := 0; i < n; i++ {
				colNames[i] = payload.String()
				payload.Bytes(6)
				colTypes[i] = uint32(payload.Int32())
				payload.Bytes(6)
				colFormats[i] = uint16(payload.Int16())
			}
		case 'D': // DataRow
			n := int(payload.Int16())
			row := make([]interface{}, n)
			for i := 0; i < n; i++ {
				l := int(payload.Int32())
				if l == -1 {
					row[i] = nil
				} else {
					row[i] = c.decode(payload.Bytes(l), colTypes[i], colFormats[i])
				}
			}
			rows = append(rows, row)
		case 'C': // CommandComplete
			return rows, nil
		default:
			log.Print("pg: processResult unknown tag=", string(tag))
		}
	}
}

func (c *Conn) send(tag byte, payload WriteBuf) error {
	d := make([]byte, 5)
	d[0] = tag
	binary.BigEndian.PutUint32(d[1:], uint32(len(payload)+4))
	d = append(d, []byte(payload)...)
	_, err := c.conn.Write(d)
	return err
}

func (c *Conn) recv() (byte, ReadBuf, error) {

	var x [5]byte

	for {
		_, err := io.ReadFull(c.rb, x[:])
		if err != nil {
			return 0, nil, err
		}

		tag := x[0]
		n := int(binary.BigEndian.Uint32(x[1:])) - 4

		payload := make([]byte, n)
		_, err = io.ReadFull(c.rb, payload)
		if err != nil {
			return 0, nil, err
		}

		switch tag {
		case 'E': // ErrorResponse
			return 0, nil, errors.New(errorResponseString(payload))
		case 'N': // NoticeResponse
			log.Print("pg: ", errorResponseString(payload))
		default:
			return tag, ReadBuf(payload), nil
		}
	}
}

func (c *Conn) auth(payload ReadBuf, user, password string) error {
	code := payload.Int32()
	switch code {
	case 0:
		// OK
		return nil
	case 3, 5:
		b := WriteBuf{}
		if code == 3 {
			b.String(password)
		} else {
			challenge := string(payload.Bytes(4))
			b.String("md5" + md5sum(md5sum(password+user)+challenge))
		}
		c.send('p', b)

		tag, payload, err := c.recv()
		if err != nil {
			return err
		}
		switch tag {
		case 'R':
			if payload.Int32() == 0 {
				return nil
			}
			return errProtocol
		default:
			return errProtocol
		}
	default:
		return errAuth
	}
}

func errorResponseString(payload ReadBuf) string {
	var severity, msg string
	for t := payload.Byte(); t != 0; t = payload.Byte() {
		s := payload.String()
		switch t {
		case 'S':
			severity = s
		case 'M':
			msg = s
		}
	}
	return fmt.Sprintf("%s: %s", severity, msg)
}

func md5sum(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Very ad-hoc still, eg doesnt support quotes
func parseConnString(s string) (string, string, map[string]string, error) {

	// bunch of defaults
	opts := map[string]string{
		"user":     "postgres",
		"password": "",
		"database": "postgres",
	}
	network := "tcp"
	addr := "localhost"
	port := 5432

	p := strings.Split(s, " ")
	for _, kv := range p {
		s := strings.Split(kv, "=")
		if len(s) != 2 {
			continue
		}

		// fixup some weird mappings
		if s[0] == "dbname" {
			s[0] = "database"
		}
		if s[0] == "replication" && s[1] == "true" {
			s[1] = "database"
		}

		if s[0] == "host" && s[1] != "" && (s[1][0] == '/' || s[1][0] == '.') {
			network = "unix"
			addr = s[1]
		} else if s[0] == "host" && s[1] != "" {
			network = "tcp"
			addr = s[1]
		} else if s[0] == "port" {
			if p, _ := strconv.Atoi(s[1]); p != 0 {
				port = p
			}
		} else {
			opts[s[0]] = s[1]
		}
	}

	if network == "tcp" {
		addr = fmt.Sprintf("%s:%d", addr, port)
	} else if network == "unix" {
		addr = fmt.Sprintf("%s/.s.PGSQL.%d", addr, port)
	}

	return network, addr, opts, nil
}
