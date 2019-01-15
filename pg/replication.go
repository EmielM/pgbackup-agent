package pg

import (
	"log"
	"strconv"
	"strings"
	"time"
)

func (c *Conn) IdentifySystem() (uint64, int, string, error) {
	rows, err := c.SimpleQuery("IDENTIFY_SYSTEM")
	if err != nil {
		return 0, 0, "", err
	}

	if len(rows) != 1 || len(rows[0]) != 4 {
		return 0, 0, "", errProtocol
	}

	systemID, _ := rows[0][0].(string)
	timeline, _ := rows[0][1].(int64)
	lsn, _ := rows[0][2].(string)
	//dbName, _ := rows[0][3].(string)

	systemID0, _ := strconv.Atoi(systemID)
	return uint64(systemID0), int(timeline), lsn, nil
}

type WALData struct {
	Lsn        uint64
	ServerLsn  uint64
	ServerTime time.Time
	Data       []byte
}

// https://www.postgresql.org/docs/9.5/static/protocol-replication.html
func (c *Conn) StartReplication(q string) (<-chan WALData, error) {
	b := WriteBuf{}
	b.String(q)
	c.send('Q', b)

	for {
		tag, _, err := c.recv()
		if err != nil {
			return nil, err
		}
		if tag == 'W' {
			// CopyBothResponse
			break
		}
		log.Print("pg: StartReplication unknown tag=", string(tag))
	}

	walC := make(chan WALData)
	go func() {
		// todo: hmm, do we need to lock c.rb now?
		defer close(walC)
		var clientLsn uint64
		for {
			tag, payload, err := c.recv()
			if err != nil {
				log.Print("pg: replication err=", err)
				if strings.Contains(err.Error(), "already been removed") {
					walC <- WALData{} // indicates missing wal segment
				}
				c.processReady()
				return
			}

			switch tag {
			case 'd':
				b := ReadBuf(payload)
				tag = b.Byte()
				switch tag {
				case 'w':
					var p WALData
					p.Lsn = uint64(b.Int64())
					p.ServerLsn = uint64(b.Int64())
					p.ServerTime = time.Unix(0, b.Int64()*1000000)
					p.Data = []byte(b)
					//log.Print("walData! tag=", tag, " lsn=", p.Lsn, " serverLsn=", p.ServerLsn, " data=", len(p.Data))
					walC <- p
					// TODO: queue locally if sending would block, we'd need flow control on the channel
					clientLsn = p.Lsn
				case 'k':
					//log.Print("pg: ping received")
					b := WriteBuf{}
					b.Byte('r')
					b.Int64(int64(clientLsn))
					b.Int64(int64(clientLsn))
					b.Int64(int64(clientLsn))
					b.Int64(pgEpoch())
					b.Byte(0)
					c.send('d', b)
				}
			default:
				log.Print("pg: StartReplication unknown tag=", string(tag))
			}
		}
	}()
	return walC, nil
}

// pgEpoch returns microseconds since Jan 1, 2000
func pgEpoch() int64 {
	return time.Since(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)).Nanoseconds() / 1000
}

func (c *Conn) BaseBackup(q string) (int, string, <-chan []byte, error) {
	b := WriteBuf{}
	b.String(q)
	c.send('Q', b)

	rows, err := c.processResult()
	if err != nil {
		return 0, "", nil, err
	}
	if len(rows) != 1 || len(rows[0]) != 2 {
		return 0, "", nil, errProtocol
	}
	startLsn := rows[0][0].(string)
	timeline := rows[0][1].(int64)

	rows, err = c.processResult()
	if err != nil {
		return 0, "", nil, err
	}
	//log.Print("tblMappingRow=", rows)

	bbC := make(chan []byte)
	go func() {
		done := false
		for !done {
			tag, payload, err := c.recv()
			if err != nil {
				log.Print("pg: BaseBackup err=", err)
				close(bbC)
				return
			}

			switch tag {
			case 'H': // CopyOutResponse
			case 'd': // CopyData
				bbC <- payload
			case 'c': // CopyDone
				done = true
			default:
				log.Print("pg: BaseBackup unknown tag=", string(tag))
			}
		}

		rows, _ := c.processResult()
		log.Print("pg: BaseBackup end=", rows[0])

		c.processResult() // TODO: not sure why/if this is necessary

		bbC <- nil // indicates success

		close(bbC)
	}()

	return int(timeline), startLsn, bbC, nil
}
