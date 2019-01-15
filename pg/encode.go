package pg

import (
	"encoding/binary"
	"log"
	"strconv"
)

const formatText uint16 = 0
const formatBinary uint16 = 1

func (c *Conn) decode(raw []byte, colType uint32, colFormat uint16) interface{} {
	if colFormat == formatBinary {
		return c.decodeBinary(raw, colType, colFormat)
	} else {
		return c.decodeText(raw, colType, colFormat)
	}
}

func (c *Conn) decodeBinary(raw []byte, colType uint32, colFormat uint16) interface{} {
	switch colType {
	case 17: // T_bytea
		return raw
	case 20: // T_int8
		return int64(binary.BigEndian.Uint64(raw))
	case 23: // T_int4
		return int64(int32(binary.BigEndian.Uint32(raw)))
	case 21: // T_int2
		return int64(int16(binary.BigEndian.Uint16(raw)))
	default:
		log.Print("pgrepl: can't decodeBinary colType=", colType)
	}
	return nil
}

func (c *Conn) decodeText(raw []byte, colType uint32, colFormat uint16) interface{} {
	switch colType {
	case 18, 1043, 25: // T_char, T_varchar, T_text
		return string(raw)
	case 17: // T_bytea
		return "todo"
	case 16: // T_bool
		return raw[0] == 'T'
	case 20, 23, 21: // T_int8, T_int4, T_int2
		i, _ := strconv.ParseInt(string(raw), 10, 64)
		return i
	case 700: // T_float4
		f, _ := strconv.ParseFloat(string(raw), 32)
		return f
	case 701: // T_float8
		f, _ := strconv.ParseFloat(string(raw), 64)
		return f
	default:
		log.Print("pgrepl: can't decodeText colType=", colType)
	}
	return nil
}

func (c *Conn) encode(value interface{}, colType uint32, colFormat uint16) []byte {

	return nil

}
