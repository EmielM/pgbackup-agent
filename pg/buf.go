package pg

import (
	"bytes"
	"encoding/binary"
	"log"
)

type ReadBuf []byte

func (b *ReadBuf) Int16() int16 {
	n := int16(binary.BigEndian.Uint16(*b))
	*b = (*b)[2:]
	return n
}

func (b *ReadBuf) Int32() int32 {
	n := int32(binary.BigEndian.Uint32(*b))
	*b = (*b)[4:]
	return n
}

func (b *ReadBuf) Int64() int64 {
	n := int64(binary.BigEndian.Uint64(*b))
	*b = (*b)[8:]
	return n
}

func (b *ReadBuf) String() string {
	i := bytes.IndexByte(*b, 0)
	if i < 0 {
		log.Print("pgrepl: no string terminator")
		return ""
	}
	s := (*b)[:i]
	*b = (*b)[i+1:]
	return string(s)
}

func (b *ReadBuf) Bytes(n int) (v []byte) {
	v = (*b)[:n]
	*b = (*b)[n:]
	return
}

func (b *ReadBuf) Byte() byte {
	v := (*b)[0]
	*b = (*b)[1:]
	return v
}

type WriteBuf []byte

func (b *WriteBuf) Int16(n int) {
	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(n))
	*b = append(*b, x...)
}

func (b *WriteBuf) Int32(n int) {
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(n))
	*b = append(*b, x...)
}

func (b *WriteBuf) Int64(n int64) {
	x := make([]byte, 8)
	binary.BigEndian.PutUint64(x, uint64(n))
	*b = append(*b, x...)
}

func (b *WriteBuf) String(s string) {
	*b = append(*b, (s + "\000")...)
}

func (b *WriteBuf) Bytes(v []byte) {
	*b = append(*b, v...)
}

func (b *WriteBuf) Byte(c byte) {
	*b = append(*b, c)
}
