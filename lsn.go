package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%x/%08x", uint64(lsn)>>32, uint64(lsn)&0xffffffff)
}

func ParseLSN(s string) (LSN, error) {
	if ss := strings.Split(s, "/"); len(ss) == 2 {
		if a, err := strconv.ParseUint(ss[0], 16, 64); err == nil {
			if b, err := strconv.ParseUint(ss[1], 16, 64); err == nil {
				return LSN(a<<32 | b&0xffffffff), nil
			}
		}
	}
	return LSN(0), errors.New("illegalLSN")
}
