package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
)

type Backend struct {
	C net.Conn
}

func Connect() (*Backend, error) {

	// To deterministically create a ecdsa private key on the P256() curve, we
	// need 40 bytes of entropy: 256/8+8: https://golang.org/src/crypto/ecdsa/ecdsa.go#L89
	// As it happens, we have 32 bytes of config.key, and 8 bytes of config.SystemId

	var material [40]byte
	material0 := sha256.Sum256(config.key[:]) // to be sure we don't leak our key through NSA A.2.1
	copy(material[0:32], material0[:])
	binary.BigEndian.PutUint64(material[32:40], config.SystemId)

	key, err := ecdsa.GenerateKey(elliptic.P256(), bytes.NewReader(material[:]))
	if err != nil {
		return nil, err
	}

	// might be useful to debug client/server account mismatch:
	//account0, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	//account1 := sha256.Sum256(account0)
	//account := base64.RawURLEncoding.EncodeToString(account1[0:12])
	//log.Print("clientAccount: ", account)

	cert := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"pgbackup.com"},
		},
	}
	// from *x509.Certificate to tls.Certificate, we encode/decode der, that's a bit weird
	der0, _ := x509.CreateCertificate(rand.Reader, &cert, &cert, &key.PublicKey, key)
	der1, _ := x509.MarshalECPrivateKey(key)
	tlsCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der0}),
		pem.EncodeToMemory(&pem.Block{Type: "ECDSA PRIVATE KEY", Bytes: der1}),
	)
	if err != nil {
		return nil, err
	}

	conn, err := tls.Dial("tcp", "pgbackup.com:54321", &tls.Config{
		CipherSuites: []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA},
		Certificates: []tls.Certificate{tlsCert},
	})
	if err != nil {
		return nil, err
	}

	b := &Backend{C: conn}

	err = b.Send("pgbackup.put email")
	if err != nil {
		return nil, err
	}
	cw := &chunkWriter{W: b.C}
	_, err = cw.Write(([]byte)(config.Email))
	if err != nil {
		return nil, err
	}
	err = cw.Close()
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (b Backend) Send(s string) error {
	_, err := b.C.Write(([]byte)(s + "\n"))
	return err
}

func (b Backend) Request(s string) (string, error) {

	err := b.Send(s)
	if err != nil {
		return "", err
	}

	// read line byte-for-byte
	var l []byte
	for {
		var buf [1]byte
		_, err := b.C.Read(buf[:])
		if err != nil {
			return "", err
		}
		if buf[0] == '\n' {
			return string(l), nil
		}
		l = append(l, buf[0])
	}
}

func (b Backend) Close() error {
	return b.C.Close()
}

type chunkWriter struct {
	W io.Writer
}

func (cw chunkWriter) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	_, err := cw.W.Write(([]byte)(fmt.Sprintf("%x\n", len(b))))
	if err != nil {
		return 0, err
	}
	return cw.W.Write(b)
}

func (cw chunkWriter) Close() error {
	_, err := cw.W.Write([]byte("0\n"))
	if err != nil {
		return err
	}
	// not closing .W, are these correct semantics?
	return nil
}
