
pgbackup: *.go pg/*.go
	go build -o $@ *.go
