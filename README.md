pgbackup agent
===

Pgbackup agent, use with https://pgbackup.com.

Build
-----
Build with `make` and a modern go env.

Don't want to build and feeling (l|cr)azy? Run `curl https://pgbackup.com/setup | sh`.

Setup backup
------------
- Run `pgbackup setup` on your database server, it will guide you through connecting to the local postgres database over a replica connection.
- It will need to connect as a user with the `LOGIN` and `REPLICATION` privileges.
  - If possible, create a separate `pgbackup` user and allow connecting over a local unix domain socket.
- Be sure to save the resulting `~/pgbackup.conf` to a safe place, as it contains the key needed to restore later.
- Run `pgbackup status` to check how things are going.

Restore backup
--------------
- Restore the previously saved `pgbackup.conf` to your homedir.
- Run `pgbackup status` to see if your backup is there and to where you could restore.
- Run `pgbackup restore [lsn] [dir]` to restore your db up to a certain LSN (eg 08/20003016) in a target dir.

Encryption
----------
- A one-time 256-bit key is generated during `pgbackup setup`.
  - Saved in `pgbackup.conf` in base64 form
- Wal segment and base backup files are encrypted using AES256
  - AES IV is derived from file name
- The key and postgres systemID deterministically generate a private key used for TLS connection to the pgbackup backend
  - The public part of this key is used as account identifier on the server (shown with `pgbackup status`)
  - In short, the 256-bit key and systemID always combine to the same account

