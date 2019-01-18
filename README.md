pgbackup agent
===

Should add more info here, for now look at https://pgbackup.com for its purpose.

Build with `make` and a modern go env.

Encryption
---
- A one-time 256-bit key is generated during `pgbackup setup`.
  - Saved in `pgbackup.conf` in base64 form
- Wal segment and base backup files are encrypted using AES256
  - AES IV is derived from file name
- The key and postgres systemID deterministically generate a private key used for TLS connection to the pgbackup backend
  - The public part of this key is used as account identifier on the server (shown with `pgbackup status`)
  - In short, the 256-bit key and systemID always combine to the same account

