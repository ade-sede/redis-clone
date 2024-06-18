A Redis clone for educational purposes, following Codecrafter's ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

# Scope

- Replication
- Persistence
- Strings
- Streams (no persistence)
- Fullresync (RDB file over the network)
- Basic commands such as `SET`, `DEL`, `GET`, `WAIT`, `KEYS`, `XADD`, `XRANGE`, `XREAD` etc ...

# Usage

```bash 
# Spawn a first instance
$> ./spawn_redis_server.sh --port 6666 --dir $(pwd) --dbfilename dump.rdb

# Send data
$> redis-cli -p 6666 set mykey myvalue

# Query your data
$> redis-cli -p 6666 get mykey # myvalue

# Save to the disk
$> redis-cli -p 6666 save

# Restart the instance.
# The database should contain previously saved.

# Spawn a second instance
# As soon as it starts, it completes a handshake 
$> ./spawn_redis_server.sh --port 6667 --dir $(pwd) --dbfilename dump.rdb --replicaof "localhost 6666"

# Any further data sent to the first instance will be replicated to the second one.
$> redis-cli -p 6666 set anotherkey anothervalue
$> redis-cli -p 6667 get anotherkey # anothervalue
```

# TODO

- Fix flaky `read map while writing`
- Fix ordering of keys in stream maps
- Refactor types for everything stream related, too many []map[]map[]map[]...
- Figure out proper logic around returns for `XREAD` and `XRANGE`. Current code is spaghettie logic and full of undefined behavior
