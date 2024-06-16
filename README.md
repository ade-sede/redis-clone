A naive Redis clone, following Codecrafter's ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

# Scope

- Strings only
- Replication
- Persistence
- Fullresync (RDB file over the network)
- Basic commands such as `SET`, `DEL`, `GET`, `WAIT`, `KEYS`, etc ...

This is a toy for educational purposes only.  
There is almost no error handling.

# Usage

```bash 
# Spawn a first instance
$> ./spawn_redis_server.sh --port 6666 --dir (pwd) --dbfilename dump.rdb

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
$> ./spawn_redis_server.sh --port 6667 --dir (pwd) --dbfilename dump.rdb --replicaof "localhost 6666"

# Any further data sent to the first instance will be replicated to the second one.
$> redis-cli -p 6666 set anotherkey anothervalue
$> redis-cli -p 6667 get anotherkey # anothervalue
```
