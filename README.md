# ReRedis - A Redis Clone in Rust
AI generated readme

A lightweight Redis-compatible server implemented in Rust using Tokio for async I/O. This project implements the RESP (Redis Serialization Protocol) and supports many common Redis commands.

## Features

- **Async I/O**: Uses Tokio for efficient handling of multiple concurrent clients
- **RESP Protocol**: Full implementation of the Redis Serialization Protocol
- **Multiple Data Types**: Supports strings, lists, sets, and hashes
- **Key Expiration**: TTL support with automatic cleanup of expired keys
- **Thread-Safe**: Safe concurrent access using `Arc<RwLock<_>>`

## Supported Commands

### Connection
- `PING [message]` - Test connection, returns PONG or the message
- `ECHO message` - Returns the message
- `QUIT` - Close the connection
- `INFO [section]` - Get server information
- `DBSIZE` - Return the number of keys
- `COMMAND` - Get command information (for redis-cli compatibility)
- `CONFIG GET pattern` - Get configuration (basic support)
- `CLIENT SETINFO/SETNAME/GETNAME/LIST/ID` - Client commands

### Strings
- `SET key value [EX seconds] [PX ms] [NX|XX] [GET]` - Set a key
- `GET key` - Get a key's value
- `SETNX key value` - Set if not exists
- `SETEX key seconds value` - Set with expiration (seconds)
- `PSETEX key ms value` - Set with expiration (milliseconds)
- `GETSET key value` - Set and return old value
- `MSET key value [key value ...]` - Set multiple keys
- `MGET key [key ...]` - Get multiple keys
- `INCR key` - Increment by 1
- `INCRBY key delta` - Increment by delta
- `DECR key` - Decrement by 1
- `DECRBY key delta` - Decrement by delta
- `APPEND key value` - Append to string
- `STRLEN key` - Get string length

### Keys
- `DEL key [key ...]` - Delete keys
- `EXISTS key [key ...]` - Check if keys exist
- `EXPIRE key seconds` - Set expiration (seconds)
- `PEXPIRE key ms` - Set expiration (milliseconds)
- `TTL key` - Get time to live (seconds)
- `PTTL key` - Get time to live (milliseconds)
- `PERSIST key` - Remove expiration
- `KEYS pattern` - Find keys matching pattern (supports `*` and `?`)
- `TYPE key` - Get the type of a key
- `RENAME oldkey newkey` - Rename a key
- `RENAMENX oldkey newkey` - Rename if newkey doesn't exist
- `FLUSHDB` - Delete all keys
- `FLUSHALL` - Delete all keys (same as FLUSHDB)

### Lists
- `LPUSH key value [value ...]` - Push to left
- `RPUSH key value [value ...]` - Push to right
- `LPOP key` - Pop from left
- `RPOP key` - Pop from right
- `LLEN key` - Get list length
- `LRANGE key start stop` - Get range of elements
- `LINDEX key index` - Get element at index
- `LSET key index value` - Set element at index

### Sets
- `SADD key member [member ...]` - Add members
- `SREM key member [member ...]` - Remove members
- `SMEMBERS key` - Get all members
- `SISMEMBER key member` - Check if member exists
- `SCARD key` - Get set cardinality

### Hashes
- `HSET key field value [field value ...]` - Set hash fields
- `HGET key field` - Get hash field
- `HMSET key field value [field value ...]` - Set multiple fields
- `HMGET key field [field ...]` - Get multiple fields
- `HGETALL key` - Get all fields and values
- `HDEL key field [field ...]` - Delete fields
- `HEXISTS key field` - Check if field exists
- `HLEN key` - Get number of fields
- `HKEYS key` - Get all field names
- `HVALS key` - Get all values
- `HINCRBY key field delta` - Increment field value

## Building

```bash
cargo build --release
```

## Running

```bash
cargo run --release
```

The server will start listening on `127.0.0.1:6379` (the default Redis port).

## Usage

You can connect using any Redis client, including `redis-cli`:

```bash
redis-cli

127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET mykey "Hello World"
OK

127.0.0.1:6379> GET mykey
"Hello World"

127.0.0.1:6379> INCR counter
(integer) 1

127.0.0.1:6379> LPUSH mylist "a" "b" "c"
(integer) 3

127.0.0.1:6379> LRANGE mylist 0 -1
1) "c"
2) "b"
3) "a"

127.0.0.1:6379> HSET myhash field1 value1 field2 value2
(integer) 2

127.0.0.1:6379> HGETALL myhash
1) "field1"
2) "value1"
3) "field2"
4) "value2"
```

## Architecture

```
src/
├── main.rs       # Entry point, TCP server, client handling
├── parser.rs     # RESP protocol parser
├── commands.rs   # Command parsing and execution
└── storage.rs    # Thread-safe key-value storage
```

### Components

- **Parser** (`parser.rs`): Implements RESP protocol parsing for all five data types:
  - Simple Strings (`+`)
  - Errors (`-`)
  - Integers (`:`)
  - Bulk Strings (`$`)
  - Arrays (`*`)

- **Storage** (`storage.rs`): Thread-safe storage engine supporting:
  - Multiple data types (String, List, Set, Hash)
  - Key expiration with lazy + active cleanup
  - Glob pattern matching for KEYS command

- **Commands** (`commands.rs`): Command execution layer that:
  - Parses commands from RESP format
  - Executes commands against the storage
  - Encodes responses back to RESP format

- **Server** (`main.rs`): Async TCP server using Tokio:
  - Accepts concurrent client connections
  - Spawns a task per client
  - Background task for expired key cleanup

## Testing

Run the built-in tests:

```bash
cargo test
```

## Limitations

- No persistence (RDB/AOF) - data is stored in memory only
- No clustering or replication
- No Lua scripting
- No pub/sub
- No transactions (MULTI/EXEC)
- No blocking operations (BLPOP, BRPOP, etc.)

## License

This project is for educational purposes.
