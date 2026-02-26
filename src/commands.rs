use crate::parser::Resp;
use crate::storage::Storage;

#[derive(Debug)]
pub struct Command {
    pub name: String,
    pub args: Vec<String>,
}

impl Command {
    pub fn from_resp(resp: &Resp) -> Result<Command, String> {
        match resp {
            Resp::Array(Some(items)) => {
                if items.is_empty() {
                    return Err("ERR empty command".to_string());
                }

                let mut args = Vec::new();
                for item in items {
                    match item {
                        Resp::Bulk(Some(s)) => args.push(s.clone()),
                        Resp::Simple(s) => args.push(s.clone()),
                        _ => return Err("ERR invalid command format".to_string()),
                    }
                }

                let name = args.remove(0).to_uppercase();
                Ok(Command { name, args })
            }
            Resp::Simple(s) => {
                let parts: Vec<&str> = s.split_whitespace().collect();
                if parts.is_empty() {
                    return Err("ERR empty command".to_string());
                }
                let name = parts[0].to_uppercase();
                let args = parts[1..].iter().map(|s| s.to_string()).collect();
                Ok(Command { name, args })
            }
            _ => Err("ERR invalid command format".to_string()),
        }
    }
}

pub fn execute(cmd: &Command, storage: &Storage) -> Resp {
    match cmd.name.as_str() {
        "PING" => cmd_ping(cmd),
        "ECHO" => cmd_echo(cmd),
        "QUIT" => cmd_quit(),
        "COMMAND" => cmd_command(cmd),
        "CONFIG" => cmd_config(cmd),
        "CLIENT" => cmd_client(cmd),
        "INFO" => cmd_info(cmd, storage),
        "DBSIZE" => cmd_dbsize(storage),

        "SET" => cmd_set(cmd, storage),
        "GET" => cmd_get(cmd, storage),
        "SETNX" => cmd_setnx(cmd, storage),
        "SETEX" => cmd_setex(cmd, storage),
        "PSETEX" => cmd_psetex(cmd, storage),
        "GETSET" => cmd_getset(cmd, storage),
        "MSET" => cmd_mset(cmd, storage),
        "MGET" => cmd_mget(cmd, storage),
        "INCR" => cmd_incr(cmd, storage),
        "INCRBY" => cmd_incrby(cmd, storage),
        "DECR" => cmd_decr(cmd, storage),
        "DECRBY" => cmd_decrby(cmd, storage),
        "APPEND" => cmd_append(cmd, storage),
        "STRLEN" => cmd_strlen(cmd, storage),

        "DEL" => cmd_del(cmd, storage),
        "EXISTS" => cmd_exists(cmd, storage),
        "EXPIRE" => cmd_expire(cmd, storage),
        "PEXPIRE" => cmd_pexpire(cmd, storage),
        "TTL" => cmd_ttl(cmd, storage),
        "PTTL" => cmd_pttl(cmd, storage),
        "PERSIST" => cmd_persist(cmd, storage),
        "KEYS" => cmd_keys(cmd, storage),
        "TYPE" => cmd_type(cmd, storage),
        "RENAME" => cmd_rename(cmd, storage),
        "RENAMENX" => cmd_renamenx(cmd, storage),
        "FLUSHDB" => cmd_flushdb(storage),
        "FLUSHALL" => cmd_flushdb(storage),

        "LPUSH" => cmd_lpush(cmd, storage),
        "RPUSH" => cmd_rpush(cmd, storage),
        "LPOP" => cmd_lpop(cmd, storage),
        "RPOP" => cmd_rpop(cmd, storage),
        "LLEN" => cmd_llen(cmd, storage),
        "LRANGE" => cmd_lrange(cmd, storage),
        "LINDEX" => cmd_lindex(cmd, storage),
        "LSET" => cmd_lset(cmd, storage),

        "SADD" => cmd_sadd(cmd, storage),
        "SREM" => cmd_srem(cmd, storage),
        "SMEMBERS" => cmd_smembers(cmd, storage),
        "SISMEMBER" => cmd_sismember(cmd, storage),
        "SCARD" => cmd_scard(cmd, storage),

        "HSET" => cmd_hset(cmd, storage),
        "HGET" => cmd_hget(cmd, storage),
        "HMSET" => cmd_hmset(cmd, storage),
        "HMGET" => cmd_hmget(cmd, storage),
        "HGETALL" => cmd_hgetall(cmd, storage),
        "HDEL" => cmd_hdel(cmd, storage),
        "HEXISTS" => cmd_hexists(cmd, storage),
        "HLEN" => cmd_hlen(cmd, storage),
        "HKEYS" => cmd_hkeys(cmd, storage),
        "HVALS" => cmd_hvals(cmd, storage),
        "HINCRBY" => cmd_hincrby(cmd, storage),

        _ => Resp::Error(format!("ERR unknown command '{}'", cmd.name)),
    }
}

fn cmd_ping(cmd: &Command) -> Resp {
    if cmd.args.is_empty() {
        Resp::Simple("PONG".to_string())
    } else {
        Resp::Bulk(Some(cmd.args[0].clone()))
    }
}

fn cmd_echo(cmd: &Command) -> Resp {
    if cmd.args.is_empty() {
        Resp::Error("ERR wrong number of arguments for 'echo' command".to_string())
    } else {
        Resp::Bulk(Some(cmd.args[0].clone()))
    }
}

fn cmd_quit() -> Resp {
    Resp::Simple("OK".to_string())
}

fn cmd_command(cmd: &Command) -> Resp {
    if cmd.args.is_empty() || cmd.args[0].to_uppercase() == "DOCS" {
        Resp::Array(Some(vec![]))
    } else if cmd.args[0].to_uppercase() == "COUNT" {
        Resp::Integer(40)
    } else {
        Resp::Array(Some(vec![]))
    }
}

fn cmd_config(cmd: &Command) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'config' command".to_string());
    }

    match cmd.args[0].to_uppercase().as_str() {
        "GET" => {
            if cmd.args.len() < 2 {
                return Resp::Error(
                    "ERR wrong number of arguments for 'config|get' command".to_string(),
                );
            }

            let pattern = &cmd.args[1];
            if pattern == "save" || pattern == "*" {
                Resp::Array(Some(vec![
                    Resp::Bulk(Some("save".to_string())),
                    Resp::Bulk(Some("".to_string())),
                ]))
            } else {
                Resp::Array(Some(vec![]))
            }
        }
        "SET" => Resp::Simple("OK".to_string()),
        _ => Resp::Error(format!("ERR Unknown subcommand '{}'", cmd.args[0])),
    }
}

fn cmd_client(cmd: &Command) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'client' command".to_string());
    }

    match cmd.args[0].to_uppercase().as_str() {
        "SETINFO" => Resp::Simple("OK".to_string()),
        "SETNAME" => Resp::Simple("OK".to_string()),
        "GETNAME" => Resp::Bulk(None),
        "LIST" => Resp::Bulk(Some("id=1 addr=127.0.0.1:0 fd=1 name= db=0\n".to_string())),
        "ID" => Resp::Integer(1),
        _ => Resp::Simple("OK".to_string()),
    }
}

fn cmd_info(cmd: &Command, storage: &Storage) -> Resp {
    let section = cmd.args.get(0).map(|s| s.to_uppercase());

    let mut info = String::new();

    if section.is_none()
        || section.as_deref() == Some("SERVER")
        || section.as_deref() == Some("ALL")
    {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0-reredis\r\n");
        info.push_str("redis_mode:standalone\r\n");
        info.push_str("os:Linux\r\n");
        info.push_str("arch_bits:64\r\n");
        info.push_str("tcp_port:6379\r\n");
        info.push_str("\r\n");
    }

    if section.is_none()
        || section.as_deref() == Some("KEYSPACE")
        || section.as_deref() == Some("ALL")
    {
        info.push_str("# Keyspace\r\n");
        let db_size = storage.dbsize();
        if db_size > 0 {
            info.push_str(&format!("db0:keys={},expires=0,avg_ttl=0\r\n", db_size));
        }
    }

    Resp::Bulk(Some(info))
}

fn cmd_dbsize(storage: &Storage) -> Resp {
    Resp::Integer(storage.dbsize() as i64)
}

fn cmd_set(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'set' command".to_string());
    }

    let key = cmd.args[0].clone();
    let value = cmd.args[1].clone();

    let mut expiry_ms: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;

    let mut i = 2;
    while i < cmd.args.len() {
        match cmd.args[i].to_uppercase().as_str() {
            "EX" => {
                if i + 1 >= cmd.args.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                match cmd.args[i + 1].parse::<u64>() {
                    Ok(secs) => expiry_ms = Some(secs * 1000),
                    Err(_) => {
                        return Resp::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        );
                    }
                }
                i += 2;
            }
            "PX" => {
                if i + 1 >= cmd.args.len() {
                    return Resp::Error("ERR syntax error".to_string());
                }
                match cmd.args[i + 1].parse::<u64>() {
                    Ok(ms) => expiry_ms = Some(ms),
                    Err(_) => {
                        return Resp::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        );
                    }
                }
                i += 2;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                get = true;
                i += 1;
            }
            "KEEPTTL" => {
                i += 1;
            }
            _ => {
                return Resp::Error("ERR syntax error".to_string());
            }
        }
    }

    let exists = storage.get(&key).is_some();
    if nx && exists {
        return if get {
            match storage.get(&key) {
                Some(v) => Resp::Bulk(Some(v)),
                None => Resp::Bulk(None),
            }
        } else {
            Resp::Bulk(None)
        };
    }
    if xx && !exists {
        return if get {
            Resp::Bulk(None)
        } else {
            Resp::Bulk(None)
        };
    }

    let old_value = if get { storage.get(&key) } else { None };

    match expiry_ms {
        Some(ms) => storage.set_with_expiry(key, value, ms),
        None => storage.set(key, value),
    }

    if get {
        match old_value {
            Some(v) => Resp::Bulk(Some(v)),
            None => Resp::Bulk(None),
        }
    } else {
        Resp::Simple("OK".to_string())
    }
}

fn cmd_get(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'get' command".to_string());
    }

    match storage.get(&cmd.args[0]) {
        Some(value) => Resp::Bulk(Some(value)),
        None => Resp::Bulk(None),
    }
}

fn cmd_setnx(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'setnx' command".to_string());
    }

    let key = cmd.args[0].clone();
    let value = cmd.args[1].clone();

    if storage.setnx(key, value) {
        Resp::Integer(1)
    } else {
        Resp::Integer(0)
    }
}

fn cmd_setex(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'setex' command".to_string());
    }

    let key = cmd.args[0].clone();
    let seconds: u64 = match cmd.args[1].parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let value = cmd.args[2].clone();

    storage.set_with_expiry(key, value, seconds * 1000);
    Resp::Simple("OK".to_string())
}

fn cmd_psetex(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'psetex' command".to_string());
    }

    let key = cmd.args[0].clone();
    let ms: u64 = match cmd.args[1].parse() {
        Ok(m) => m,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let value = cmd.args[2].clone();

    storage.set_with_expiry(key, value, ms);
    Resp::Simple("OK".to_string())
}

fn cmd_getset(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'getset' command".to_string());
    }

    let key = cmd.args[0].clone();
    let value = cmd.args[1].clone();

    match storage.getset(key, value) {
        Some(old) => Resp::Bulk(Some(old)),
        None => Resp::Bulk(None),
    }
}

fn cmd_mset(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() || cmd.args.len() % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'mset' command".to_string());
    }

    let pairs: Vec<(String, String)> = cmd
        .args
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    storage.mset(pairs);
    Resp::Simple("OK".to_string())
}

fn cmd_mget(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'mget' command".to_string());
    }

    let values = storage.mget(&cmd.args);
    let resp_values: Vec<Resp> = values
        .into_iter()
        .map(|v| match v {
            Some(s) => Resp::Bulk(Some(s)),
            None => Resp::Bulk(None),
        })
        .collect();

    Resp::Array(Some(resp_values))
}

fn cmd_incr(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'incr' command".to_string());
    }

    match storage.incr(&cmd.args[0]) {
        Ok(n) => Resp::Integer(n),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_incrby(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'incrby' command".to_string());
    }

    let delta: i64 = match cmd.args[1].parse() {
        Ok(d) => d,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    match storage.incr_by(&cmd.args[0], delta) {
        Ok(n) => Resp::Integer(n),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_decr(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'decr' command".to_string());
    }

    match storage.decr(&cmd.args[0]) {
        Ok(n) => Resp::Integer(n),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_decrby(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'decrby' command".to_string());
    }

    let delta: i64 = match cmd.args[1].parse() {
        Ok(d) => d,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    match storage.incr_by(&cmd.args[0], -delta) {
        Ok(n) => Resp::Integer(n),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_append(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'append' command".to_string());
    }

    match storage.append(&cmd.args[0], &cmd.args[1]) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_strlen(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'strlen' command".to_string());
    }

    match storage.strlen(&cmd.args[0]) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_del(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'del' command".to_string());
    }

    let count = storage.del(&cmd.args);
    Resp::Integer(count as i64)
}

fn cmd_exists(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'exists' command".to_string());
    }

    let count = storage.exists(&cmd.args);
    Resp::Integer(count as i64)
}

fn cmd_expire(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'expire' command".to_string());
    }

    let seconds: u64 = match cmd.args[1].parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if storage.expire(&cmd.args[0], seconds * 1000) {
        Resp::Integer(1)
    } else {
        Resp::Integer(0)
    }
}

fn cmd_pexpire(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'pexpire' command".to_string());
    }

    let ms: u64 = match cmd.args[1].parse() {
        Ok(m) => m,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    if storage.expire(&cmd.args[0], ms) {
        Resp::Integer(1)
    } else {
        Resp::Integer(0)
    }
}

fn cmd_ttl(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'ttl' command".to_string());
    }

    let ttl_ms = storage.ttl(&cmd.args[0]);
    if ttl_ms == -2 || ttl_ms == -1 {
        Resp::Integer(ttl_ms)
    } else {
        Resp::Integer(ttl_ms / 1000)
    }
}

fn cmd_pttl(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'pttl' command".to_string());
    }

    Resp::Integer(storage.ttl(&cmd.args[0]))
}

fn cmd_persist(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'persist' command".to_string());
    }

    if storage.persist(&cmd.args[0]) {
        Resp::Integer(1)
    } else {
        Resp::Integer(0)
    }
}

fn cmd_keys(cmd: &Command, storage: &Storage) -> Resp {
    let pattern = cmd.args.get(0).map(|s| s.as_str()).unwrap_or("*");
    let keys = storage.keys(pattern);
    let resp_keys: Vec<Resp> = keys.into_iter().map(|k| Resp::Bulk(Some(k))).collect();
    Resp::Array(Some(resp_keys))
}

fn cmd_type(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'type' command".to_string());
    }

    match storage.get_type(&cmd.args[0]) {
        Some(t) => Resp::Simple(t.to_string()),
        None => Resp::Simple("none".to_string()),
    }
}

fn cmd_rename(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'rename' command".to_string());
    }

    match storage.rename(&cmd.args[0], &cmd.args[1]) {
        Ok(()) => Resp::Simple("OK".to_string()),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_renamenx(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'renamenx' command".to_string());
    }

    match storage.renamenx(&cmd.args[0], &cmd.args[1]) {
        Ok(true) => Resp::Integer(1),
        Ok(false) => Resp::Integer(0),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_flushdb(storage: &Storage) -> Resp {
    storage.flushdb();
    Resp::Simple("OK".to_string())
}

fn cmd_lpush(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'lpush' command".to_string());
    }

    let key = &cmd.args[0];
    let values: Vec<String> = cmd.args[1..].to_vec();

    match storage.lpush(key, values) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_rpush(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'rpush' command".to_string());
    }

    let key = &cmd.args[0];
    let values: Vec<String> = cmd.args[1..].to_vec();

    match storage.rpush(key, values) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_lpop(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'lpop' command".to_string());
    }

    match storage.lpop(&cmd.args[0]) {
        Ok(Some(v)) => Resp::Bulk(Some(v)),
        Ok(None) => Resp::Bulk(None),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_rpop(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'rpop' command".to_string());
    }

    match storage.rpop(&cmd.args[0]) {
        Ok(Some(v)) => Resp::Bulk(Some(v)),
        Ok(None) => Resp::Bulk(None),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_llen(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'llen' command".to_string());
    }

    match storage.llen(&cmd.args[0]) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_lrange(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'lrange' command".to_string());
    }

    let key = &cmd.args[0];
    let start: i64 = match cmd.args[1].parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let stop: i64 = match cmd.args[2].parse() {
        Ok(s) => s,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    match storage.lrange(key, start, stop) {
        Ok(values) => {
            let resp_values: Vec<Resp> = values.into_iter().map(|v| Resp::Bulk(Some(v))).collect();
            Resp::Array(Some(resp_values))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_lindex(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'lindex' command".to_string());
    }

    let key = &cmd.args[0];
    let index: i64 = match cmd.args[1].parse() {
        Ok(i) => i,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    match storage.lindex(key, index) {
        Ok(Some(v)) => Resp::Bulk(Some(v)),
        Ok(None) => Resp::Bulk(None),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_lset(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'lset' command".to_string());
    }

    let key = &cmd.args[0];
    let index: i64 = match cmd.args[1].parse() {
        Ok(i) => i,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };
    let value = cmd.args[2].clone();

    match storage.lset(key, index, value) {
        Ok(()) => Resp::Simple("OK".to_string()),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_sadd(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'sadd' command".to_string());
    }

    let key = &cmd.args[0];
    let members: Vec<String> = cmd.args[1..].to_vec();

    match storage.sadd(key, members) {
        Ok(added) => Resp::Integer(added as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_srem(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'srem' command".to_string());
    }

    let key = &cmd.args[0];
    let members: Vec<String> = cmd.args[1..].to_vec();

    match storage.srem(key, members) {
        Ok(removed) => Resp::Integer(removed as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_smembers(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'smembers' command".to_string());
    }

    match storage.smembers(&cmd.args[0]) {
        Ok(members) => {
            let resp_members: Vec<Resp> =
                members.into_iter().map(|m| Resp::Bulk(Some(m))).collect();
            Resp::Array(Some(resp_members))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_sismember(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'sismember' command".to_string());
    }

    match storage.sismember(&cmd.args[0], &cmd.args[1]) {
        Ok(true) => Resp::Integer(1),
        Ok(false) => Resp::Integer(0),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_scard(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'scard' command".to_string());
    }

    match storage.scard(&cmd.args[0]) {
        Ok(card) => Resp::Integer(card as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hset(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 || (cmd.args.len() - 1) % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'hset' command".to_string());
    }

    let key = &cmd.args[0];
    let mut added = 0;

    for chunk in cmd.args[1..].chunks(2) {
        let field = chunk[0].clone();
        let value = chunk[1].clone();
        match storage.hset(key, field, value) {
            Ok(is_new) => {
                if is_new {
                    added += 1;
                }
            }
            Err(e) => return Resp::Error(e),
        }
    }

    Resp::Integer(added)
}

fn cmd_hget(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'hget' command".to_string());
    }

    match storage.hget(&cmd.args[0], &cmd.args[1]) {
        Ok(Some(v)) => Resp::Bulk(Some(v)),
        Ok(None) => Resp::Bulk(None),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hmset(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 || (cmd.args.len() - 1) % 2 != 0 {
        return Resp::Error("ERR wrong number of arguments for 'hmset' command".to_string());
    }

    let key = &cmd.args[0];
    let pairs: Vec<(String, String)> = cmd.args[1..]
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    match storage.hmset(key, pairs) {
        Ok(()) => Resp::Simple("OK".to_string()),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hmget(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'hmget' command".to_string());
    }

    let key = &cmd.args[0];
    let fields: Vec<String> = cmd.args[1..].to_vec();

    match storage.hmget(key, &fields) {
        Ok(values) => {
            let resp_values: Vec<Resp> = values
                .into_iter()
                .map(|v| match v {
                    Some(s) => Resp::Bulk(Some(s)),
                    None => Resp::Bulk(None),
                })
                .collect();
            Resp::Array(Some(resp_values))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hgetall(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'hgetall' command".to_string());
    }

    match storage.hgetall(&cmd.args[0]) {
        Ok(pairs) => {
            let mut resp_values: Vec<Resp> = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                resp_values.push(Resp::Bulk(Some(k)));
                resp_values.push(Resp::Bulk(Some(v)));
            }
            Resp::Array(Some(resp_values))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hdel(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'hdel' command".to_string());
    }

    let key = &cmd.args[0];
    let fields: Vec<String> = cmd.args[1..].to_vec();

    match storage.hdel(key, fields) {
        Ok(removed) => Resp::Integer(removed as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hexists(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 2 {
        return Resp::Error("ERR wrong number of arguments for 'hexists' command".to_string());
    }

    match storage.hexists(&cmd.args[0], &cmd.args[1]) {
        Ok(true) => Resp::Integer(1),
        Ok(false) => Resp::Integer(0),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hlen(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'hlen' command".to_string());
    }

    match storage.hlen(&cmd.args[0]) {
        Ok(len) => Resp::Integer(len as i64),
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hkeys(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'hkeys' command".to_string());
    }

    match storage.hkeys(&cmd.args[0]) {
        Ok(keys) => {
            let resp_keys: Vec<Resp> = keys.into_iter().map(|k| Resp::Bulk(Some(k))).collect();
            Resp::Array(Some(resp_keys))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hvals(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.is_empty() {
        return Resp::Error("ERR wrong number of arguments for 'hvals' command".to_string());
    }

    match storage.hvals(&cmd.args[0]) {
        Ok(vals) => {
            let resp_vals: Vec<Resp> = vals.into_iter().map(|v| Resp::Bulk(Some(v))).collect();
            Resp::Array(Some(resp_vals))
        }
        Err(e) => Resp::Error(e),
    }
}

fn cmd_hincrby(cmd: &Command, storage: &Storage) -> Resp {
    if cmd.args.len() < 3 {
        return Resp::Error("ERR wrong number of arguments for 'hincrby' command".to_string());
    }

    let key = &cmd.args[0];
    let field = &cmd.args[1];
    let delta: i64 = match cmd.args[2].parse() {
        Ok(d) => d,
        Err(_) => return Resp::Error("ERR value is not an integer or out of range".to_string()),
    };

    match storage.hincrby(key, field, delta) {
        Ok(n) => Resp::Integer(n),
        Err(e) => Resp::Error(e),
    }
}

pub fn encode_resp(resp: &Resp) -> Vec<u8> {
    match resp {
        Resp::Simple(s) => format!("+{}\r\n", s).into_bytes(),
        Resp::Error(e) => format!("-{}\r\n", e).into_bytes(),
        Resp::Integer(i) => format!(":{}\r\n", i).into_bytes(),
        Resp::Bulk(None) => b"$-1\r\n".to_vec(),
        Resp::Bulk(Some(s)) => {
            let mut result = format!("${}\r\n", s.len()).into_bytes();
            result.extend(s.as_bytes());
            result.extend(b"\r\n");
            result
        }
        Resp::Array(None) => b"*-1\r\n".to_vec(),
        Resp::Array(Some(items)) => {
            let mut result = format!("*{}\r\n", items.len()).into_bytes();
            for item in items {
                result.extend(encode_resp(item));
            }
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping() {
        let storage = Storage::new();
        let cmd = Command {
            name: "PING".to_string(),
            args: vec![],
        };
        assert_eq!(execute(&cmd, &storage), Resp::Simple("PONG".to_string()));
    }

    #[test]
    fn test_ping_with_message() {
        let storage = Storage::new();
        let cmd = Command {
            name: "PING".to_string(),
            args: vec!["hello".to_string()],
        };
        assert_eq!(
            execute(&cmd, &storage),
            Resp::Bulk(Some("hello".to_string()))
        );
    }

    #[test]
    fn test_set_get() {
        let storage = Storage::new();
        let set_cmd = Command {
            name: "SET".to_string(),
            args: vec!["key".to_string(), "value".to_string()],
        };
        assert_eq!(execute(&set_cmd, &storage), Resp::Simple("OK".to_string()));

        let get_cmd = Command {
            name: "GET".to_string(),
            args: vec!["key".to_string()],
        };
        assert_eq!(
            execute(&get_cmd, &storage),
            Resp::Bulk(Some("value".to_string()))
        );
    }

    #[test]
    fn test_encode_resp() {
        assert_eq!(
            encode_resp(&Resp::Simple("OK".to_string())),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(
            encode_resp(&Resp::Error("ERR".to_string())),
            b"-ERR\r\n".to_vec()
        );
        assert_eq!(encode_resp(&Resp::Integer(42)), b":42\r\n".to_vec());
        assert_eq!(encode_resp(&Resp::Bulk(None)), b"$-1\r\n".to_vec());
        assert_eq!(
            encode_resp(&Resp::Bulk(Some("hello".to_string()))),
            b"$5\r\nhello\r\n".to_vec()
        );
    }
}
