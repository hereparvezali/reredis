use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
}

#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: Value) -> Self {
        Entry {
            value,
            expires_at: None,
        }
    }

    fn with_expiry(value: Value, duration: Duration) -> Self {
        Entry {
            value,
            expires_at: Some(Instant::now() + duration),
        }
    }

    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() >= exp,
            None => false,
        }
    }

    fn ttl_ms(&self) -> Option<i64> {
        match self.expires_at {
            Some(exp) => {
                let now = Instant::now();
                if now >= exp {
                    Some(-2)
                } else {
                    Some((exp - now).as_millis() as i64)
                }
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    data: Arc<RwLock<HashMap<String, Entry>>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn cleanup_expired(&self) {
        let mut data = self.data.write().unwrap();
        data.retain(|_, entry| !entry.is_expired());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::String(s) = &entry.value {
                    Some(s.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_type(&self, key: &str) -> Option<&'static str> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => match &entry.value {
                Value::String(_) => Some("string"),
                Value::List(_) => Some("list"),
                Value::Set(_) => Some("set"),
                Value::Hash(_) => Some("hash"),
            },
            _ => None,
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut data = self.data.write().unwrap();
        data.insert(key, Entry::new(Value::String(value)));
    }

    pub fn set_with_expiry(&self, key: String, value: String, expiry_ms: u64) {
        let mut data = self.data.write().unwrap();
        let entry = Entry::with_expiry(Value::String(value), Duration::from_millis(expiry_ms));
        data.insert(key, entry);
    }

    pub fn expire(&self, key: &str, expiry_ms: u64) -> bool {
        let mut data = self.data.write().unwrap();
        if let Some(entry) = data.get_mut(key) {
            if !entry.is_expired() {
                entry.expires_at = Some(Instant::now() + Duration::from_millis(expiry_ms));
                return true;
            }
        }
        false
    }

    pub fn persist(&self, key: &str) -> bool {
        let mut data = self.data.write().unwrap();
        if let Some(entry) = data.get_mut(key) {
            if !entry.is_expired() && entry.expires_at.is_some() {
                entry.expires_at = None;
                return true;
            }
        }
        false
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => entry.ttl_ms().unwrap_or(-1),
            _ => -2,
        }
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let mut data = self.data.write().unwrap();
        let mut count = 0;
        for key in keys {
            if data.remove(key).is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        let data = self.data.read().unwrap();
        keys.iter()
            .filter(|key| data.get(*key).map(|e| !e.is_expired()).unwrap_or(false))
            .count()
    }

    pub fn incr(&self, key: &str) -> Result<i64, String> {
        self.incr_by(key, 1)
    }

    pub fn decr(&self, key: &str) -> Result<i64, String> {
        self.incr_by(key, -1)
    }

    pub fn incr_by(&self, key: &str, delta: i64) -> Result<i64, String> {
        let mut data = self.data.write().unwrap();
        let entry = data.get(key);

        let current = match entry {
            Some(e) if !e.is_expired() => {
                if let Value::String(s) = &e.value {
                    s.parse::<i64>()
                        .map_err(|_| "ERR value is not an integer or out of range".to_string())?
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            _ => 0,
        };

        let new_value = current
            .checked_add(delta)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;

        data.insert(
            key.to_string(),
            Entry::new(Value::String(new_value.to_string())),
        );
        Ok(new_value)
    }

    pub fn append(&self, key: &str, value: &str) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        let entry = data.get(key);

        let new_value = match entry {
            Some(e) if !e.is_expired() => {
                if let Value::String(s) = &e.value {
                    format!("{}{}", s, value)
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }
            _ => value.to_string(),
        };

        let len = new_value.len();
        data.insert(key.to_string(), Entry::new(Value::String(new_value)));
        Ok(len)
    }

    pub fn strlen(&self, key: &str) -> Result<usize, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::String(s) = &entry.value {
                    Ok(s.len())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn setnx(&self, key: String, value: String) -> bool {
        let mut data = self.data.write().unwrap();

        let exists = data.get(&key).map(|e| !e.is_expired()).unwrap_or(false);

        if !exists {
            data.insert(key, Entry::new(Value::String(value)));
            true
        } else {
            false
        }
    }

    pub fn getset(&self, key: String, value: String) -> Option<String> {
        let mut data = self.data.write().unwrap();
        let old = data.get(&key).and_then(|e| {
            if !e.is_expired() {
                if let Value::String(s) = &e.value {
                    Some(s.clone())
                } else {
                    None
                }
            } else {
                None
            }
        });
        data.insert(key, Entry::new(Value::String(value)));
        old
    }

    pub fn mset(&self, pairs: Vec<(String, String)>) {
        let mut data = self.data.write().unwrap();
        for (key, value) in pairs {
            data.insert(key, Entry::new(Value::String(value)));
        }
    }

    pub fn mget(&self, keys: &[String]) -> Vec<Option<String>> {
        let data = self.data.read().unwrap();
        keys.iter()
            .map(|key| {
                data.get(key).and_then(|e| {
                    if !e.is_expired() {
                        if let Value::String(s) = &e.value {
                            Some(s.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    pub fn lpush(&self, key: &str, values: Vec<String>) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::List(VecDeque::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::List(VecDeque::new()));
        }

        if let Value::List(list) = &mut entry.value {
            for v in values {
                list.push_front(v);
            }
            Ok(list.len())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn rpush(&self, key: &str, values: Vec<String>) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::List(VecDeque::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::List(VecDeque::new()));
        }

        if let Value::List(list) = &mut entry.value {
            for v in values {
                list.push_back(v);
            }
            Ok(list.len())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>, String> {
        let mut data = self.data.write().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &mut entry.value {
                    Ok(list.pop_front())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(None),
        }
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>, String> {
        let mut data = self.data.write().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &mut entry.value {
                    Ok(list.pop_back())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(None),
        }
    }

    pub fn llen(&self, key: &str) -> Result<usize, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &entry.value {
                    Ok(list.len())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &entry.value {
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(vec![]);
                    }

                    let start = if start < 0 {
                        (len + start).max(0) as usize
                    } else {
                        start.min(len) as usize
                    };

                    let stop = if stop < 0 {
                        (len + stop).max(0) as usize
                    } else {
                        stop.min(len - 1) as usize
                    };

                    if start > stop || start >= len as usize {
                        return Ok(vec![]);
                    }

                    Ok(list
                        .iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &entry.value {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        Ok(None)
                    } else {
                        Ok(list.get(idx as usize).cloned())
                    }
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(None),
        }
    }

    pub fn lset(&self, key: &str, index: i64, value: String) -> Result<(), String> {
        let mut data = self.data.write().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::List(list) = &mut entry.value {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        Err("ERR index out of range".to_string())
                    } else {
                        list[idx as usize] = value;
                        Ok(())
                    }
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Err("ERR no such key".to_string()),
        }
    }

    pub fn sadd(&self, key: &str, members: Vec<String>) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::Set(HashSet::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::Set(HashSet::new()));
        }

        if let Value::Set(set) = &mut entry.value {
            let mut added = 0;
            for member in members {
                if set.insert(member) {
                    added += 1;
                }
            }
            Ok(added)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn srem(&self, key: &str, members: Vec<String>) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Set(set) = &mut entry.value {
                    let mut removed = 0;
                    for member in members {
                        if set.remove(&member) {
                            removed += 1;
                        }
                    }
                    Ok(removed)
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Set(set) = &entry.value {
                    Ok(set.iter().cloned().collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    pub fn sismember(&self, key: &str, member: &str) -> Result<bool, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Set(set) = &entry.value {
                    Ok(set.contains(member))
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(false),
        }
    }

    pub fn scard(&self, key: &str) -> Result<usize, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Set(set) = &entry.value {
                    Ok(set.len())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn hset(&self, key: &str, field: String, value: String) -> Result<bool, String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::Hash(HashMap::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::Hash(HashMap::new()));
        }

        if let Value::Hash(hash) = &mut entry.value {
            let is_new = !hash.contains_key(&field);
            hash.insert(field, value);
            Ok(is_new)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn hmset(&self, key: &str, pairs: Vec<(String, String)>) -> Result<(), String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::Hash(HashMap::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::Hash(HashMap::new()));
        }

        if let Value::Hash(hash) = &mut entry.value {
            for (field, value) in pairs {
                hash.insert(field, value);
            }
            Ok(())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.get(field).cloned())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(None),
        }
    }

    pub fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<String>>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(fields.iter().map(|f| hash.get(f).cloned()).collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(fields.iter().map(|_| None).collect()),
        }
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    pub fn hdel(&self, key: &str, fields: Vec<String>) -> Result<usize, String> {
        let mut data = self.data.write().unwrap();
        match data.get_mut(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &mut entry.value {
                    let mut removed = 0;
                    for field in fields {
                        if hash.remove(&field).is_some() {
                            removed += 1;
                        }
                    }
                    Ok(removed)
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.contains_key(field))
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(false),
        }
    }

    pub fn hlen(&self, key: &str) -> Result<usize, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.len())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(0),
        }
    }

    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.keys().cloned().collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    pub fn hvals(&self, key: &str) -> Result<Vec<String>, String> {
        let data = self.data.read().unwrap();
        match data.get(key) {
            Some(entry) if !entry.is_expired() => {
                if let Value::Hash(hash) = &entry.value {
                    Ok(hash.values().cloned().collect())
                } else {
                    Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )
                }
            }
            _ => Ok(vec![]),
        }
    }

    pub fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64, String> {
        let mut data = self.data.write().unwrap();
        let entry = data
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Value::Hash(HashMap::new())));

        if entry.is_expired() {
            *entry = Entry::new(Value::Hash(HashMap::new()));
        }

        if let Value::Hash(hash) = &mut entry.value {
            let current = hash
                .get(field)
                .map(|v| v.parse::<i64>())
                .transpose()
                .map_err(|_| "ERR hash value is not an integer".to_string())?
                .unwrap_or(0);

            let new_value = current
                .checked_add(delta)
                .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;

            hash.insert(field.to_string(), new_value.to_string());
            Ok(new_value)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let data = self.data.read().unwrap();
        data.iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| Self::glob_match(pattern, key))
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn glob_match(pattern: &str, text: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        let pattern_chars: Vec<_> = pattern.chars().collect();
        let text_chars: Vec<_> = text.chars().collect();

        Self::glob_match_recursive(&pattern_chars, &text_chars)
    }

    fn glob_match_recursive(pattern: &[char], text: &[char]) -> bool {
        if pattern.is_empty() {
            return text.is_empty();
        }

        match pattern[0] {
            '*' => {
                for i in 0..=text.len() {
                    if Self::glob_match_recursive(&pattern[1..], &text[i..]) {
                        return true;
                    }
                }
                false
            }
            '?' => !text.is_empty() && Self::glob_match_recursive(&pattern[1..], &text[1..]),
            c => {
                !text.is_empty()
                    && text[0] == c
                    && Self::glob_match_recursive(&pattern[1..], &text[1..])
            }
        }
    }

    pub fn rename(&self, old_key: &str, new_key: &str) -> Result<(), String> {
        let mut data = self.data.write().unwrap();
        match data.remove(old_key) {
            Some(entry) if !entry.is_expired() => {
                data.insert(new_key.to_string(), entry);
                Ok(())
            }
            _ => Err("ERR no such key".to_string()),
        }
    }

    pub fn renamenx(&self, old_key: &str, new_key: &str) -> Result<bool, String> {
        let mut data = self.data.write().unwrap();

        let new_exists = data.get(new_key).map(|e| !e.is_expired()).unwrap_or(false);
        if new_exists {
            return Ok(false);
        }

        match data.remove(old_key) {
            Some(entry) if !entry.is_expired() => {
                data.insert(new_key.to_string(), entry);
                Ok(true)
            }
            _ => Err("ERR no such key".to_string()),
        }
    }

    pub fn dbsize(&self) -> usize {
        let data = self.data.read().unwrap();
        data.iter().filter(|(_, e)| !e.is_expired()).count()
    }

    pub fn flushdb(&self) {
        let mut data = self.data.write().unwrap();
        data.clear();
    }

    pub fn run_expiry_cleanup(&self) {
        self.cleanup_expired();
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let storage = Storage::new();
        storage.set("key".to_string(), "value".to_string());
        assert_eq!(storage.get("key"), Some("value".to_string()));
    }

    #[test]
    fn test_del() {
        let storage = Storage::new();
        storage.set("key".to_string(), "value".to_string());
        assert_eq!(storage.del(&["key".to_string()]), 1);
        assert_eq!(storage.get("key"), None);
    }

    #[test]
    fn test_incr() {
        let storage = Storage::new();
        storage.set("counter".to_string(), "10".to_string());
        assert_eq!(storage.incr("counter"), Ok(11));
        assert_eq!(storage.incr("counter"), Ok(12));
    }

    #[test]
    fn test_list_operations() {
        let storage = Storage::new();
        assert_eq!(
            storage.rpush("list", vec!["a".to_string(), "b".to_string()]),
            Ok(2)
        );
        assert_eq!(storage.lpush("list", vec!["c".to_string()]), Ok(3));
        assert_eq!(
            storage.lrange("list", 0, -1),
            Ok(vec!["c".to_string(), "a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_set_operations() {
        let storage = Storage::new();
        assert_eq!(
            storage.sadd("myset", vec!["a".to_string(), "b".to_string()]),
            Ok(2)
        );
        assert_eq!(storage.sadd("myset", vec!["a".to_string()]), Ok(0));
        assert_eq!(storage.scard("myset"), Ok(2));
    }

    #[test]
    fn test_hash_operations() {
        let storage = Storage::new();
        assert_eq!(
            storage.hset("hash", "field1".to_string(), "value1".to_string()),
            Ok(true)
        );
        assert_eq!(
            storage.hget("hash", "field1"),
            Ok(Some("value1".to_string()))
        );
        assert_eq!(storage.hlen("hash"), Ok(1));
    }

    #[test]
    fn test_glob_match() {
        assert!(Storage::glob_match("*", "anything"));
        assert!(Storage::glob_match("user:*", "user:123"));
        assert!(Storage::glob_match("user:*:name", "user:123:name"));
        assert!(!Storage::glob_match("user:*:name", "user:123:age"));
        assert!(Storage::glob_match("h?llo", "hello"));
        assert!(Storage::glob_match("h?llo", "hallo"));
        assert!(!Storage::glob_match("h?llo", "hllo"));
    }
}
