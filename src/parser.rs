#[derive(Debug, PartialEq)]
pub enum Resp {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<String>),
    Array(Option<Vec<Resp>>),
}

pub fn parse(buff: &[u8]) -> Result<(Resp, usize), String> {
    if buff.is_empty() {
        return Err("empty input".to_string());
    }

    match buff[0] {
        b'+' => parse_simple(&buff),
        b'-' => parse_error(buff),
        b':' => parse_integer(buff),
        b'$' => parse_bulk(buff),
        b'*' => parse_array(buff),
        _ => Err("unknown type".into()),
    }
}

fn read_line(input: &[u8]) -> Result<(&[u8], usize), String> {
    for i in 0..input.len().saturating_sub(1) {
        if input[i] == b'\r' && input[i + 1] == b'\n' {
            return Ok((&input[..i], i + 2));
        }
    }
    Err("no CRLF found".into())
}

fn parse_simple(input: &[u8]) -> Result<(Resp, usize), String> {
    let (line, consumed) = read_line(&input[1..])?;
    let s = String::from_utf8(line.to_vec()).map_err(|_| "utf8")?;
    Ok((Resp::Simple(s), consumed + 1))
}

fn parse_error(input: &[u8]) -> Result<(Resp, usize), String> {
    let (line, consumed) = read_line(&input[1..])?;
    let s = String::from_utf8(line.to_vec()).map_err(|_| "utf8")?;
    Ok((Resp::Error(s), consumed + 1))
}

fn parse_integer(input: &[u8]) -> Result<(Resp, usize), String> {
    let (line, consumed) = read_line(&input[1..])?;
    let n = std::str::from_utf8(line)
        .map_err(|_| "utf8")?
        .parse::<i64>()
        .map_err(|_| "parse int")?;
    Ok((Resp::Integer(n), consumed + 1))
}

fn parse_bulk(input: &[u8]) -> Result<(Resp, usize), String> {
    let (line, mut offset) = read_line(&input[1..])?;
    let len = std::str::from_utf8(line)
        .map_err(|_| "utf8")?
        .parse::<isize>()
        .map_err(|_| "parse len")?;

    offset += 1;

    if len == -1 {
        return Ok((Resp::Bulk(None), offset));
    }

    let len = len as usize;
    let start = offset;
    let end = start + len;

    if input.len() < end + 2 {
        return Err("incomplete bulk".into());
    }

    let data = String::from_utf8_lossy(&input[start..end]).to_string();
    Ok((Resp::Bulk(Some(data)), end + 2))
}

fn parse_array(input: &[u8]) -> Result<(Resp, usize), String> {
    let (line, mut offset) = read_line(&input[1..])?;
    let len = std::str::from_utf8(line)
        .map_err(|_| "utf8")?
        .parse::<isize>()
        .map_err(|_| "parse len")?;

    offset += 1;

    if len == -1 {
        return Ok((Resp::Array(None), offset));
    }

    let mut items = Vec::with_capacity(len as usize);
    let mut total = offset;

    for _ in 0..len {
        let (val, used) = parse(&input[total..])?;
        total += used;
        items.push(val);
    }

    Ok((Resp::Array(Some(items)), total))
}
