//! The CSV dialect: commas always separate, a field may be wrapped in one pair of
//! quotes, any other quote is malformed, the field count must match.

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    if !f.contains(&b'"') {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && !f[1..n - 1].contains(&b'"') {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split `line` into exactly N fields. False if malformed (empty line included).
#[inline]
pub fn fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut n = 0;
    for f in line.split(|&c| c == b',') {
        if n == N {
            return false;
        }
        match unquote(f) {
            Some(v) => out[n] = v,
            None => return false,
        }
        n += 1;
    }
    n == N
}

/// Iterate the lines of a chunk (ends on '\n' or EOF). A trailing '\n' does not
/// make an extra empty line.
pub fn lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = buf.strip_suffix(b"\n").unwrap_or(buf);
    let empty = body.is_empty() && buf.is_empty();
    body.split(|&c| c == b'\n').filter(move |_| !empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect() {
        let mut f = [&b""[..]; 2];
        assert!(fields(b"\"u4\",\"u1\"", &mut f) && f == [b"u4", b"u1"]);
        assert!(fields(b"\"\",u1", &mut f) && f[0].is_empty());
        assert!(!fields(b"u1,u5\"", &mut f));
        assert!(!fields(b"u1,u2,u3", &mut f));
        assert!(!fields(b"", &mut f));
        assert!(!fields(b"u1", &mut f));
        assert_eq!(lines(b"a\n\nb\n").collect::<Vec<_>>(), vec![&b"a"[..], b"", b"b"]);
        assert_eq!(lines(b"a").count(), 1);
        assert_eq!(lines(b"").count(), 0);
        assert_eq!(lines(b"\n").collect::<Vec<_>>(), vec![&b""[..]]);
    }
}
