//! The CSV dialect of rules.md: lines end at `\n` only, every comma separates
//! fields, and a quote is legal only as a pair wrapping a whole field.

use memchr::{memchr, memchr_iter};

/// Strips the wrapping quotes of one field. `None` if a quote sits anywhere else.
#[inline]
fn unquote(field: &[u8]) -> Option<&[u8]> {
    match memchr(b'"', field) {
        None => Some(field),
        Some(0) => {
            let n = field.len();
            if n >= 2 && field[n - 1] == b'"' && memchr(b'"', &field[1..n - 1]).is_none() {
                Some(&field[1..n - 1])
            } else {
                None
            }
        }
        Some(_) => None,
    }
}

/// Splits a line (without its `\n`) into exactly `N` unquoted fields.
/// `None` means the line is malformed and must be dropped.
#[inline]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out = [&line[..0]; N];
    let mut start = 0;
    let mut k = 0;
    for pos in memchr_iter(b',', line) {
        if k == N - 1 {
            return None;
        }
        out[k] = unquote(&line[start..pos])?;
        k += 1;
        start = pos + 1;
    }
    if k != N - 1 {
        return None;
    }
    out[k] = unquote(&line[start..])?;
    Some(out)
}

/// Calls `f` for every data line that starts in `data[start..end)`.
/// The header (the first line of the file) is skipped. A chunk that begins
/// mid-line leaves that line to the chunk before it.
#[inline]
pub fn for_each_line<'a>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&'a [u8])) {
    let mut pos = if start == 0 {
        match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else if data[start - 1] == b'\n' {
        start
    } else {
        match memchr(b'\n', &data[start..]) {
            Some(i) => start + i + 1,
            None => return,
        }
    };
    while pos < end {
        let stop = match memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        f(&data[pos..stop]);
        pos = stop + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s<const N: usize>(line: &str) -> Option<Vec<String>> {
        split::<N>(line.as_bytes()).map(|a| a.iter().map(|f| String::from_utf8_lossy(f).into_owned()).collect())
    }

    #[test]
    fn splits_and_unquotes() {
        assert_eq!(s::<3>("a,b,c").unwrap(), ["a", "b", "c"]);
        assert_eq!(s::<3>("\"a\",\"\",c").unwrap(), ["a", "", "c"]);
        assert_eq!(s::<3>(",,").unwrap(), ["", "", ""]);
        assert_eq!(s::<2>(" u1,\" u1\"").unwrap(), [" u1", " u1"]);
        assert_eq!(s::<2>("a,b\r").unwrap(), ["a", "b\r"]);
    }

    #[test]
    fn rejects_malformed() {
        assert!(s::<3>("a,b").is_none());
        assert!(s::<3>("a,b,c,d").is_none());
        assert!(s::<2>("").is_none());
        assert!(s::<2>("a\"b,c").is_none());
        assert!(s::<2>("\"a,c").is_none());
        assert!(s::<2>("\"a,b\"").is_none());
        assert!(s::<2>("\",c").is_none());
        assert!(s::<2>("\"\"\",c").is_none());
        assert!(s::<2>("\"a\"x,c").is_none());
        assert!(s::<2>("a,\"b\"\r").is_none());
    }

    fn lines(data: &str, start: usize, end: usize) -> Vec<String> {
        let mut v = Vec::new();
        for_each_line(data.as_bytes(), start, end, |l| v.push(String::from_utf8_lossy(l).into_owned()));
        v
    }

    #[test]
    fn chunks_cover_each_line_once() {
        let data = "h1,h2\nl1\n\nl3\nlast";
        assert_eq!(lines(data, 0, data.len()), ["l1", "", "l3", "last"]);
        for cut in 0..=data.len() {
            let mut all = lines(data, 0, cut);
            all.extend(lines(data, cut, data.len()));
            if cut == 0 {
                // An empty first chunk: the second one starts at 0 and takes everything.
                assert_eq!(all, ["l1", "", "l3", "last"]);
            } else {
                assert_eq!(all, ["l1", "", "l3", "last"], "cut {cut}");
            }
        }
        assert!(lines("h1,h2\n", 0, 6).is_empty());
        assert!(lines("h1,h2", 0, 5).is_empty());
        assert!(lines("", 0, 0).is_empty());
    }
}
