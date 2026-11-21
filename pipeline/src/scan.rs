//! The fast path of the CSV dialect: one vectorised pass finds every comma,
//! quote and newline of a chunk, then lines are split by walking those
//! positions. It must agree exactly with `csv::for_each_line` + `csv::split`,
//! which stay as the plain statement of the dialect; the tests check that.

/// Positions of every `,`, `"` and `\n` in `data`, in order.
pub fn index(data: &[u8], tokens: &mut Vec<u32>) {
    tokens.clear();
    tokens.extend(memchr::memchr3_iter(b',', b'"', b'\n', data).map(|p| p as u32));
}

/// Calls `f` with the `N` unquoted fields of every well-formed data line that
/// starts in `data[first..last)`. Same contract as `csv::for_each_line`: the
/// header is skipped, and a chunk that begins mid-line leaves that line to the
/// chunk before it. `tokens` must be `index(data)`.
#[inline]
pub fn for_each_record<'a, const N: usize>(
    data: &'a [u8],
    first: usize,
    last: usize,
    tokens: &[u32],
    mut f: impl FnMut(&[&'a [u8]; N]),
) {
    let count = tokens.len();
    let mut k = 0;
    // Where the first line of this chunk starts.
    let mut pos = if first != 0 && data[first - 1] == b'\n' {
        while k < count && (tokens[k] as usize) < first {
            k += 1;
        }
        first
    } else {
        // The header, or the tail of a line that belongs to the chunk before.
        loop {
            if k == count {
                return;
            }
            let t = tokens[k] as usize;
            k += 1;
            if t >= first && data[t] == b'\n' {
                break t + 1;
            }
        }
    };
    let mut fields = [&data[..0]; N];
    while pos < last {
        let mut start = pos;
        let mut n = 0;
        let mut well_formed = true;
        let line_end;
        loop {
            if k == count {
                // The file's last line has no newline: its last field runs to the end.
                if n < N {
                    fields[n] = &data[start..];
                }
                n += 1;
                line_end = data.len();
                break;
            }
            let t = tokens[k] as usize;
            match data[t] {
                b',' => {
                    if n < N {
                        fields[n] = &data[start..t];
                    }
                    n += 1;
                    start = t + 1;
                    k += 1;
                }
                b'\n' => {
                    if n < N {
                        fields[n] = &data[start..t];
                    }
                    n += 1;
                    k += 1;
                    line_end = t;
                    break;
                }
                _ => {
                    // A quote is legal only as a pair wrapping the whole field: it opens at the
                    // field's first byte, the next token closes it, and a comma, a newline or the
                    // end of the file follows at once.
                    if t == start && k + 1 < count && data[tokens[k + 1] as usize] == b'"' {
                        let close = tokens[k + 1] as usize;
                        let after = close + 1;
                        if after == data.len() {
                            if n < N {
                                fields[n] = &data[start + 1..close];
                            }
                            n += 1;
                            k += 2;
                            line_end = after;
                            break;
                        }
                        if k + 2 < count && tokens[k + 2] as usize == after && data[after] != b'"' {
                            if n < N {
                                fields[n] = &data[start + 1..close];
                            }
                            n += 1;
                            k += 3;
                            if data[after] == b'\n' {
                                line_end = after;
                                break;
                            }
                            start = after + 1;
                            continue;
                        }
                    }
                    // Malformed: skip the rest of the line.
                    well_formed = false;
                    loop {
                        if k == count {
                            line_end = data.len();
                            break;
                        }
                        let t = tokens[k] as usize;
                        k += 1;
                        if data[t] == b'\n' {
                            line_end = t;
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if well_formed && n == N {
            f(&fields);
        }
        pos = line_end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv;

    fn slow<const N: usize>(data: &[u8], first: usize, last: usize) -> Vec<Vec<Vec<u8>>> {
        let mut rows = Vec::new();
        csv::for_each_line(data, first, last, |line| {
            if let Some(fields) = csv::split::<N>(line) {
                rows.push(fields.iter().map(|f| f.to_vec()).collect());
            }
        });
        rows
    }

    fn fast<const N: usize>(data: &[u8], first: usize, last: usize) -> Vec<Vec<Vec<u8>>> {
        let mut tokens = Vec::new();
        index(data, &mut tokens);
        let mut rows = Vec::new();
        for_each_record::<N>(data, first, last, &tokens, |fields| rows.push(fields.iter().map(|f| f.to_vec()).collect()));
        rows
    }

    fn check<const N: usize>(data: &[u8]) {
        assert_eq!(fast::<N>(data, 0, data.len()), slow::<N>(data, 0, data.len()), "whole {:?}", String::from_utf8_lossy(data));
        for cut in 1..data.len() {
            assert_eq!(fast::<N>(data, 0, cut), slow::<N>(data, 0, cut), "head {cut} {:?}", String::from_utf8_lossy(data));
            assert_eq!(fast::<N>(data, cut, data.len()), slow::<N>(data, cut, data.len()), "tail {cut} {:?}", String::from_utf8_lossy(data));
        }
    }

    #[test]
    fn hand_picked() {
        for text in [
            "", "h", "h\n", "h\na,b", "h\na,b\n", "h\n\"a\",\"b\"", "h\n\"a\",\"b\"\n", "h\n\"\",\"\"\n\n,\n",
            "h\n\"a,b\"\nc,d\n", "h\na\"b,c\nd,e\n", "h\n\"a\"\"b\",c\nd,e\n", "h\n\"a\"x,c\nd,e\n", "h\na,b,c\nd,e\n",
            "h\n\"a\nb\",c\nd,e\n", "h\na,\"b\"\r\nc,d\r\n", "h\na,\"", "h\na,\"\"", "h\na,\"\"\"", "h\n\",\"\n", "\n\n\na,b\n",
            "h\na,\"b", "h\n\"a\",b\n\"", "h\n\"\n\"\n", "a,b\nc,d", "h\n,\n,", "h\n\"a\"\n",
        ] {
            check::<2>(text.as_bytes());
            check::<3>(text.as_bytes());
        }
    }

    /// Random soup of the characters that matter, compared with the plain splitter at every cut.
    #[test]
    fn agrees_with_plain_splitter() {
        let alphabet = b"ab,,,\"\"\n\n\r ";
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..6000 {
            let len = (next() % 40) as usize;
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            if round % 3 == 0 {
                check::<2>(&data);
            } else if round % 3 == 1 {
                check::<4>(&data);
            } else {
                check::<7>(&data);
            }
        }
    }
}
