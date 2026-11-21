//! Reading input files in chunks into a reusable buffer.

use std::fs::File;
use std::os::unix::fs::FileExt;

/// Bytes kept readable (and zero) after the data: the tokenizer loads whole 64-byte
/// blocks and the key constructors load 16 bytes at any field.
pub const SLACK: usize = crate::scan::SLACK;
const STEP: usize = 64 << 10;

fn read_full(file: &File, buf: &mut [u8], mut offset: u64) -> std::io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match file.read_at(&mut buf[done..], offset) {
            Ok(0) => break,
            Ok(n) => {
                done += n;
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

/// Loads what is needed to handle every line that starts in `start..end` of the file:
/// the byte before `start` (to know whether `start` begins a line) and everything up to
/// the end of the last such line. Returns `(data_len, first)`: the data is
/// `buf[..data_len]`, and `first` is where file offset `start` sits in it (0 or 1).
/// Pass the result to `csv::for_each_line(&buf[..data_len], first, first + (end - start))`.
pub fn load_chunk(file: &File, len: u64, start: u64, end: u64, buf: &mut Vec<u8>) -> std::io::Result<(usize, usize)> {
    let from = start.saturating_sub(1);
    let first = (start - from) as usize;
    let mut want = (end - from) as usize;
    buf.clear();
    buf.resize(want + SLACK, 0);
    let mut have = read_full(file, &mut buf[..want], from)?;
    // The last line that starts before `end` may run past it: read on to its newline.
    while have == want && have > 0 && buf[have - 1] != b'\n' && from + (have as u64) < len {
        let more = STEP.min((len - from - have as u64) as usize);
        want += more;
        buf.resize(want + SLACK, 0);
        let got = read_full(file, &mut buf[have..want], from + have as u64)?;
        if let Some(i) = memchr::memchr(b'\n', &buf[have..have + got]) {
            have += i + 1;
            break;
        }
        have += got;
        if got < more {
            break;
        }
    }
    buf[have..have + SLACK].fill(0);
    Ok((have, first))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv;
    use std::io::Write;

    /// Every chunking of a file must yield each data line exactly once.
    #[test]
    fn chunks_cover_each_line_once() {
        let dir = std::env::temp_dir().join(format!("ss-io-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let long = "x".repeat(3 * STEP + 17);
        let texts = [
            "h1,h2\nl1\n\nl3\nlast".to_string(),
            "h1,h2\nl1\n\nl3\nlast\n".to_string(),
            "h\n".to_string(),
            "h".to_string(),
            String::new(),
            format!("h\n{long}\nb\n{long}"),
            format!("{long}\na\n\n\nb"),
        ];
        for (n, text) in texts.iter().enumerate() {
            let path = dir.join(format!("f{n}.csv"));
            File::create(&path).unwrap().write_all(text.as_bytes()).unwrap();
            let file = File::open(&path).unwrap();
            let len = text.len() as u64;
            let mut want = Vec::new();
            csv::for_each_line(text.as_bytes(), 0, text.len(), |l| want.push(l.to_vec()));
            let sizes: &[u64] = if text.len() < 100 { &[1, 2, 3, 5, 7, 1000] } else { &[1000, STEP as u64, STEP as u64 + 1, 100_000, 1 << 30] };
            for &size in sizes {
                let mut got = Vec::new();
                let mut buf = Vec::new();
                let mut start = 0;
                while start < len {
                    let end = (start + size).min(len);
                    let (data_len, first) = load_chunk(&file, len, start, end, &mut buf).unwrap();
                    assert!(buf.len() >= data_len + SLACK);
                    csv::for_each_line(&buf[..data_len], first, first + (end - start) as usize, |l| got.push(l.to_vec()));
                    start = end;
                }
                assert_eq!(got, want, "file {n} chunk {size}");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
