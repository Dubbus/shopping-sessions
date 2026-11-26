//! The fast path of the CSV dialect. One vectorised pass over a chunk records
//! where every comma and newline is and how many quotes precede each position;
//! lines are then cut into fields from those positions with no per-byte work.
//!
//! It must agree exactly with `csv::for_each_line` + `csv::split`, which stay
//! as the plain statement of the dialect. The tests check that, and check the
//! AVX-512 indexer against the portable one.

/// Bytes that must be readable after the data handed to [`Index::build`].
pub const SLACK: usize = 128;

/// Benchmarking only: use the portable index builder even where AVX-512 is available.
pub static PORTABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
pub struct Index {
    /// Positions of commas, then sentinels.
    commas: Vec<u32>,
    /// Positions of newlines, plus the data length if the last line has none.
    newlines: Vec<u32>,
    lines: usize,
    /// Per 64-byte block: which bytes are quotes, and how many quotes come before the block.
    quote_bits: Vec<u64>,
    quotes_before: Vec<u32>,
}

impl Index {
    /// Indexes `buf[..len]`. `buf` must be at least `len + SLACK` long.
    pub fn build(&mut self, buf: &[u8], len: usize) {
        assert!(buf.len() >= len + SLACK && len < u32::MAX as usize - SLACK);
        let blocks = len.div_ceil(64);
        // Room for the worst case (every byte a comma) plus what the vector code overwrites.
        self.commas.clear();
        self.commas.reserve(len + SLACK);
        self.newlines.clear();
        self.newlines.reserve(len + SLACK);
        self.quote_bits.clear();
        self.quote_bits.resize(blocks + 1, 0);
        self.quotes_before.clear();
        self.quotes_before.resize(blocks + 1, 0);
        // SAFETY: the capacities above cover every write the builders make.
        unsafe {
            #[cfg(target_arch = "x86_64")]
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw") && !PORTABLE.load(std::sync::atomic::Ordering::Relaxed) {
                self.build_avx512(buf.as_ptr(), len);
            } else {
                self.build_portable(buf.as_ptr(), len);
            }
            #[cfg(not(target_arch = "x86_64"))]
            self.build_portable(buf.as_ptr(), len);
        }
        if len > 0 && buf[len - 1] != b'\n' {
            // The file's last line has no newline: it ends where the data does.
            self.newlines.push(len as u32);
        }
        self.lines = self.newlines.len();
        // Sentinels, so a line's comma count can be read without bounds checks.
        self.commas.extend_from_slice(&[u32::MAX; 16]);
        self.newlines.push(u32::MAX);
    }

    /// One byte at a time. The reference for the vector version, and what other CPUs run.
    unsafe fn build_portable(&mut self, buf: *const u8, len: usize) {
        let mut quotes = 0u32;
        for block in 0..len.div_ceil(64) {
            self.quotes_before[block] = quotes;
            let mut bits = 0u64;
            for i in block * 64..(block * 64 + 64).min(len) {
                match unsafe { *buf.add(i) } {
                    b',' => self.commas.push(i as u32),
                    b'\n' => self.newlines.push(i as u32),
                    b'"' => bits |= 1 << (i & 63),
                    _ => {}
                }
            }
            self.quote_bits[block] = bits;
            quotes += bits.count_ones();
        }
        let blocks = len.div_ceil(64);
        self.quotes_before[blocks] = quotes;
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512bw,popcnt,bmi1,bmi2")]
    unsafe fn build_avx512(&mut self, buf: *const u8, len: usize) {
        use std::arch::x86_64::*;
        unsafe {
            let (comma, newline, quote) = (_mm512_set1_epi8(b',' as i8), _mm512_set1_epi8(b'\n' as i8), _mm512_set1_epi8(b'"' as i8));
            let iota = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
            let mut commas = self.commas.as_mut_ptr();
            let mut newlines = self.newlines.as_mut_ptr();
            let mut quotes = 0u32;
            let blocks = len.div_ceil(64);
            for block in 0..blocks {
                let base = block * 64;
                let bytes = _mm512_loadu_si512(buf.add(base) as *const _);
                // Bytes past the data (only in the last block) must not count.
                let live = if len - base >= 64 { u64::MAX } else { (1u64 << (len - base)) - 1 };
                let comma_bits = _mm512_cmpeq_epi8_mask(bytes, comma) & live;
                let mut newline_bits = _mm512_cmpeq_epi8_mask(bytes, newline) & live;
                let quote_bits = _mm512_cmpeq_epi8_mask(bytes, quote) & live;
                *self.quote_bits.get_unchecked_mut(block) = quote_bits;
                *self.quotes_before.get_unchecked_mut(block) = quotes;
                quotes += quote_bits.count_ones();
                // Commas: 16 byte positions at a time, packed to the front and stored whole.
                for quarter in 0..4u32 {
                    let bits = (comma_bits >> (16 * quarter)) as u16;
                    let positions = _mm512_add_epi32(iota, _mm512_set1_epi32((base as u32 + 16 * quarter) as i32));
                    _mm512_storeu_si512(commas as *mut _, _mm512_maskz_compress_epi32(bits, positions));
                    commas = commas.add(bits.count_ones() as usize);
                }
                // Newlines are rare: about one per block.
                while newline_bits != 0 {
                    *newlines = base as u32 + newline_bits.trailing_zeros();
                    newlines = newlines.add(1);
                    newline_bits &= newline_bits - 1;
                }
            }
            *self.quotes_before.get_unchecked_mut(blocks) = quotes;
            self.commas.set_len(commas.offset_from(self.commas.as_ptr()) as usize);
            self.newlines.set_len(newlines.offset_from(self.newlines.as_ptr()) as usize);
        }
    }

    /// Quotes in `data[..pos]`.
    #[inline]
    fn quotes_until(&self, pos: usize) -> u32 {
        // SAFETY: `pos` is at most the data length, and both tables have an entry for its block.
        unsafe { *self.quotes_before.get_unchecked(pos >> 6) + (*self.quote_bits.get_unchecked(pos >> 6) & ((1u64 << (pos & 63)) - 1)).count_ones() }
    }

    /// Calls `f` with the `N` unquoted fields of every well-formed data line that starts in
    /// `buf[first..last)`, for the buffer this index was built from. Same contract as
    /// `csv::for_each_line`: the header is skipped, and a chunk that begins mid-line leaves
    /// that line to the chunk before it.
    #[inline]
    pub fn for_each_record<'a, const N: usize>(&self, buf: &'a [u8], first: usize, last: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
        let (commas, newlines) = (&self.commas[..], &self.newlines[..]);
        let mut line = 0;
        // Where the first line of this chunk starts.
        let mut pos = if first != 0 && buf[first - 1] == b'\n' {
            first
        } else {
            // The header, or the tail of a line that belongs to the chunk before.
            while line < self.lines && (newlines[line] as usize) < first {
                line += 1;
            }
            if line == self.lines {
                return;
            }
            line += 1;
            newlines[line - 1] as usize + 1
        };
        while line < self.lines && (newlines[line] as usize) < pos {
            line += 1;
        }
        let mut c = commas.partition_point(|&p| (p as usize) < pos);
        let data = buf.as_ptr();
        let mut fields = [&buf[..0]; N];
        while pos < last && line < self.lines {
            // SAFETY: `line` is in range, and `commas` ends in 16 sentinels, more than N.
            let end = unsafe { *newlines.get_unchecked(line) } as usize;
            line += 1;
            let start = pos;
            pos = end + 1;
            // A well-formed line has exactly N - 1 commas.
            let enough = unsafe { *commas.get_unchecked(c + N - 2) as usize } < end;
            let too_many = unsafe { *commas.get_unchecked(c + N - 1) as usize } < end;
            if !enough || too_many {
                while (unsafe { *commas.get_unchecked(c) } as usize) < end {
                    c += 1;
                }
                continue;
            }
            // Cut the fields. A field that starts with a quote must also end with one and is
            // unwrapped; any other quote on the line makes the counts below disagree.
            let mut from = start;
            let mut wrapped = 0u32;
            let mut broken = false;
            for (n, field) in fields.iter_mut().enumerate() {
                let to = if n == N - 1 { end } else { unsafe { *commas.get_unchecked(c + n) as usize } };
                // SAFETY: `from <= to <= end <= len`, and the buffer has slack after `len`;
                // `to >= 1` because the header line precedes every data line.
                let quoted = unsafe { *data.add(from) } == b'"';
                let q = quoted as usize;
                broken |= quoted & ((to - from < 2) | (unsafe { *data.add(to - 1) } != b'"'));
                wrapped += q as u32;
                // A broken field can make this range inverted; it is never read then.
                *field = unsafe { std::slice::from_raw_parts(data.add(from + q), (to - q).wrapping_sub(from + q)) };
                from = to + 1;
            }
            c += N - 1;
            if broken || self.quotes_until(end) - self.quotes_until(start) != 2 * wrapped {
                continue;
            }
            f(&fields);
        }
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

    fn padded(data: &[u8]) -> Vec<u8> {
        let mut buf = data.to_vec();
        // Junk in the slack must not leak into the result.
        buf.extend((0..SLACK).map(|i| b",\"\n"[i % 3]));
        buf
    }

    fn fast<const N: usize>(data: &[u8], first: usize, last: usize) -> Vec<Vec<Vec<u8>>> {
        let buf = padded(data);
        let mut index = Index::default();
        index.build(&buf, data.len());
        let mut rows = Vec::new();
        index.for_each_record::<N>(&buf, first, last, |fields| rows.push(fields.iter().map(|f| f.to_vec()).collect()));
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
            "h\na,\"b", "h\n\"a\",b\n\"", "h\n\"\n\"\n", "a,b\nc,d", "h\n,\n,", "h\n\"a\"\n", "h\n\",\n\",\"\n", "h\nx\",\"y\n",
            "h\n\"\"\"\",a\n", "h\na,\"\"\"\"\n",
        ] {
            check::<2>(text.as_bytes());
            check::<3>(text.as_bytes());
        }
    }

    fn random(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed;
        move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        }
    }

    /// Random soup of the characters that matter, compared with the plain splitter at every cut.
    #[test]
    fn agrees_with_plain_splitter() {
        let alphabet = b"ab,,,\"\"\n\n\r ";
        let mut next = random(0x9E37_79B9_7F4A_7C15);
        for round in 0..6000 {
            let len = (next() % 40) as usize;
            let data: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            match round % 3 {
                0 => check::<2>(&data),
                1 => check::<4>(&data),
                _ => check::<7>(&data),
            }
        }
    }

    /// Longer inputs, so lines span 64-byte blocks in every alignment.
    #[test]
    fn agrees_across_blocks() {
        let mut next = random(0x1234_5678_9ABC_DEF1);
        for _ in 0..300 {
            let mut data = b"header,line\n".to_vec();
            for _ in 0..(next() % 30) {
                let fields = if next() % 9 == 0 { next() % 9 } else { 4 };
                for n in 0..fields {
                    if n > 0 {
                        data.push(b',');
                    }
                    let quoted = next() % 5 == 0;
                    if quoted {
                        data.push(b'"');
                    }
                    for _ in 0..(next() % 30) {
                        data.push(match next() % 60 {
                            0 => b'"',
                            1 => b'\r',
                            x => b'a' + (x % 26) as u8,
                        });
                    }
                    if quoted && next() % 20 != 0 {
                        data.push(b'"');
                    }
                }
                if next() % 40 != 0 {
                    data.push(b'\n');
                }
            }
            assert_eq!(fast::<4>(&data, 0, data.len()), slow::<4>(&data, 0, data.len()), "{:?}", String::from_utf8_lossy(&data));
            for _ in 0..8 {
                let cut = (next() % (data.len() as u64 + 1)) as usize;
                assert_eq!(fast::<4>(&data, cut, data.len()), slow::<4>(&data, cut, data.len()), "cut {cut} {:?}", String::from_utf8_lossy(&data));
                assert_eq!(fast::<4>(&data, 0, cut), slow::<4>(&data, 0, cut), "head {cut} {:?}", String::from_utf8_lossy(&data));
            }
        }
    }

    /// The vector indexer must produce exactly what the portable one does.
    #[test]
    fn vector_index_matches_portable() {
        let mut next = random(0xDEAD_BEEF_CAFE_F00D);
        for round in 0..2000 {
            let len = (next() % 700) as usize;
            let dense = round % 4 == 0;
            let data: Vec<u8> = (0..len)
                .map(|_| match next() % if dense { 4 } else { 12 } {
                    0 => b',',
                    1 => b'"',
                    2 => b'\n',
                    _ => b'x',
                })
                .collect();
            let buf = padded(&data);
            let mut built = Index::default();
            built.build(&buf, len);
            let mut plain = Index::default();
            plain.commas.reserve(len + SLACK);
            plain.newlines.reserve(len + SLACK);
            plain.quote_bits.resize(len.div_ceil(64) + 1, 0);
            plain.quotes_before.resize(len.div_ceil(64) + 1, 0);
            unsafe { plain.build_portable(buf.as_ptr(), len) };
            assert_eq!(&built.commas[..built.commas.len() - 16], &plain.commas[..], "commas, len {len}");
            let newlines = &built.newlines[..built.newlines.len() - 1];
            let newlines = if len > 0 && data[len - 1] != b'\n' { &newlines[..newlines.len() - 1] } else { newlines };
            assert_eq!(newlines, &plain.newlines[..], "newlines, len {len}");
            assert_eq!(built.quote_bits, plain.quote_bits, "quote bits, len {len}");
            assert_eq!(built.quotes_before, plain.quotes_before, "quote counts, len {len}");
        }
    }
}
