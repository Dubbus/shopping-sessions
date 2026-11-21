//! Minimal Parquet writer: one row group, one PLAIN uncompressed data page (v1) per
//! column, Thrift compact footer. Types: required INT64, required DATE (INT32),
//! required and optional UTF8 strings.

use std::io::Write;

pub enum Data {
    I64(Vec<i64>),
    Date(Vec<i32>),
    Str { bytes: Vec<u8>, ends: Vec<u32> },
    OptStr { bytes: Vec<u8>, ends: Vec<u32>, present: Vec<bool> },
}

pub struct Column {
    pub name: &'static str,
    pub data: Data,
}

impl Data {
    pub fn str() -> Self {
        Data::Str { bytes: Vec::new(), ends: Vec::new() }
    }
    pub fn opt_str() -> Self {
        Data::OptStr { bytes: Vec::new(), ends: Vec::new(), present: Vec::new() }
    }
    pub fn push_str(&mut self, s: &[u8]) {
        match self {
            Data::Str { bytes, ends } | Data::OptStr { bytes, ends, .. } => {
                bytes.extend_from_slice(s);
                ends.push(bytes.len() as u32);
                if let Data::OptStr { present, .. } = self {
                    present.push(true);
                }
            }
            _ => unreachable!(),
        }
    }
    pub fn push_null(&mut self) {
        if let Data::OptStr { present, .. } = self {
            present.push(false);
        } else {
            unreachable!()
        }
    }
    fn rows(&self) -> usize {
        match self {
            Data::I64(v) => v.len(),
            Data::Date(v) => v.len(),
            Data::Str { ends, .. } => ends.len(),
            Data::OptStr { present, .. } => present.len(),
        }
    }
    fn physical(&self) -> i32 {
        match self {
            Data::I64(_) => 2,
            Data::Date(_) => 1,
            _ => 6,
        }
    }
}

// ---- Thrift compact protocol ----
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn head(&mut self, id: i16, ty: u8) {
        let d = id - *self.last.last().unwrap();
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.head(id, 5);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.head(id, 6);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.head(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.head(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.head(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn list_struct_begin(&mut self) {
        self.last.push(0);
    }
    fn list_i32(&mut self, v: i32) {
        self.zz(v as i64);
    }
    fn list_bin(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
}

fn page_body(d: &Data) -> Vec<u8> {
    let mut out = Vec::new();
    match d {
        Data::I64(v) => {
            out.reserve(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        Data::Date(v) => {
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
        }
        Data::Str { bytes, ends } => plain_strings(&mut out, bytes, ends),
        Data::OptStr { bytes, ends, present } => {
            // Definition levels: RLE/bit-packed hybrid, one bit-packed run, 4-byte length prefix.
            let groups = present.len().div_ceil(8);
            let mut lv = Vec::new();
            let mut h = ((groups as u64) << 1) | 1;
            while h >= 0x80 {
                lv.push((h as u8) | 0x80);
                h >>= 7;
            }
            lv.push(h as u8);
            for g in 0..groups {
                let mut byte = 0u8;
                for i in 0..8 {
                    if present.get(g * 8 + i).copied().unwrap_or(false) {
                        byte |= 1 << i;
                    }
                }
                lv.push(byte);
            }
            out.extend_from_slice(&(lv.len() as u32).to_le_bytes());
            out.extend_from_slice(&lv);
            plain_strings(&mut out, bytes, ends);
        }
    }
    out
}

fn plain_strings(out: &mut Vec<u8>, bytes: &[u8], ends: &[u32]) {
    out.reserve(bytes.len() + ends.len() * 4);
    let mut start = 0usize;
    for &e in ends {
        let e = e as usize;
        out.extend_from_slice(&((e - start) as u32).to_le_bytes());
        out.extend_from_slice(&bytes[start..e]);
        start = e;
    }
}

pub fn write(path: &std::path::Path, cols: &[Column]) -> std::io::Result<()> {
    let rows = cols.first().map_or(0, |c| c.data.rows());
    let mut file = Vec::new();
    file.extend_from_slice(b"PAR1");
    let mut metas = Vec::new(); // (offset, total size)
    for c in cols {
        assert_eq!(c.data.rows(), rows);
        let body = page_body(&c.data);
        let mut h = T::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body.len() as i32);
        h.i32(3, body.len() as i32);
        h.begin(5);
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3);
        h.end();
        h.b.push(0);
        let off = file.len();
        file.extend_from_slice(&h.b);
        file.extend_from_slice(&body);
        metas.push((off, file.len() - off));
    }

    let mut t = T::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    // root
    t.list_struct_begin();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.list_struct_begin();
        t.i32(1, c.data.physical());
        let optional = matches!(c.data, Data::OptStr { .. });
        t.i32(3, optional as i32);
        t.bin(4, c.name.as_bytes());
        match c.data {
            Data::Str { .. } | Data::OptStr { .. } => {
                t.i32(6, 0); // UTF8
                t.begin(10);
                t.begin(1); // STRING
                t.end();
                t.end();
            }
            Data::Date(_) => {
                t.i32(6, 6); // DATE
                t.begin(10);
                t.begin(6);
                t.end();
                t.end();
            }
            Data::I64(_) => {}
        }
        t.end();
    }
    t.i64(3, rows as i64);
    t.list(4, 12, 1);
    t.list_struct_begin();
    t.list(1, 12, cols.len());
    let mut total = 0i64;
    for (c, &(off, size)) in cols.iter().zip(&metas) {
        total += size as i64;
        t.list_struct_begin();
        t.i64(2, off as i64);
        t.begin(3);
        t.i32(1, c.data.physical());
        t.list(2, 5, 2);
        t.list_i32(0);
        t.list_i32(3);
        t.list(3, 8, 1);
        t.list_bin(c.name.as_bytes());
        t.i32(4, 0);
        t.i64(5, rows as i64);
        t.i64(6, size as i64);
        t.i64(7, size as i64);
        t.i64(9, off as i64);
        t.end();
        t.end();
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end();
    t.bin(6, b"shopping-sessions");
    t.b.push(0);

    file.extend_from_slice(&t.b);
    file.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&file)
}
