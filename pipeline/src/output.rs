//! Parquet output for the two tables, written by hand.
//!
//! Every column is dictionary-encoded: a dictionary page of the distinct values,
//! then one data page of indexes in the RLE / bit-packed hybrid. Pages are
//! uncompressed version-1 data pages, which every reader understands. Small
//! files matter: Docker returns only once the output has reached the disk.
//!
//! Layout: `PAR1`, the column chunks, the footer (Thrift compact protocol), its
//! length, `PAR1`.

use std::path::Path;

use crate::key::{self, Id};
use crate::table::Str;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Rows of `user_daily`, column by column.
#[derive(Default)]
pub struct UserRows {
    pub user_id: Vec<Id>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue_cents: Vec<i64>,
    /// Index into the category names, or `NO_CATEGORY` when the day has no purchases.
    pub top_category: Vec<u32>,
}

pub const NO_CATEGORY: u32 = u32::MAX;

/// Rows of `campaign_daily`, column by column.
#[derive(Default)]
pub struct CampaignRows {
    /// Index into the campaign names and channels.
    pub campaign: Vec<u32>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing_sessions: Vec<i64>,
    pub revenue_cents: Vec<i64>,
    pub assisted_sessions: Vec<i64>,
}

// ---- Thrift compact protocol ------------------------------------------------------------

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[derive(Default)]
struct Thrift {
    out: Vec<u8>,
    /// The last field ID written in each open struct.
    last: Vec<i16>,
}

impl Thrift {
    fn zigzag(&mut self, v: i64) {
        varint(&mut self.out, ((v << 1) ^ (v >> 63)) as u64);
    }

    fn field(&mut self, id: i16, kind: u8) {
        let last = self.last.last_mut().expect("field outside a struct");
        let delta = id - *last;
        *last = id;
        if (1..=15).contains(&delta) {
            self.out.push((delta as u8) << 4 | kind);
        } else {
            self.out.push(kind);
            self.zigzag(id as i64);
        }
    }

    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }

    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }

    fn string(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.bare_string(s);
    }

    fn bare_string(&mut self, s: &[u8]) {
        varint(&mut self.out, s.len() as u64);
        self.out.extend_from_slice(s);
    }

    fn list(&mut self, id: i16, kind: u8, len: usize) {
        self.field(id, T_LIST);
        if len < 15 {
            self.out.push((len as u8) << 4 | kind);
        } else {
            self.out.push(0xF0 | kind);
            varint(&mut self.out, len as u64);
        }
    }

    /// Opens a struct that is a field of the current one.
    fn open(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }

    /// Opens a struct that is a list element or the top level.
    fn open_bare(&mut self) {
        self.last.push(0);
    }

    fn close(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
}

// ---- Parquet constants ------------------------------------------------------------------

const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;

const PLAIN: i32 = 0;
const RLE: i32 = 3;
const RLE_DICTIONARY: i32 = 8;

const DATA_PAGE: i32 = 0;
const DICTIONARY_PAGE: i32 = 2;

const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;

#[derive(Clone, Copy, PartialEq)]
enum Logical {
    None,
    String,
    Date,
}

struct Column {
    name: &'static str,
    physical: i32,
    logical: Logical,
    optional: bool,
}

/// One column's pages, ready to be placed in the file.
struct Chunk {
    dictionary: Vec<u8>,
    dictionary_len: usize,
    /// Definition levels (for an optional column) followed by the index run data.
    data: Vec<u8>,
    values: usize,
}

// ---- The RLE / bit-packed hybrid ---------------------------------------------------------

fn bit_width(max: u32) -> u32 {
    (32 - max.leading_zeros()).max(1)
}

/// Writes values in the hybrid encoding. Whole groups of 8 equal values become RLE runs;
/// everything else is bit-packed in groups of 8.
struct Hybrid<'a> {
    out: &'a mut Vec<u8>,
    width: u32,
    /// Values of the group being collected.
    group: [u32; 8],
    filled: usize,
    /// A run of whole groups that all hold `run_value`.
    run_value: u32,
    run_groups: usize,
    /// Bit-packed groups not yet written, and how many.
    packed: Vec<u8>,
    packed_groups: usize,
}

impl<'a> Hybrid<'a> {
    fn new(out: &'a mut Vec<u8>, width: u32) -> Self {
        Hybrid { out, width, group: [0; 8], filled: 0, run_value: 0, run_groups: 0, packed: Vec::new(), packed_groups: 0 }
    }

    #[inline]
    fn push(&mut self, value: u32) {
        self.group[self.filled] = value;
        self.filled += 1;
        if self.filled == 8 {
            self.filled = 0;
            self.close_group();
        }
    }

    /// `count` copies of `value`.
    #[cfg(test)]
    fn repeat(&mut self, value: u32, mut count: usize) {
        // Finish the open group one value at a time, then add whole groups at once.
        while count > 0 && self.filled != 0 {
            self.push(value);
            count -= 1;
        }
        let groups = count / 8;
        if groups > 0 {
            if self.run_groups > 0 && self.run_value != value {
                self.flush_run();
            }
            if self.run_groups == 0 {
                self.flush_packed();
                self.run_value = value;
            }
            self.run_groups += groups;
        }
        for _ in 0..count % 8 {
            self.push(value);
        }
    }

    fn close_group(&mut self) {
        let first = self.group[0];
        let uniform = self.group.iter().all(|&v| v == first);
        if uniform && self.run_groups > 0 && self.run_value == first {
            self.run_groups += 1;
            return;
        }
        self.flush_run();
        if uniform {
            self.flush_packed();
            self.run_value = first;
            self.run_groups = 1;
            return;
        }
        // Pack the 8 values, least significant bit first.
        let mut bits: u128 = 0;
        let mut have = 0;
        for &v in &self.group {
            bits |= (v as u128) << have;
            have += self.width;
            while have >= 8 {
                self.packed.push(bits as u8);
                bits >>= 8;
                have -= 8;
            }
        }
        debug_assert_eq!(have, 0);
        self.packed_groups += 1;
    }

    fn flush_run(&mut self) {
        if self.run_groups == 0 {
            return;
        }
        varint(self.out, (self.run_groups as u64 * 8) << 1);
        self.out.extend_from_slice(&self.run_value.to_le_bytes()[..self.width.div_ceil(8) as usize]);
        self.run_groups = 0;
    }

    fn flush_packed(&mut self) {
        if self.packed_groups == 0 {
            return;
        }
        varint(self.out, (self.packed_groups as u64) << 1 | 1);
        self.out.append(&mut self.packed);
        self.packed_groups = 0;
    }

    fn finish(mut self) {
        if self.filled > 0 {
            // The last group is padded; readers stop at the page's value count.
            let pad = self.group[self.filled - 1];
            for slot in self.filled..8 {
                self.group[slot] = pad;
            }
            self.filled = 0;
            self.close_group();
        }
        self.flush_run();
        self.flush_packed();
    }
}

// ---- Column encoders --------------------------------------------------------------------

#[inline]
fn slot_of(v: i64, capacity: usize) -> usize {
    ((v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & (capacity - 1)
}

/// An integer column: dictionary of the distinct values in first-seen order, each stored in
/// `bytes` bytes.
fn int_chunk(values: impl Iterator<Item = i64>, bytes: usize) -> Chunk {
    // Value -> dictionary index plus one (0 marks an empty slot). Open addressing.
    let mut table: Vec<(i64, u32)> = vec![(0, 0); 64];
    let mut dictionary = Vec::new();
    let mut distinct = 0u32;
    let mut indexes: Vec<u32> = Vec::with_capacity(values.size_hint().0);
    for v in values {
        let mut at = slot_of(v, table.len());
        let index = loop {
            let slot = table[at];
            if slot.1 == 0 {
                table[at] = (v, distinct + 1);
                dictionary.extend_from_slice(&v.to_le_bytes()[..bytes]);
                distinct += 1;
                break distinct - 1;
            }
            if slot.0 == v {
                break slot.1 - 1;
            }
            at = (at + 1) & (table.len() - 1);
        };
        indexes.push(index);
        if distinct as usize * 2 > table.len() {
            let mut bigger = vec![(0, 0); table.len() * 4];
            for &(v, i) in table.iter().filter(|slot| slot.1 != 0) {
                let mut at = slot_of(v, bigger.len());
                while bigger[at].1 != 0 {
                    at = (at + 1) & (bigger.len() - 1);
                }
                bigger[at] = (v, i);
            }
            table = bigger;
        }
    }
    let width = bit_width(distinct.saturating_sub(1));
    let mut data = vec![width as u8];
    let mut hybrid = Hybrid::new(&mut data, width);
    for &index in &indexes {
        hybrid.push(index);
    }
    hybrid.finish();
    Chunk { dictionary, dictionary_len: distinct as usize, data, values: indexes.len() }
}

/// A string column given as indexes into `names`; `absent` marks a null.
fn name_chunk(indexes: &[u32], names: &[Str], absent: Option<u32>) -> Chunk {
    // Only the names in use go into the dictionary, in first-seen order.
    let mut entry_of = vec![u32::MAX; names.len()];
    let mut dictionary = Vec::new();
    let mut distinct = 0u32;
    for &index in indexes {
        if Some(index) != absent && entry_of[index as usize] == u32::MAX {
            entry_of[index as usize] = distinct;
            distinct += 1;
            dictionary.extend_from_slice(&(names[index as usize].len() as u32).to_le_bytes());
            dictionary.extend_from_slice(names[index as usize]);
        }
    }
    let mut data = Vec::new();
    if absent.is_some() {
        // Definition levels: 1 where a value is present. Prefixed with their byte length.
        let mut levels = Vec::new();
        let mut hybrid = Hybrid::new(&mut levels, 1);
        for &index in indexes {
            hybrid.push((Some(index) != absent) as u32);
        }
        hybrid.finish();
        data.extend_from_slice(&(levels.len() as u32).to_le_bytes());
        data.append(&mut levels);
    }
    let width = bit_width(distinct.saturating_sub(1));
    data.push(width as u8);
    let mut hybrid = Hybrid::new(&mut data, width);
    for &index in indexes {
        if Some(index) != absent {
            hybrid.push(entry_of[index as usize]);
        }
    }
    hybrid.finish();
    Chunk { dictionary, dictionary_len: distinct as usize, data, values: indexes.len() }
}

/// The ID column of `user_daily`. A person's rows are consecutive, so each stretch of equal
/// IDs is one dictionary entry and one run.
fn id_chunk(ids: &[Id]) -> Chunk {
    let mut dictionary = Vec::with_capacity(ids.len() * 6);
    let mut runs: Vec<u32> = Vec::new();
    let mut i = 0;
    while i < ids.len() {
        let mut end = i + 1;
        while end < ids.len() && ids[end] == ids[i] {
            end += 1;
        }
        let at = dictionary.len();
        dictionary.extend_from_slice(&[0; 4]);
        key::write_bytes(ids[i], &mut dictionary);
        let len = (dictionary.len() - at - 4) as u32;
        dictionary[at..at + 4].copy_from_slice(&len.to_le_bytes());
        runs.push((end - i) as u32);
        i = end;
    }
    let width = bit_width((runs.len() as u32).saturating_sub(1));
    // One RLE run per person, whatever its length: about 4 bytes for a person's rows, where
    // bit-packing would spend `width` bits on every row.
    let mut data = Vec::with_capacity(runs.len() * 5 + 1);
    data.push(width as u8);
    let bytes = width.div_ceil(8) as usize;
    for (entry, &count) in runs.iter().enumerate() {
        varint(&mut data, (count as u64) << 1);
        data.extend_from_slice(&(entry as u32).to_le_bytes()[..bytes]);
    }
    Chunk { dictionary, dictionary_len: runs.len(), data, values: ids.len() }
}

// ---- File assembly ----------------------------------------------------------------------

fn page_header(kind: i32, size: usize, values: usize) -> Vec<u8> {
    let mut t = Thrift::default();
    t.open_bare();
    t.i32(1, kind);
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    if kind == DATA_PAGE {
        t.open(5);
        t.i32(1, values as i32);
        t.i32(2, RLE_DICTIONARY);
        // The level encodings are required fields even for a column without levels.
        t.i32(3, RLE);
        t.i32(4, RLE);
        t.close();
    } else {
        t.open(7);
        t.i32(1, values as i32);
        t.i32(2, PLAIN);
        t.close();
    }
    t.close();
    t.out
}

fn write_file(path: &Path, table: &str, columns: &[Column], chunks: Vec<Chunk>, rows: usize) -> Res<()> {
    let mut file = Vec::with_capacity(chunks.iter().map(|c| c.dictionary.len() + c.data.len() + 64).sum::<usize>() + 1024);
    file.extend_from_slice(b"PAR1");
    // (dictionary page offset, data page offset, total size) per column.
    let mut placed = Vec::new();
    if rows > 0 {
        for chunk in &chunks {
            let start = file.len();
            file.extend_from_slice(&page_header(DICTIONARY_PAGE, chunk.dictionary.len(), chunk.dictionary_len));
            file.extend_from_slice(&chunk.dictionary);
            let data_at = file.len();
            file.extend_from_slice(&page_header(DATA_PAGE, chunk.data.len(), chunk.values));
            file.extend_from_slice(&chunk.data);
            placed.push((start, data_at, file.len() - start));
        }
    }

    let mut t = Thrift::default();
    t.open_bare();
    t.i32(1, 1); // format version
    t.list(2, T_STRUCT, columns.len() + 1);
    t.open_bare();
    t.string(4, table.as_bytes());
    t.i32(5, columns.len() as i32);
    t.close();
    for column in columns {
        t.open_bare();
        t.i32(1, column.physical);
        t.i32(3, if column.optional { OPTIONAL } else { REQUIRED });
        t.string(4, column.name.as_bytes());
        // Both the older converted type and the newer logical type, for every reader.
        match column.logical {
            Logical::String => {
                t.i32(6, 0);
                t.open(10);
                t.open(1);
                t.close();
                t.close();
            }
            Logical::Date => {
                t.i32(6, 6);
                t.open(10);
                t.open(6);
                t.close();
                t.close();
            }
            Logical::None => {}
        }
        t.close();
    }
    t.i64(3, rows as i64);
    t.list(4, T_STRUCT, if rows > 0 { 1 } else { 0 });
    if rows > 0 {
        t.open_bare();
        t.list(1, T_STRUCT, columns.len());
        for (column, &(dictionary_at, data_at, size)) in columns.iter().zip(&placed) {
            t.open_bare();
            t.i64(2, dictionary_at as i64);
            t.open(3);
            t.i32(1, column.physical);
            t.list(2, T_I32, 3);
            for encoding in [PLAIN, RLE, RLE_DICTIONARY] {
                t.zigzag(encoding as i64);
            }
            t.list(3, T_BINARY, 1);
            t.bare_string(column.name.as_bytes());
            t.i32(4, 0); // uncompressed
            t.i64(5, rows as i64);
            t.i64(6, size as i64);
            t.i64(7, size as i64);
            t.i64(9, data_at as i64);
            t.i64(11, dictionary_at as i64);
            t.close();
            t.close();
        }
        t.i64(2, placed.iter().map(|p| p.2 as i64).sum());
        t.i64(3, rows as i64);
        t.close();
    }
    t.string(6, b"shopping-sessions");
    t.close();

    file.extend_from_slice(&t.out);
    file.extend_from_slice(&(t.out.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::write(path, file)?;
    Ok(())
}

const fn column(name: &'static str, physical: i32, logical: Logical, optional: bool) -> Column {
    Column { name, physical, logical, optional }
}

pub fn write_user_daily(path: &Path, rows: &UserRows, categories: &[Str]) -> Res<()> {
    let columns = [
        column("user_id", BYTE_ARRAY, Logical::String, false),
        column("day", INT32, Logical::Date, false),
        column("sessions", INT64, Logical::None, false),
        column("events", INT64, Logical::None, false),
        column("revenue_cents", INT64, Logical::None, false),
        column("top_category", BYTE_ARRAY, Logical::String, true),
    ];
    let chunks = vec![
        id_chunk(&rows.user_id),
        int_chunk(rows.day.iter().map(|&d| d as i64), 4),
        int_chunk(rows.sessions.iter().copied(), 8),
        int_chunk(rows.events.iter().copied(), 8),
        int_chunk(rows.revenue_cents.iter().copied(), 8),
        name_chunk(&rows.top_category, categories, Some(NO_CATEGORY)),
    ];
    write_file(path, "user_daily", &columns, chunks, rows.user_id.len())
}

pub fn write_campaign_daily(path: &Path, rows: &CampaignRows, campaigns: &[Str], channels: &[Str]) -> Res<()> {
    let columns = [
        column("campaign_id", BYTE_ARRAY, Logical::String, false),
        column("channel", BYTE_ARRAY, Logical::String, false),
        column("day", INT32, Logical::Date, false),
        column("sessions", INT64, Logical::None, false),
        column("purchasing_sessions", INT64, Logical::None, false),
        column("revenue_cents", INT64, Logical::None, false),
        column("assisted_sessions", INT64, Logical::None, false),
    ];
    // Channels repeat across campaigns; as a dictionary keyed by campaign they would too, so
    // the channel column gets its own dictionary of distinct names.
    let mut channel_names: Vec<Str> = Vec::new();
    let channel_of: Vec<u32> = channels
        .iter()
        .map(|&name| match channel_names.iter().position(|&known| known == name) {
            Some(index) => index as u32,
            None => {
                channel_names.push(name);
                channel_names.len() as u32 - 1
            }
        })
        .collect();
    let channel: Vec<u32> = rows.campaign.iter().map(|&c| channel_of[c as usize]).collect();
    let chunks = vec![
        name_chunk(&rows.campaign, campaigns, None),
        name_chunk(&channel, &channel_names, None),
        int_chunk(rows.day.iter().map(|&d| d as i64), 4),
        int_chunk(rows.sessions.iter().copied(), 8),
        int_chunk(rows.purchasing_sessions.iter().copied(), 8),
        int_chunk(rows.revenue_cents.iter().copied(), 8),
        int_chunk(rows.assisted_sessions.iter().copied(), 8),
    ];
    write_file(path, "campaign_daily", &columns, chunks, rows.campaign.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decodes hybrid data back into `count` values.
    fn decode(mut data: &[u8], width: u32, count: usize) -> Vec<u32> {
        fn read_varint(data: &mut &[u8]) -> u64 {
            let (mut v, mut shift) = (0u64, 0);
            loop {
                let byte = data[0];
                *data = &data[1..];
                v |= ((byte & 0x7f) as u64) << shift;
                if byte < 0x80 {
                    return v;
                }
                shift += 7;
            }
        }
        let mut out = Vec::new();
        while out.len() < count {
            let header = read_varint(&mut data);
            if header & 1 == 0 {
                let bytes = width.div_ceil(8) as usize;
                let mut raw = [0u8; 4];
                raw[..bytes].copy_from_slice(&data[..bytes]);
                data = &data[bytes..];
                out.extend(std::iter::repeat_n(u32::from_le_bytes(raw), (header >> 1) as usize));
            } else {
                let values = (header >> 1) as usize * 8;
                let bytes = (values * width as usize).div_ceil(8);
                let mut bits: u128 = 0;
                let mut have = 0;
                let mut next = 0;
                for _ in 0..values {
                    while have < width {
                        bits |= (data[next] as u128) << have;
                        next += 1;
                        have += 8;
                    }
                    out.push((bits & ((1u128 << width) - 1)) as u32);
                    bits >>= width;
                    have -= width;
                }
                data = &data[bytes..];
            }
        }
        assert!(data.is_empty(), "trailing bytes");
        out.truncate(count);
        out
    }

    #[test]
    fn hybrid_round_trips() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..3000 {
            let width = 1 + (next() % 20) as u32;
            let len = (next() % 200) as usize;
            let max = (1u32 << width) - 1;
            let mut values = Vec::new();
            let mut out = Vec::new();
            let mut hybrid = Hybrid::new(&mut out, width);
            while values.len() < len {
                let v = (next() as u32) & max;
                // Mix single values with runs of many lengths.
                let count = match round % 3 {
                    0 => 1,
                    1 => 1 + (next() % 40) as usize,
                    _ => {
                        if next() % 4 == 0 {
                            1 + (next() % 100) as usize
                        } else {
                            1
                        }
                    }
                };
                if next() % 2 == 0 {
                    hybrid.repeat(v, count);
                } else {
                    for _ in 0..count {
                        hybrid.push(v);
                    }
                }
                values.extend(std::iter::repeat_n(v, count));
            }
            hybrid.finish();
            assert_eq!(decode(&out, width, values.len()), values, "width {width} len {len}");
        }
    }

    #[test]
    fn bit_widths() {
        assert_eq!([0, 1, 2, 3, 4, 255, 256, u32::MAX].map(bit_width), [1, 1, 2, 2, 3, 8, 9, 32]);
    }
}
