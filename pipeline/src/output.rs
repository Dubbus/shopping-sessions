//! Parquet output for the two tables.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::{SerializedFileWriter, SerializedRowGroupWriter};
use parquet::schema::parser::parse_message_type;

use crate::key::Key;
use crate::table::Str;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Rows of `user_daily`, column by column.
#[derive(Default)]
pub struct UserRows {
    pub user_id: Vec<Key>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub events: Vec<i64>,
    pub revenue_cents: Vec<i64>,
    /// `None` when the day has no purchases.
    pub top_category: Vec<Option<Str>>,
}

/// Rows of `campaign_daily`, column by column.
#[derive(Default)]
pub struct CampaignRows {
    pub campaign_id: Vec<Str>,
    pub channel: Vec<Str>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing_sessions: Vec<i64>,
    pub revenue_cents: Vec<i64>,
    pub assisted_sessions: Vec<i64>,
}

fn writer(path: &Path, schema: &str) -> Res<SerializedFileWriter<File>> {
    let schema = Arc::new(parse_message_type(schema)?);
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    Ok(SerializedFileWriter::new(File::create(path)?, schema, Arc::new(props))?)
}

fn strings(group: &mut SerializedRowGroupWriter<'_, File>, values: &[Str]) -> Res<()> {
    let values: Vec<ByteArray> = values.iter().map(|s| ByteArray::from(Bytes::from_static(s))).collect();
    let mut column = group.next_column()?.expect("column");
    column.typed::<ByteArrayType>().write_batch(&values, None, None)?;
    column.close()?;
    Ok(())
}

/// The ID column: all names packed into one buffer, each value a slice of it.
fn keys(group: &mut SerializedRowGroupWriter<'_, File>, values: &[Key]) -> Res<()> {
    let mut packed = Vec::with_capacity(values.len() * 12);
    let mut ends = Vec::with_capacity(values.len());
    for key in values {
        packed.extend_from_slice(key.bytes());
        ends.push(packed.len());
    }
    let packed = Bytes::from(packed);
    let mut start = 0;
    let values: Vec<ByteArray> = ends
        .iter()
        .map(|&end| {
            let value = ByteArray::from(packed.slice(start..end));
            start = end;
            value
        })
        .collect();
    let mut column = group.next_column()?.expect("column");
    column.typed::<ByteArrayType>().write_batch(&values, None, None)?;
    column.close()?;
    Ok(())
}

fn optional_strings(group: &mut SerializedRowGroupWriter<'_, File>, values: &[Option<Str>]) -> Res<()> {
    let levels: Vec<i16> = values.iter().map(|v| v.is_some() as i16).collect();
    let present: Vec<ByteArray> = values.iter().flatten().map(|s| ByteArray::from(Bytes::from_static(s))).collect();
    let mut column = group.next_column()?.expect("column");
    column.typed::<ByteArrayType>().write_batch(&present, Some(&levels), None)?;
    column.close()?;
    Ok(())
}

fn int32s(group: &mut SerializedRowGroupWriter<'_, File>, values: &[i32]) -> Res<()> {
    let mut column = group.next_column()?.expect("column");
    column.typed::<Int32Type>().write_batch(values, None, None)?;
    column.close()?;
    Ok(())
}

fn int64s(group: &mut SerializedRowGroupWriter<'_, File>, values: &[i64]) -> Res<()> {
    let mut column = group.next_column()?.expect("column");
    column.typed::<Int64Type>().write_batch(values, None, None)?;
    column.close()?;
    Ok(())
}

pub fn write_user_daily(path: &Path, rows: &UserRows) -> Res<()> {
    let mut file = writer(
        path,
        "message user_daily {
            REQUIRED BYTE_ARRAY user_id (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 events;
            REQUIRED INT64 revenue_cents;
            OPTIONAL BYTE_ARRAY top_category (UTF8);
        }",
    )?;
    let mut group = file.next_row_group()?;
    keys(&mut group, &rows.user_id)?;
    int32s(&mut group, &rows.day)?;
    int64s(&mut group, &rows.sessions)?;
    int64s(&mut group, &rows.events)?;
    int64s(&mut group, &rows.revenue_cents)?;
    optional_strings(&mut group, &rows.top_category)?;
    group.close()?;
    file.close()?;
    Ok(())
}

pub fn write_campaign_daily(path: &Path, rows: &CampaignRows) -> Res<()> {
    let mut file = writer(
        path,
        "message campaign_daily {
            REQUIRED BYTE_ARRAY campaign_id (UTF8);
            REQUIRED BYTE_ARRAY channel (UTF8);
            REQUIRED INT32 day (DATE);
            REQUIRED INT64 sessions;
            REQUIRED INT64 purchasing_sessions;
            REQUIRED INT64 revenue_cents;
            REQUIRED INT64 assisted_sessions;
        }",
    )?;
    let mut group = file.next_row_group()?;
    strings(&mut group, &rows.campaign_id)?;
    strings(&mut group, &rows.channel)?;
    int32s(&mut group, &rows.day)?;
    int64s(&mut group, &rows.sessions)?;
    int64s(&mut group, &rows.purchasing_sessions)?;
    int64s(&mut group, &rows.revenue_cents)?;
    int64s(&mut group, &rows.assisted_sessions)?;
    group.close()?;
    file.close()?;
    Ok(())
}
