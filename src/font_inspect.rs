use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use memmap2::Mmap;

use crate::models::{FontFaceInfo, FontNameInfo};

const NAME_IDS: &[u16] = &[1, 2, 4, 5, 6, 16, 17, 18, 20, 21, 22, 25];

#[derive(Clone, Copy)]
struct TableRecord {
    offset: usize,
    length: usize,
}

#[derive(Clone)]
struct NameRecord {
    name_id: u16,
    platform_id: u16,
    lang_id: u16,
    value: String,
}

pub async fn inspect_font(path: &Path) -> anyhow::Result<Vec<FontFaceInfo>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || inspect_font_sync(path))
        .await
        .context("font inspect task failed")?
}

fn inspect_font_sync(path: PathBuf) -> anyhow::Result<Vec<FontFaceInfo>> {
    let file =
        File::open(&path).with_context(|| format!("open font for inspect {}", path.display()))?;
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("mmap font for inspect {}", path.display()))?;
    let data = &mmap[..];
    let head = slice_at(data, 0, 12)?;

    let mut faces = Vec::new();
    if &head[0..4] == b"ttcf" {
        let count = be_u32(head, 8)? as usize;
        if count == 0 || count > 4096 {
            bail!("invalid TTC face count: {count}");
        }
        let offsets_buf = slice_at(data, 12, count * 4)?;
        for i in 0..count {
            let offset = be_u32(offsets_buf, i * 4)? as usize;
            faces.push(parse_face(data, offset, i as i32)?);
        }
    } else {
        faces.push(parse_face(data, 0, -1)?);
    }

    if faces.is_empty() {
        bail!("no font faces found");
    }
    Ok(faces)
}

fn parse_face(data: &[u8], base: usize, ttc_index: i32) -> anyhow::Result<FontFaceInfo> {
    let head = slice_at(data, base, 12)?;
    let sfnt = &head[0..4];
    if sfnt != b"OTTO" && sfnt != b"true" && sfnt != b"typ1" && sfnt != [0, 1, 0, 0] {
        bail!("unrecognized sfnt version");
    }
    let num_tables = be_u16(head, 4)? as usize;
    if num_tables == 0 || num_tables > 4096 {
        bail!("invalid sfnt table count: {num_tables}");
    }
    let dir = slice_at(data, base + 12, num_tables * 16)?;
    let mut name = None;
    let mut os2 = None;
    let mut head_table = None;
    for i in 0..num_tables {
        let off = i * 16;
        let tag = &dir[off..off + 4];
        let record = TableRecord {
            offset: be_u32(dir, off + 8)? as usize,
            length: be_u32(dir, off + 12)? as usize,
        };
        match tag {
            b"name" => name = Some(record),
            b"OS/2" => os2 = Some(record),
            b"head" => head_table = Some(record),
            _ => {}
        }
    }
    let Some(name) = name else {
        bail!("font has no name table");
    };
    let name_records = parse_name_table(data, name)?;
    if name_records.is_empty() {
        bail!("font has no usable name records");
    }

    let subfamily = best_name(&name_records, &[17, 2]).unwrap_or_default();
    let os2_buf = os2.and_then(|record| read_table_prefix(data, record, 64).ok());
    let mut weight = os2_buf
        .as_ref()
        .and_then(|buf| (buf.len() >= 6).then(|| be_u16(buf, 4).ok()).flatten())
        .map(i32::from)
        .unwrap_or(400);
    let mut italic = os2_buf
        .as_ref()
        .and_then(|buf| {
            (buf.len() >= 64)
                .then(|| be_u16(buf, 62).ok().map(|bits| bits & 0x01 != 0))
                .flatten()
        })
        .unwrap_or(false);
    italic = italic
        || head_table
            .and_then(|record| read_table_prefix(data, record, 46).ok())
            .and_then(|buf| {
                if buf.len() >= 46 {
                    Some(be_u16(buf, 44).ok()? & 0x02 != 0)
                } else {
                    None
                }
            })
            .unwrap_or(false);

    infer_style_from_subfamily(&subfamily, &mut weight, &mut italic);

    Ok(FontFaceInfo {
        ttc_index,
        family: best_name(&name_records, &[16, 21, 1]),
        full_name: best_name(&name_records, &[4, 18]),
        postscript_name: best_name(&name_records, &[6, 20]),
        subfamily: if subfamily.is_empty() {
            None
        } else {
            Some(subfamily)
        },
        version: best_name(&name_records, &[5]),
        weight,
        italic,
        names: all_interesting_names(&name_records),
    })
}

fn parse_name_table(data: &[u8], record: TableRecord) -> anyhow::Result<Vec<NameRecord>> {
    let len = record.length;
    if !(6..=16 * 1024 * 1024).contains(&len) {
        bail!("invalid name table length: {len}");
    }
    let buf = slice_at(data, record.offset, len)?;
    let format = be_u16(buf, 0)?;
    if format > 1 {
        bail!("unsupported name table format: {format}");
    }
    let count = be_u16(buf, 2)? as usize;
    let string_offset = be_u16(buf, 4)? as usize;
    if 6 + count * 12 > buf.len() || string_offset > buf.len() {
        bail!("corrupt name table");
    }

    let mut out = Vec::new();
    for i in 0..count {
        let off = 6 + i * 12;
        let platform_id = be_u16(buf, off)?;
        let encoding_id = be_u16(buf, off + 2)?;
        let lang_id = be_u16(buf, off + 4)?;
        let name_id = be_u16(buf, off + 6)?;
        if !matches!(platform_id, 0 | 3) {
            continue;
        }
        if !NAME_IDS.contains(&name_id) {
            continue;
        }
        let length = be_u16(buf, off + 8)? as usize;
        let string_rel = be_u16(buf, off + 10)? as usize;
        let start = string_offset.saturating_add(string_rel);
        let end = start.saturating_add(length);
        if start > buf.len() || end > buf.len() || start > end {
            continue;
        }
        let Some(value) = decode_name(platform_id, encoding_id, &buf[start..end]) else {
            continue;
        };
        let value = value.trim().to_string();
        if value.is_empty() {
            continue;
        }
        out.push(NameRecord {
            name_id,
            platform_id,
            lang_id,
            value,
        });
    }
    Ok(out)
}

fn decode_name(platform_id: u16, _encoding_id: u16, bytes: &[u8]) -> Option<String> {
    if platform_id == 0 || platform_id == 3 {
        if !bytes.len().is_multiple_of(2) {
            return None;
        }
        let words: Vec<u16> = bytes
            .chunks_exact(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        return String::from_utf16(&words).ok();
    }
    if bytes
        .iter()
        .all(|b| matches!(*b, 0x09 | 0x0A | 0x0D | 0x20..=0x7E))
    {
        return String::from_utf8(bytes.to_vec()).ok();
    }
    None
}

fn best_name(records: &[NameRecord], ids: &[u16]) -> Option<String> {
    for id in ids {
        if let Some(value) = records
            .iter()
            .find(|r| r.name_id == *id && is_preferred_lang(r))
            .map(|r| r.value.clone())
        {
            return Some(value);
        }
        if let Some(value) = records
            .iter()
            .find(|r| r.name_id == *id)
            .map(|r| r.value.clone())
        {
            return Some(value);
        }
    }
    None
}

fn all_interesting_names(records: &[NameRecord]) -> Vec<FontNameInfo> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for record in records {
        let Some(kind) = name_kind(record.name_id) else {
            continue;
        };
        let key = (kind, record.value.to_ascii_lowercase());
        if !seen.insert(key) {
            continue;
        }
        out.push(FontNameInfo {
            name: record.value.clone(),
            kind: kind.to_string(),
        });
    }
    out
}

fn name_kind(id: u16) -> Option<&'static str> {
    match id {
        1 => Some("family"),
        4 => Some("full"),
        6 => Some("postscript"),
        16 => Some("typographic_family"),
        18 => Some("compatible_full"),
        20 => Some("postscript_cid"),
        21 => Some("wws_family"),
        25 => Some("variations_postscript_prefix"),
        _ => None,
    }
}

fn is_preferred_lang(record: &NameRecord) -> bool {
    (record.platform_id == 3 && matches!(record.lang_id, 0x0409 | 0))
        || (record.platform_id == 0 && record.lang_id == 0)
}

fn infer_style_from_subfamily(subfamily: &str, weight: &mut i32, italic: &mut bool) {
    let low = subfamily.to_ascii_lowercase();
    if *weight == 400 {
        if low.contains("thin") || low.contains("hairline") {
            *weight = 100;
        } else if low.contains("extra light")
            || low.contains("extralight")
            || low.contains("ultra light")
            || low.contains("ultralight")
        {
            *weight = 200;
        } else if low.contains("light") {
            *weight = 300;
        } else if low.contains("medium") {
            *weight = 500;
        } else if low.contains("semi bold")
            || low.contains("semibold")
            || low.contains("demi bold")
            || low.contains("demibold")
        {
            *weight = 600;
        } else if low.contains("extra bold")
            || low.contains("extrabold")
            || low.contains("ultra bold")
            || low.contains("ultrabold")
        {
            *weight = 800;
        } else if low.contains("black") || low.contains("heavy") {
            *weight = 900;
        } else if low.contains("bold") {
            *weight = 700;
        }
    }
    *italic = *italic || low.contains("italic") || low.contains("oblique");
}

fn read_table_prefix(data: &[u8], record: TableRecord, max_len: usize) -> anyhow::Result<&[u8]> {
    slice_at(data, record.offset, record.length.min(max_len))
}

fn slice_at(buf: &[u8], offset: usize, len: usize) -> anyhow::Result<&[u8]> {
    let end = offset
        .checked_add(len)
        .context("font table offset overflow")?;
    if end > buf.len() {
        bail!("font table out of bounds");
    }
    Ok(&buf[offset..end])
}

fn be_u16(buf: &[u8], off: usize) -> anyhow::Result<u16> {
    if off + 2 > buf.len() {
        bail!("u16 out of bounds");
    }
    Ok(u16::from_be_bytes([buf[off], buf[off + 1]]))
}

fn be_u32(buf: &[u8], off: usize) -> anyhow::Result<u32> {
    if off + 4 > buf.len() {
        bail!("u32 out of bounds");
    }
    Ok(u32::from_be_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
    ]))
}
