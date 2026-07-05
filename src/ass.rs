use std::collections::{HashMap, HashSet};

use anyhow::{Context, bail};
use regex::Regex;

use crate::models::{EmbeddedFont, FontSlot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BomKind {
    None,
    Utf8,
    Utf16Le,
    Utf16Be,
}

#[derive(Clone, Debug)]
pub struct DecodedSubtitle {
    pub text: String,
    pub bom: BomKind,
}

#[derive(Clone, Debug, Default)]
pub struct StyleInfo {
    pub font: String,
    pub bold: bool,
    pub italic: bool,
}

#[derive(Clone, Debug, Default)]
pub struct FontUsage {
    pub normal: HashSet<u32>,
    pub bold: HashSet<u32>,
    pub italic: HashSet<u32>,
    pub bold_italic: HashSet<u32>,
}

impl FontUsage {
    pub fn add(&mut self, slot: FontSlot, cp: u32) {
        match slot {
            FontSlot::Normal => {
                self.normal.insert(cp);
            }
            FontSlot::Bold => {
                self.bold.insert(cp);
            }
            FontSlot::Italic => {
                self.italic.insert(cp);
            }
            FontSlot::BoldItalic => {
                self.bold_italic.insert(cp);
            }
        }
    }

    pub fn all_codepoints(&self) -> Vec<u32> {
        let mut cps: Vec<u32> = self
            .normal
            .iter()
            .chain(&self.bold)
            .chain(&self.italic)
            .chain(&self.bold_italic)
            .copied()
            .collect();
        cps.sort_unstable();
        cps.dedup();
        cps
    }

    pub fn slot_codepoints(&self, slot: FontSlot) -> Vec<u32> {
        let set = match slot {
            FontSlot::Normal => &self.normal,
            FontSlot::Bold => &self.bold,
            FontSlot::Italic => &self.italic,
            FontSlot::BoldItalic => &self.bold_italic,
        };
        let mut cps: Vec<u32> = set.iter().copied().collect();
        cps.sort_unstable();
        cps
    }
}

#[derive(Clone, Debug)]
pub struct ParsedSubtitle {
    pub newline: String,
    pub styles: HashMap<String, StyleInfo>,
    pub usages: HashMap<String, FontUsage>,
    pub drawing_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssEmbeddedFont {
    pub fontname: String,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrawRestoreEntry {
    pub data: String,
    pub ch: String,
    pub flags: u8,
}

#[derive(Clone, Debug, Default)]
pub struct DrawSubsetRewrite {
    pub text: String,
    pub entries: Vec<DrawRestoreEntry>,
}

pub fn decode_subtitle(bytes: &[u8]) -> anyhow::Result<DecodedSubtitle> {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        let text = std::str::from_utf8(&bytes[3..])
            .context("invalid UTF-8 after BOM")?
            .to_string();
        return Ok(DecodedSubtitle {
            text,
            bom: BomKind::Utf8,
        });
    }
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let text = decode_utf16(&bytes[2..], true)?;
        return Ok(DecodedSubtitle {
            text,
            bom: BomKind::Utf16Le,
        });
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let text = decode_utf16(&bytes[2..], false)?;
        return Ok(DecodedSubtitle {
            text,
            bom: BomKind::Utf16Be,
        });
    }
    let text = std::str::from_utf8(bytes)
        .context("subtitle has no Unicode BOM and is not valid UTF-8")?
        .to_string();
    Ok(DecodedSubtitle {
        text,
        bom: BomKind::None,
    })
}

pub fn encode_subtitle(text: &str, bom: &BomKind) -> Vec<u8> {
    match bom {
        BomKind::None => text.as_bytes().to_vec(),
        BomKind::Utf8 => {
            let mut out = vec![0xEF, 0xBB, 0xBF];
            out.extend_from_slice(text.as_bytes());
            out
        }
        BomKind::Utf16Le => {
            let mut out = vec![0xFF, 0xFE];
            for ch in text.encode_utf16() {
                out.extend_from_slice(&ch.to_le_bytes());
            }
            out
        }
        BomKind::Utf16Be => {
            let mut out = vec![0xFE, 0xFF];
            for ch in text.encode_utf16() {
                out.extend_from_slice(&ch.to_be_bytes());
            }
            out
        }
    }
}

fn decode_utf16(bytes: &[u8], little: bool) -> anyhow::Result<String> {
    if bytes.len() % 2 != 0 {
        bail!("UTF-16 subtitle has an odd byte length");
    }
    let words: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| {
            if little {
                u16::from_le_bytes([c[0], c[1]])
            } else {
                u16::from_be_bytes([c[0], c[1]])
            }
        })
        .collect();
    String::from_utf16(&words).context("invalid UTF-16 subtitle")
}

pub fn parse_subtitle(text: &str) -> ParsedSubtitle {
    let newline = detect_newline(text).to_string();
    let mut section = String::new();
    let mut style_format: Vec<String> = Vec::new();
    let mut event_format: Vec<String> = Vec::new();
    let mut styles = HashMap::new();
    let mut usages: HashMap<String, FontUsage> = HashMap::new();
    let mut drawing_count = 0usize;

    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        let trimmed = line.trim();
        if let Some(h) = section_header(trimmed) {
            section = h.to_ascii_lowercase();
            continue;
        }
        if section.contains("styles") {
            if let Some(rest) = strip_prefix_ci(trimmed, "Format:") {
                style_format = rest
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect();
            } else if let Some(rest) = strip_prefix_ci(trimmed, "Style:") {
                let fields = split_ass_fields(rest.trim(), style_format.len().max(23));
                let name_idx = style_format.iter().position(|f| f == "name").unwrap_or(0);
                let font_idx = style_format
                    .iter()
                    .position(|f| f == "fontname")
                    .unwrap_or(1);
                let bold_idx = style_format.iter().position(|f| f == "bold");
                let italic_idx = style_format.iter().position(|f| f == "italic");
                if fields.len() > font_idx {
                    let name = fields
                        .get(name_idx)
                        .cloned()
                        .unwrap_or_default()
                        .trim()
                        .to_string();
                    let font = norm_font(fields[font_idx].trim());
                    if !name.is_empty() && !font.is_empty() {
                        styles.insert(
                            name,
                            StyleInfo {
                                font,
                                bold: bold_idx
                                    .and_then(|i| fields.get(i))
                                    .map(|v| parse_boolish_style(v))
                                    .unwrap_or(false),
                                italic: italic_idx
                                    .and_then(|i| fields.get(i))
                                    .map(|v| parse_boolish_style(v))
                                    .unwrap_or(false),
                            },
                        );
                    }
                }
            }
        } else if section == "events" {
            if let Some(rest) = strip_prefix_ci(trimmed, "Format:") {
                event_format = rest
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect();
            } else if let Some(rest) = strip_prefix_ci(line.trim_start(), "Dialogue:") {
                if event_format.is_empty() {
                    continue;
                }
                let fields = split_ass_fields(rest.trim_start(), event_format.len());
                let Some(style_idx) = event_format.iter().position(|f| f == "style") else {
                    continue;
                };
                let Some(text_idx) = event_format.iter().position(|f| f == "text") else {
                    continue;
                };
                let style_name = fields.get(style_idx).map(|s| s.trim()).unwrap_or("");
                let base_style = styles
                    .get(style_name)
                    .cloned()
                    .unwrap_or_else(|| StyleInfo {
                        font: "Arial".to_string(),
                        bold: false,
                        italic: false,
                    });
                if let Some(text_field) = fields.get(text_idx) {
                    drawing_count +=
                        parse_dialogue_text(text_field, &base_style, &styles, &mut usages);
                }
            }
        }
    }

    ParsedSubtitle {
        newline,
        styles,
        usages,
        drawing_count,
    }
}

fn parse_dialogue_text(
    text: &str,
    base_style: &StyleInfo,
    styles: &HashMap<String, StyleInfo>,
    usages: &mut HashMap<String, FontUsage>,
) -> usize {
    let mut state = base_style.clone();
    let mut drawing_level = 0i32;
    let mut drawing_count = 0usize;
    let mut i = 0usize;
    let chars: Vec<char> = text.chars().collect();

    while i < chars.len() {
        if chars[i] == '{' {
            if let Some(end) = chars[i + 1..].iter().position(|c| *c == '}') {
                let tag: String = chars[i + 1..i + 1 + end].iter().collect();
                let was_drawing = drawing_level > 0;
                apply_override_tags(&tag, &mut state, base_style, styles, &mut drawing_level);
                if !was_drawing && drawing_level > 0 {
                    drawing_count += 1;
                }
                i += end + 2;
                continue;
            }
        }
        if drawing_level <= 0 {
            if chars[i] == '\\' && i + 1 < chars.len() {
                let next = chars[i + 1];
                if matches!(next, 'N' | 'n' | 'h') {
                    i += 2;
                    continue;
                }
            }
            let ch = chars[i];
            if !ch.is_control() {
                let slot = slot_for_state(&state);
                usages
                    .entry(norm_font(&state.font))
                    .or_default()
                    .add(slot, ch as u32);
            }
        }
        i += 1;
    }
    drawing_count
}

fn apply_override_tags(
    tag: &str,
    state: &mut StyleInfo,
    base_style: &StyleInfo,
    styles: &HashMap<String, StyleInfo>,
    drawing_level: &mut i32,
) {
    let mut i = 0usize;
    let bytes = tag.as_bytes();
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
            continue;
        }
        i += 1;
        if tag[i..].starts_with("fn") {
            let (arg, next) = read_tag_arg(tag, i + 2);
            let font = norm_font(&arg);
            if !font.is_empty() && font != "0" {
                state.font = font;
            }
            i = next;
        } else if tag[i..].starts_with('r') {
            let (arg, next) = read_tag_arg(tag, i + 1);
            let name = arg.trim();
            if name.is_empty() {
                *state = base_style.clone();
            } else if let Some(s) = styles.get(name) {
                *state = s.clone();
            }
            i = next;
        } else if tag[i..].starts_with('b') {
            let (arg, next) = read_tag_arg(tag, i + 1);
            let v = arg.trim();
            if v.is_empty() {
                state.bold = true;
            } else if let Ok(n) = v.parse::<i32>() {
                state.bold = n != 0 && n >= 600 || n == 1 || n == -1;
            }
            i = next;
        } else if tag[i..].starts_with('i') {
            let (arg, next) = read_tag_arg(tag, i + 1);
            state.italic = parse_boolish_style(arg.trim());
            i = next;
        } else if tag[i..].starts_with('p') {
            let (arg, next) = read_tag_arg(tag, i + 1);
            *drawing_level = arg.trim().parse::<i32>().unwrap_or(0);
            i = next;
        } else {
            while i < bytes.len() && bytes[i] != b'\\' {
                i += 1;
            }
        }
    }
}

fn read_tag_arg(s: &str, start: usize) -> (String, usize) {
    let rest = &s[start..];
    if let Some(stripped) = rest.strip_prefix('(') {
        if let Some(end) = stripped.find(')') {
            return (stripped[..end].to_string(), start + end + 2);
        }
    }
    let end = rest.find('\\').unwrap_or(rest.len());
    (rest[..end].trim().to_string(), start + end)
}

pub fn rewrite_with_embedded_fonts(
    text: &str,
    newline: &str,
    rename_map: &HashMap<String, String>,
    embedded_fonts: &[EmbeddedFont],
) -> String {
    let font_section = build_fonts_section(embedded_fonts, newline);
    let mut out = Vec::new();
    let mut section = String::new();
    let mut skipping_fonts = false;
    let mut inserted_fonts = false;
    let mut style_format: Vec<String> = Vec::new();
    let mut inserted_rand_comments = false;
    let mut seen_comments = HashSet::new();
    let mut rand_comments: Vec<String> = embedded_fonts
        .iter()
        .filter(|font| font.original_name != font.embedded_name)
        .filter_map(|font| {
            let key = normalize_lookup_name(&font.embedded_name);
            if seen_comments.insert(key) {
                Some(format!(
                    "; Font Subset: {} - {}",
                    font.embedded_name, font.original_name
                ))
            } else {
                None
            }
        })
        .collect();
    if rand_comments.is_empty() {
        rand_comments = rename_map
            .iter()
            .filter(|(orig, new)| orig.as_str() != new.to_ascii_lowercase())
            .map(|(orig, new)| format!("; Font Subset: {new} - {orig}"))
            .collect();
    }

    for raw in text.lines() {
        let line = raw.trim_end_matches('\r');
        let trimmed = line.trim();
        let header = section_header(trimmed);
        if skipping_fonts {
            if header.is_none() {
                continue;
            }
            skipping_fonts = false;
        }
        if let Some(h) = header {
            let h_lower = h.to_ascii_lowercase();
            if h_lower == "fonts" {
                skipping_fonts = true;
                section = h_lower;
                continue;
            }
            if h_lower == "events" && !inserted_fonts && !font_section.is_empty() {
                out.push(font_section.clone());
                inserted_fonts = true;
            }
            section = h_lower;
            out.push(line.to_string());
            if section == "script info" && !inserted_rand_comments {
                for c in &rand_comments {
                    out.push(c.clone());
                }
                inserted_rand_comments = true;
            }
            continue;
        }

        if section == "script info" && strip_prefix_ci(trimmed, "; Font Subset:").is_some() {
            continue;
        }
        if section.contains("styles") {
            if let Some(rest) = strip_prefix_ci(trimmed, "Format:") {
                style_format = rest
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect();
                out.push(line.to_string());
            } else if strip_prefix_ci(trimmed, "Style:").is_some() {
                out.push(rewrite_style_line(line, &style_format, rename_map));
            } else {
                out.push(line.to_string());
            }
        } else if section == "events" {
            out.push(rewrite_fn_tags(line, rename_map));
        } else {
            out.push(line.to_string());
        }
    }

    if !inserted_fonts && !font_section.is_empty() {
        out.push(font_section);
    }
    let mut result = out.join(newline);
    if text.ends_with('\n') {
        result.push_str(newline);
    }
    result
}

pub fn parse_embedded_fonts(text: &str) -> Vec<AssEmbeddedFont> {
    let mut fonts = Vec::new();
    let mut section = String::new();
    let mut current_name: Option<String> = None;
    let mut current_data = String::new();

    for raw in text.lines() {
        let line = raw.trim_end_matches('\r');
        let trimmed = line.trim();
        if let Some(h) = section_header(trimmed) {
            flush_embedded_font(&mut fonts, &mut current_name, &mut current_data);
            section = h.to_ascii_lowercase();
            continue;
        }
        if section != "fonts" {
            continue;
        }
        if let Some(rest) = strip_prefix_ci(trimmed, "fontname:") {
            flush_embedded_font(&mut fonts, &mut current_name, &mut current_data);
            current_name = Some(rest.trim().to_string());
            continue;
        }
        if trimmed.is_empty() {
            flush_embedded_font(&mut fonts, &mut current_name, &mut current_data);
        } else if current_name.is_some() {
            current_data.push_str(trimmed);
        }
    }
    flush_embedded_font(&mut fonts, &mut current_name, &mut current_data);
    fonts
}

fn flush_embedded_font(
    fonts: &mut Vec<AssEmbeddedFont>,
    current_name: &mut Option<String>,
    current_data: &mut String,
) {
    let Some(fontname) = current_name.take() else {
        current_data.clear();
        return;
    };
    if let Ok(data) = ass_uu_decode(current_data) {
        fonts.push(AssEmbeddedFont { fontname, data });
    }
    current_data.clear();
}

pub fn parse_font_subset_comments(text: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for raw in text.lines() {
        let trimmed = raw.trim();
        let Some(rest) = strip_prefix_ci(trimmed, "; Font Subset:") else {
            continue;
        };
        if let Some((subset, original)) = rest.trim().split_once(" - ") {
            let subset = subset.trim();
            let original = original.trim();
            if !subset.is_empty() && !original.is_empty() {
                map.insert(normalize_lookup_name(subset), original.to_string());
            }
        }
    }
    map
}

pub fn rewrite_strip_embedded(
    text: &str,
    newline: &str,
    restore_map: &HashMap<String, String>,
    kept_fonts: &[AssEmbeddedFont],
    draw_map: &HashMap<String, DrawRestoreEntry>,
) -> String {
    let kept_section = build_fonts_section_from_blocks(kept_fonts, newline);
    let mut out = Vec::new();
    let mut section = String::new();
    let mut skipping_fonts = false;
    let mut inserted_fonts = false;
    let mut style_format: Vec<String> = Vec::new();

    for raw in text.lines() {
        let line = raw.trim_end_matches('\r');
        let trimmed = line.trim();
        let header = section_header(trimmed);
        if skipping_fonts {
            if header.is_none() {
                continue;
            }
            skipping_fonts = false;
        }
        if let Some(h) = header {
            let h_lower = h.to_ascii_lowercase();
            if h_lower == "fonts" {
                skipping_fonts = true;
                section = h_lower;
                continue;
            }
            if h_lower == "events" && !inserted_fonts && !kept_section.is_empty() {
                out.push(kept_section.clone());
                inserted_fonts = true;
            }
            section = h_lower;
            out.push(line.to_string());
            continue;
        }
        if is_restored_subset_comment(trimmed, restore_map) {
            continue;
        }
        if section.contains("styles") {
            if let Some(rest) = strip_prefix_ci(trimmed, "Format:") {
                style_format = rest
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect();
                out.push(line.to_string());
            } else if strip_prefix_ci(trimmed, "Style:").is_some() {
                out.push(rewrite_style_line(line, &style_format, restore_map));
            } else {
                out.push(line.to_string());
            }
        } else if section == "events" {
            let restored_fonts = rewrite_fn_tags(line, restore_map);
            out.push(restore_draw_font_runs(&restored_fonts, draw_map));
        } else {
            out.push(line.to_string());
        }
    }

    if !inserted_fonts && !kept_section.is_empty() {
        out.push(kept_section);
    }
    let mut result = out.join(newline);
    if text.ends_with('\n') {
        result.push_str(newline);
    }
    result
}

pub fn rewrite_drawings_as_font(text: &str, newline: &str) -> DrawSubsetRewrite {
    let mut out = Vec::new();
    let mut section = String::new();
    let mut event_format: Vec<String> = Vec::new();
    let mut entries: Vec<DrawRestoreEntry> = Vec::new();
    let mut entry_by_data: HashMap<String, String> = HashMap::new();

    for raw in text.lines() {
        let line = raw.trim_end_matches('\r');
        let trimmed = line.trim();
        if let Some(h) = section_header(trimmed) {
            section = h.to_ascii_lowercase();
            out.push(line.to_string());
            continue;
        }
        if section != "events" {
            out.push(line.to_string());
            continue;
        }
        if let Some(rest) = strip_prefix_ci(trimmed, "Format:") {
            event_format = rest
                .split(',')
                .map(|s| s.trim().to_ascii_lowercase())
                .collect();
            out.push(line.to_string());
            continue;
        }
        let trimmed_start = line.trim_start();
        let Some(rest) = strip_prefix_ci(trimmed_start, "Dialogue:") else {
            out.push(line.to_string());
            continue;
        };
        let Some(text_idx) = event_format.iter().position(|f| f == "text") else {
            out.push(line.to_string());
            continue;
        };
        let leading_len = line.len() - trimmed_start.len();
        let fields = split_ass_fields(rest.trim_start(), event_format.len());
        if fields.len() <= text_idx {
            out.push(line.to_string());
            continue;
        }
        let mut new_fields = fields;
        new_fields[text_idx] =
            rewrite_drawings_in_text(&new_fields[text_idx], &mut entries, &mut entry_by_data);
        out.push(format!(
            "{}Dialogue: {}",
            &line[..leading_len],
            new_fields.join(",")
        ));
    }

    let mut result = out.join(newline);
    if text.ends_with('\n') {
        result.push_str(newline);
    }
    DrawSubsetRewrite {
        text: result,
        entries,
    }
}

fn rewrite_drawings_in_text(
    text: &str,
    entries: &mut Vec<DrawRestoreEntry>,
    entry_by_data: &mut HashMap<String, String>,
) -> String {
    let mut out = String::with_capacity(text.len());
    let mut i = 0usize;
    while i < text.len() {
        let Some(rel_start) = text[i..].find('{') else {
            out.push_str(&text[i..]);
            break;
        };
        let start = i + rel_start;
        out.push_str(&text[i..start]);
        let Some(open_end_rel) = text[start..].find('}') else {
            out.push_str(&text[start..]);
            break;
        };
        let open_end = start + open_end_rel;
        let tag = &text[start + 1..open_end];
        let Some(level) = drawing_level_from_tag(tag).filter(|n| *n > 0 && *n <= 15) else {
            out.push_str(&text[start..=open_end]);
            i = open_end + 1;
            continue;
        };
        let Some((end, has_explicit_close)) = find_drawing_end(text, open_end + 1) else {
            out.push_str(&text[start..=open_end]);
            i = open_end + 1;
            continue;
        };
        let original = &text[start..end];
        let ch = entry_by_data.get(original).cloned().unwrap_or_else(|| {
            let cp = 0xE000 + entries.len() as u32;
            let ch = char::from_u32(cp).unwrap_or('\u{E000}').to_string();
            let flags = (level as u8 & 0x0F) | if has_explicit_close { 0x10 } else { 0 };
            entries.push(DrawRestoreEntry {
                data: original.to_string(),
                ch: ch.clone(),
                flags,
            });
            entry_by_data.insert(original.to_string(), ch.clone());
            ch
        });
        out.push_str("{\\fnASSDrawSubset\\p0}");
        out.push_str(&ch);
        i = end;
    }
    out
}

fn drawing_level_from_tag(tag: &str) -> Option<i32> {
    let mut i = 0usize;
    let bytes = tag.as_bytes();
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
            continue;
        }
        i += 1;
        if i < bytes.len() && bytes[i] == b'p' {
            let (arg, _) = read_tag_arg(tag, i + 1);
            return arg.trim().parse::<i32>().ok();
        }
        while i < bytes.len() && bytes[i] != b'\\' {
            i += 1;
        }
    }
    None
}

fn find_drawing_end(text: &str, mut i: usize) -> Option<(usize, bool)> {
    while i < text.len() {
        let rel = text[i..].find('{')?;
        let start = i + rel;
        let end_rel = text[start..].find('}')?;
        let end = start + end_rel;
        let tag = &text[start + 1..end];
        if drawing_level_from_tag(tag) == Some(0) {
            return Some((end + 1, true));
        }
        i = end + 1;
    }
    None
}

fn is_restored_subset_comment(trimmed: &str, restore_map: &HashMap<String, String>) -> bool {
    let Some(rest) = strip_prefix_ci(trimmed, "; Font Subset:") else {
        return false;
    };
    rest.trim()
        .split_once(" - ")
        .map(|(subset, _)| restore_map.contains_key(&normalize_lookup_name(subset)))
        .unwrap_or(false)
}

fn restore_draw_font_runs(line: &str, draw_map: &HashMap<String, DrawRestoreEntry>) -> String {
    if draw_map.is_empty() {
        return line.to_string();
    }
    let re = Regex::new(r"\{([^}]*)\\fn(ASSDrawSubset[^\\}]*)[^}]*\}([^\{])")
        .expect("draw restore regex");
    re.replace_all(line, |caps: &regex::Captures<'_>| {
        let ch = caps.get(3).map(|m| m.as_str()).unwrap_or("");
        draw_map
            .get(ch)
            .map(|entry| entry.data.clone())
            .unwrap_or_else(|| caps.get(0).unwrap().as_str().to_string())
    })
    .to_string()
}

fn build_fonts_section(embedded_fonts: &[EmbeddedFont], newline: &str) -> String {
    if embedded_fonts.is_empty() {
        return String::new();
    }
    let mut lines = vec!["[Fonts]".to_string()];
    for font in embedded_fonts {
        lines.push(format!(
            "fontname: {}{}",
            font.embedded_name,
            font.slot.suffix()
        ));
        let enc = ass_uu_encode(&font.data);
        for chunk in enc.as_bytes().chunks(80) {
            lines.push(String::from_utf8_lossy(chunk).to_string());
        }
        lines.push(String::new());
    }
    lines.join(newline)
}

fn build_fonts_section_from_blocks(fonts: &[AssEmbeddedFont], newline: &str) -> String {
    if fonts.is_empty() {
        return String::new();
    }
    let mut lines = vec!["[Fonts]".to_string()];
    for font in fonts {
        lines.push(format!("fontname: {}", font.fontname));
        let enc = ass_uu_encode(&font.data);
        for chunk in enc.as_bytes().chunks(80) {
            lines.push(String::from_utf8_lossy(chunk).to_string());
        }
        lines.push(String::new());
    }
    lines.join(newline)
}

fn rewrite_style_line(
    line: &str,
    style_format: &[String],
    rename_map: &HashMap<String, String>,
) -> String {
    let trimmed_start = line.trim_start();
    let Some(rest) = strip_prefix_ci(trimmed_start, "Style:") else {
        return line.to_string();
    };
    let leading_len = line.len() - trimmed_start.len();
    let fields = split_ass_fields(rest.trim_start(), style_format.len().max(23));
    let font_idx = style_format
        .iter()
        .position(|f| f == "fontname")
        .unwrap_or(1);
    if fields.len() <= font_idx {
        return line.to_string();
    }
    let mut new_fields = fields;
    let key = norm_font(&new_fields[font_idx]).to_ascii_lowercase();
    if let Some(new_name) = rename_map.get(&key) {
        new_fields[font_idx] = new_name.clone();
        format!("{}Style: {}", &line[..leading_len], new_fields.join(","))
    } else {
        line.to_string()
    }
}

fn rewrite_fn_tags(line: &str, rename_map: &HashMap<String, String>) -> String {
    if rename_map.is_empty() {
        return line.to_string();
    }
    let re = Regex::new(r"\\fn([^\\}]*)").expect("fn regex");
    re.replace_all(line, |caps: &regex::Captures<'_>| {
        let old = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let key = norm_font(old).to_ascii_lowercase();
        if let Some(new_name) = rename_map.get(&key) {
            format!("\\fn{new_name}")
        } else {
            caps.get(0).unwrap().as_str().to_string()
        }
    })
    .to_string()
}

pub fn ass_uu_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4 / 3 + 4);
    for i in (0..bytes.len()).step_by(3) {
        let rem = bytes.len() - i;
        let b0 = bytes[i] as u32;
        let b1 = if rem > 1 { bytes[i + 1] as u32 } else { 0 };
        let b2 = if rem > 2 { bytes[i + 2] as u32 } else { 0 };
        let v = (b0 << 16) | (b1 << 8) | b2;
        out.push(char::from_u32(((v >> 18) & 0x3f) + 33).unwrap());
        out.push(char::from_u32(((v >> 12) & 0x3f) + 33).unwrap());
        if rem > 1 {
            out.push(char::from_u32(((v >> 6) & 0x3f) + 33).unwrap());
        }
        if rem > 2 {
            out.push(char::from_u32((v & 0x3f) + 33).unwrap());
        }
    }
    out
}

pub fn ass_uu_decode(s: &str) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut chars = Vec::with_capacity(4);
    for ch in s.chars().filter(|c| !c.is_whitespace()) {
        let v = ch as i32 - 33;
        if !(0..=63).contains(&v) {
            bail!("invalid ASS embedded font character");
        }
        chars.push(v as u32);
        if chars.len() == 4 {
            push_decoded_group(&chars, &mut out);
            chars.clear();
        }
    }
    if !chars.is_empty() {
        if chars.len() == 1 {
            bail!("truncated ASS embedded font data");
        }
        let original_len = chars.len();
        while chars.len() < 4 {
            chars.push(0);
        }
        let count = original_len.saturating_sub(1);
        push_decoded_group_limited(&chars, &mut out, count.min(3));
    }
    Ok(out)
}

fn push_decoded_group(chars: &[u32], out: &mut Vec<u8>) {
    push_decoded_group_limited(chars, out, 3);
}

fn push_decoded_group_limited(chars: &[u32], out: &mut Vec<u8>, count: usize) {
    let v = (chars[0] << 18) | (chars[1] << 12) | (chars[2] << 6) | chars[3];
    if count >= 1 {
        out.push(((v >> 16) & 0xff) as u8);
    }
    if count >= 2 {
        out.push(((v >> 8) & 0xff) as u8);
    }
    if count >= 3 {
        out.push((v & 0xff) as u8);
    }
}

pub fn norm_font(name: &str) -> String {
    name.trim().trim_start_matches('@').trim().to_string()
}

pub fn normalize_lookup_name(name: &str) -> String {
    norm_font(name).to_ascii_lowercase()
}

pub fn is_system_font(name: &str) -> bool {
    matches!(
        normalize_lookup_name(name).as_str(),
        "arial"
            | "arial unicode ms"
            | "times new roman"
            | "courier new"
            | "verdana"
            | "tahoma"
            | "microsoft yahei"
            | "microsoft yahei ui"
            | "simhei"
            | "simsun"
            | "ms gothic"
            | "meiryo"
            | "malgun gothic"
    )
}

fn slot_for_state(state: &StyleInfo) -> FontSlot {
    match (state.bold, state.italic) {
        (true, true) => FontSlot::BoldItalic,
        (true, false) => FontSlot::Bold,
        (false, true) => FontSlot::Italic,
        (false, false) => FontSlot::Normal,
    }
}

fn parse_boolish_style(v: &str) -> bool {
    let t = v.trim();
    if let Ok(n) = t.parse::<i32>() {
        n != 0
    } else {
        matches!(t.to_ascii_lowercase().as_str(), "true" | "yes" | "on")
    }
}

fn detect_newline(text: &str) -> &'static str {
    if text.contains("\r\n") { "\r\n" } else { "\n" }
}

fn section_header(line: &str) -> Option<&str> {
    line.strip_prefix('[')?.strip_suffix(']')
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        s.get(prefix.len()..)
    } else {
        None
    }
}

fn split_ass_fields(s: &str, expected: usize) -> Vec<String> {
    if expected == 0 {
        return Vec::new();
    }
    let mut fields = Vec::with_capacity(expected);
    let mut rest = s;
    for _ in 0..expected - 1 {
        if let Some(pos) = rest.find(',') {
            fields.push(rest[..pos].trim().to_string());
            rest = &rest[pos + 1..];
        } else {
            fields.push(rest.trim().to_string());
            rest = "";
        }
    }
    fields.push(rest.trim().to_string());
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
        #[test]
        fn parses_style_and_inline_fonts() {
            let text = "[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic\nStyle: Default,MainFont,20,&H0,&H0,&H0,&H0,-1,0\n[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0:00:00.00,0:00:01.00,Default,Hello {\\fnOther\\i1}界\n";
            let parsed = parse_subtitle(text);
            assert!(parsed.usages["MainFont"].bold.contains(&('H' as u32)));
            assert!(parsed.usages["Other"].bold_italic.contains(&('界' as u32)));
        }

        #[test]
        fn uuencode_matches_ass_subset_algorithm() {
            assert_eq!(ass_uu_encode(&[0]), "!!");
            assert_eq!(ass_uu_encode(&[0, 0]), "!!!");
            assert_eq!(ass_uu_encode(&[0, 0, 0]), "!!!!");
        }

        #[test]
        fn rewrites_styles_and_font_tags() {
            let mut map = HashMap::new();
            map.insert("mainfont".to_string(), "ABCD1234".to_string());
            let text = "[Script Info]\nTitle: x\n[V4+ Styles]\nFormat: Name, Fontname\nStyle: Default,MainFont\n[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0,1,Default,{\\fnMainFont}Hi\n";
            let out = rewrite_with_embedded_fonts(text, "\n", &map, &[]);
            assert!(out.contains("Style: Default,ABCD1234"));
            assert!(out.contains("{\\fnABCD1234}Hi"));
            assert!(out.contains("; Font Subset: ABCD1234 - mainfont"));
        }
    */

    #[test]
    fn parses_style_and_inline_fonts_current() {
        let text = "[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic\nStyle: Default,MainFont,20,&H0,&H0,&H0,&H0,-1,0\n[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0:00:00.00,0:00:01.00,Default,Hello {\\fnOther\\i1}字\n";
        let parsed = parse_subtitle(text);
        assert!(parsed.usages["MainFont"].bold.contains(&('H' as u32)));
        assert!(parsed.usages["Other"].bold_italic.contains(&('字' as u32)));
    }

    #[test]
    fn uuencode_round_trips_ass_embedded_data() {
        assert_eq!(ass_uu_encode(&[0]), "!!");
        assert_eq!(ass_uu_encode(&[0, 0]), "!!!");
        assert_eq!(ass_uu_encode(&[0, 0, 0]), "!!!!");
        let data = b"hello subset";
        assert_eq!(ass_uu_decode(&ass_uu_encode(data)).unwrap(), data);
    }

    #[test]
    fn rewrites_styles_and_font_tags_current() {
        let mut map = HashMap::new();
        map.insert("mainfont".to_string(), "ABCD1234".to_string());
        let text = "[Script Info]\nTitle: x\n[V4+ Styles]\nFormat: Name, Fontname\nStyle: Default,MainFont\n[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0,1,Default,{\\fnMainFont}Hi\n";
        let out = rewrite_with_embedded_fonts(text, "\n", &map, &[]);
        assert!(out.contains("Style: Default,ABCD1234"));
        assert!(out.contains("{\\fnABCD1234}Hi"));
        assert!(out.contains("; Font Subset: ABCD1234 - mainfont"));
    }

    #[test]
    fn parses_embedded_fonts_and_subset_comments() {
        let payload = ass_uu_encode(b"abc");
        let text = format!(
            "[Script Info]\n; Font Subset: ABCD1234 - Main Font\n[Fonts]\nfontname: ABCD1234_0.ttf\n{payload}\n\n[Events]\n"
        );
        let fonts = parse_embedded_fonts(&text);
        assert_eq!(fonts.len(), 1);
        assert_eq!(fonts[0].fontname, "ABCD1234_0.ttf");
        assert_eq!(fonts[0].data, b"abc");
        let comments = parse_font_subset_comments(&text);
        assert_eq!(comments["abcd1234"], "Main Font");
    }

    #[test]
    fn strip_restores_names_keeps_unlisted_fonts() {
        let keep_payload = ass_uu_encode(b"keep");
        let text = format!(
            "[Script Info]\n; Font Subset: RANDNAME - Main Font\n[V4+ Styles]\nFormat: Name, Fontname\nStyle: Default,RANDNAME\n[Fonts]\nfontname: RANDNAME_0.ttf\n!!!!\n\nfontname: Keep_0.ttf\n{keep_payload}\n\n[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0,1,Default,{{\\fnRANDNAME}}Hi\n"
        );
        let mut restore = HashMap::new();
        restore.insert("randname".to_string(), "Main Font".to_string());
        let kept = vec![AssEmbeddedFont {
            fontname: "Keep_0.ttf".to_string(),
            data: b"keep".to_vec(),
        }];
        let out = rewrite_strip_embedded(&text, "\n", &restore, &kept, &HashMap::new());
        assert!(out.contains("Style: Default,Main Font"));
        assert!(out.contains("{\\fnMain Font}Hi"));
        assert!(out.contains("fontname: Keep_0.ttf"));
        assert!(!out.contains("fontname: RANDNAME_0.ttf"));
    }

    #[test]
    fn rewrites_and_restores_drawing_font_runs() {
        let text = "[Events]\nFormat: Layer, Start, End, Style, Text\nDialogue: 0,0,1,Default,{\\p1}m 0 0 l 10 0{\\p0} done\n";
        let rewritten = rewrite_drawings_as_font(text, "\n");
        assert_eq!(rewritten.entries.len(), 1);
        assert!(rewritten.text.contains("{\\fnASSDrawSubset\\p0}"));
        let mut draw_map = HashMap::new();
        draw_map.insert(
            rewritten.entries[0].ch.clone(),
            rewritten.entries[0].clone(),
        );
        let restored =
            rewrite_strip_embedded(&rewritten.text, "\n", &HashMap::new(), &[], &draw_map);
        assert!(restored.contains("{\\p1}m 0 0 l 10 0{\\p0} done"));
    }

    #[test]
    fn case_insensitive_prefix_is_utf8_safe() {
        assert_eq!(strip_prefix_ci("方正准圆_GBK", "Fontname:"), None);
    }
}
