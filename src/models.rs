use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FontFaceInfo {
    pub ttc_index: i32,
    pub family: Option<String>,
    pub full_name: Option<String>,
    pub postscript_name: Option<String>,
    pub subfamily: Option<String>,
    pub version: Option<String>,
    pub weight: i32,
    pub italic: bool,
    pub names: Vec<FontNameInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FontNameInfo {
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct FontCandidate {
    pub file_id: i64,
    pub face_id: i64,
    pub path: String,
    pub full_hash: String,
    pub ttc_index: i32,
    pub family: String,
    pub subfamily: String,
    pub weight: i32,
    pub italic: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum FontSlot {
    Normal,
    Bold,
    Italic,
    BoldItalic,
}

impl FontSlot {
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Normal => "_0.ttf",
            Self::Bold => "_B0.ttf",
            Self::Italic => "_I0.ttf",
            Self::BoldItalic => "_BI0.ttf",
        }
    }

    pub fn target(self) -> (i32, bool) {
        match self {
            Self::Normal => (400, false),
            Self::Bold => (700, false),
            Self::Italic => (400, true),
            Self::BoldItalic => (700, true),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Bold => "bold",
            Self::Italic => "italic",
            Self::BoldItalic => "boldItalic",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmbeddedFont {
    pub original_name: String,
    pub embedded_name: String,
    pub slot: FontSlot,
    pub data: Vec<u8>,
    pub orig_size: u64,
    pub subset_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobMode {
    Subset,
    StripEmbedded,
}

impl JobMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Subset => "subset",
            Self::StripEmbedded => "strip_embedded",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "strip_embedded" => Self::StripEmbedded,
            _ => Self::Subset,
        }
    }
}
