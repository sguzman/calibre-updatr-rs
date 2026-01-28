use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub title: String,
    pub authors: Vec<String>,
    pub publisher: String,
    pub pubdate: String,
    pub languages: Vec<String>,
    pub isbn: String,
    pub identifiers: HashMap<String, String>,
    pub tags: Vec<String>,
    pub comments_present: bool,
    pub cover_present: bool,
}

fn sort_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(v) = map.get(&k) {
                    out.insert(k, sort_value(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sort_value).collect()),
        _ => value.clone(),
    }
}

fn stable_json_string(value: &Value) -> Result<String> {
    let sorted = sort_value(value);
    Ok(serde_json::to_string(&sorted)?)
}

fn sha256_text(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn normalize_identifiers(val: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Value::Object(map) = val {
        for (k, v) in map {
            let key = k.trim().to_lowercase();
            let val_s = v.as_str().unwrap_or(&v.to_string()).trim().to_string();
            if !key.is_empty() && !val_s.is_empty() {
                out.insert(key, val_s);
            }
        }
    }
    out
}

fn normalize_languages(val: &Value) -> Vec<String> {
    match val {
        Value::Null => vec![],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_lowercase()))
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = val.as_str().unwrap_or(&val.to_string()).trim().to_lowercase();
            if s.is_empty() {
                vec![]
            } else {
                vec![s]
            }
        }
    }
}

fn normalize_formats(val: &Value) -> Vec<String> {
    match val {
        Value::Null => vec![],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.trim().to_lowercase()))
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = val.as_str().unwrap_or(&val.to_string()).to_lowercase();
            s.replace(';', ",")
                .split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        }
    }
}

pub fn has_any_format(formats_val: &Value, targets: &std::collections::BTreeMap<String, ()>) -> bool {
    let fmts = normalize_formats(formats_val);
    if fmts.is_empty() {
        return false;
    }
    fmts.iter().any(|f| targets.contains_key(f))
}

pub fn is_english_or_missing(
    langs: &[String],
    include_missing_language: bool,
    english_codes: &[String],
) -> bool {
    if langs.is_empty() {
        return include_missing_language;
    }
    for x in langs {
        let x2 = x.replace('_', "-").to_lowercase();
        if english_codes.iter().any(|c| c == &x2) {
            return true;
        }
        if x2.starts_with("en-") {
            return true;
        }
        if x2 == "english" {
            return true;
        }
    }
    false
}

pub fn metadata_snapshot(book: &Value) -> Snapshot {
    let identifiers = normalize_identifiers(book.get("identifiers").unwrap_or(&Value::Null));
    let langs = normalize_languages(book.get("languages").unwrap_or(&Value::Null));

    let authors_val = book.get("authors").unwrap_or(&Value::Null);
    let authors = match authors_val {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = authors_val.as_str().unwrap_or("").trim();
            if s.is_empty() {
                vec![]
            } else {
                vec![s.to_string()]
            }
        }
    };

    let tags_val = book.get("tags").unwrap_or(&Value::Null);
    let tags = match tags_val {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            let s = tags_val.as_str().unwrap_or("").trim();
            if s.is_empty() {
                vec![]
            } else {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            }
        }
    };

    Snapshot {
        title: book
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        authors,
        publisher: book
            .get("publisher")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        pubdate: book
            .get("pubdate")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        languages: langs,
        isbn: book
            .get("isbn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string(),
        identifiers,
        tags,
        comments_present: book
            .get("comments")
            .and_then(|v| v.as_str())
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false),
        cover_present: book.get("cover").is_some() && !book.get("cover").unwrap().is_null(),
    }
}

pub fn snapshot_hash(snap: &Snapshot) -> Result<String> {
    let value = serde_json::to_value(snap)?;
    let stable = stable_json_string(&value)?;
    Ok(sha256_text(&stable))
}

pub fn score_good_enough(
    snap: &Snapshot,
    scoring: &crate::config::ScoringConfig,
) -> (i32, Vec<String>) {
    let mut score = 0;
    let mut reasons = Vec::new();

    if !snap.title.is_empty() {
        score += scoring.title_weight;
    } else {
        reasons.push("missing title".to_string());
    }
    if !snap.authors.is_empty() {
        score += scoring.authors_weight;
    } else {
        reasons.push("missing authors".to_string());
    }
    if !snap.publisher.is_empty() {
        score += scoring.publisher_weight;
    } else {
        reasons.push("missing publisher".to_string());
    }
    if !snap.pubdate.is_empty() {
        score += scoring.pubdate_weight;
    } else {
        reasons.push("missing pubdate".to_string());
    }

    if !snap.isbn.is_empty() {
        score += scoring.isbn_weight;
    } else if !snap.identifiers.is_empty() {
        score += scoring.identifiers_weight;
    } else {
        reasons.push("missing identifiers/isbn".to_string());
    }

    if !snap.tags.is_empty() {
        score += scoring.tags_weight;
    } else {
        reasons.push("missing tags".to_string());
    }

    if snap.comments_present {
        score += scoring.comments_weight;
    } else {
        reasons.push("missing description/comments".to_string());
    }

    if snap.cover_present {
        score += scoring.cover_weight;
    } else {
        reasons.push("missing cover".to_string());
    }

    (score, reasons)
}

pub fn normalize_languages_for_filter(val: &Value) -> Vec<String> {
    normalize_languages(val)
}

pub fn normalize_identifiers_for_fetch(val: &Value) -> HashMap<String, String> {
    normalize_identifiers(val)
}
