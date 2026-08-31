//! Stock-symbol normalization shared by application workflows.

pub fn normalize_ts_code(raw: &str) -> Option<String> {
    let trimmed = raw.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.contains('.') {
        return Some(trimmed);
    }

    let digits: String = trimmed.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() != 6 {
        return None;
    }

    let suffix = if digits.starts_with("30") || digits.starts_with("00") {
        ".SZ"
    } else if digits.starts_with("60") || digits.starts_with("68") {
        ".SH"
    } else {
        ".BJ"
    };

    Some(format!("{digits}{suffix}"))
}

pub fn canonical_ts_code(raw: &str) -> String {
    let normalized = raw.trim().to_ascii_uppercase();
    if normalized.contains('.') {
        return normalized;
    }

    if normalized.starts_with("30") || normalized.starts_with("00") {
        format!("{normalized}.SZ")
    } else if normalized.starts_with("60") || normalized.starts_with("68") {
        format!("{normalized}.SH")
    } else {
        format!("{normalized}.BJ")
    }
}
