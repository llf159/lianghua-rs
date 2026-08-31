//! Shared ordering and threshold parsing for live scene stages.

pub(super) fn level(raw: Option<&str>) -> i32 {
    match raw
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("confirm") => 3,
        Some("trigger") => 2,
        Some("observe") => 1,
        Some("fail") => 0,
        _ => -1,
    }
}

pub(super) fn threshold(raw: Option<&str>) -> i32 {
    match raw
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("confirm") => 3,
        Some("observe") => 1,
        Some("fail") => 0,
        Some("trigger") | Some("") | None => 2,
        _ => 2,
    }
}
