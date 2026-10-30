use std::{fmt::Display, thread, time::Duration};

/// 只用于可重放的数据库读取。每次调用必须丢弃本次未完成结果；流式回调须自行保留交付边界并拦截回调错误。
/// 不能重放写入、计算或事务提交。
/// 沿用原连接，保留临时表、共享实例及调用方事务的快照；不自动重连或回滚事务。
pub fn read_with_retry<T, E: Display>(
    context: &str,
    mut read: impl FnMut() -> Result<T, E>,
) -> Result<T, E> {
    let mut retries = 0;
    loop {
        match read() {
            Ok(value) => return Ok(value),
            Err(error) => {
                let retryable = is_retryable_read_error(&error.to_string());
                if !retryable || retries == 3 {
                    if retries > 0 {
                        eprintln!("{context}: 自动重试 {retries} 次后读取仍失败: {error}");
                    }
                    return Err(error);
                }
                let delay = [250, 500, 1_000][retries];
                retries += 1;
                eprintln!("{context}: 读取失败，{delay}ms 后自动重试 {retries}/3: {error}");
                thread::sleep(Duration::from_millis(delay));
            }
        }
    }
}

pub(super) fn is_retryable_read_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "io error:",
        "i/o error",
        "input/output error",
        "computed checksum",
    ]
    .iter()
    .any(|text| message.contains(text))
        && ![
            "permission denied",
            "no such file or directory",
            "no space left on device",
            "read-only file system",
        ]
        .iter()
        .any(|text| message.contains(text))
}

#[cfg(test)]
mod tests {
    #[test]
    fn retries_io_reads_with_a_limit_and_preserves_other_errors() {
        for (error, failures, expected_calls) in [
            (
                "IO Error: computed checksum does not match stored checksum",
                1,
                2,
            ),
            ("IO Error: Input/output error", 9, 4),
            ("Binder Error: missing column", 9, 1),
            ("IO Error: Permission denied", 9, 1),
        ] {
            let mut calls = 0;
            let result = super::read_with_retry("test read", || {
                calls += 1;
                if calls <= failures {
                    Err(error)
                } else {
                    Ok(42)
                }
            });
            assert_eq!(calls, expected_calls);
            assert_eq!(result, if failures == 1 { Ok(42) } else { Err(error) });
        }
    }
}
