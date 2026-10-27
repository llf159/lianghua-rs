use duckdb::Connection;

/// Scope the checkpoint threshold without changing transaction or durability settings.
pub struct BulkWriteSettings {
    checkpoint_threshold: String,
}

impl BulkWriteSettings {
    /// Run a multi-transaction write scope, checkpoint on success, and restore settings
    /// on both success and error. The action must close its transactions and appenders.
    pub fn run<T>(
        conn: &Connection,
        action: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let settings = Self::apply(conn)?;
        let result = action();
        settings.checkpoint_and_finish(conn, result)
    }

    pub fn apply(conn: &Connection) -> Result<Self, String> {
        let checkpoint_threshold = conn
            .query_row(
                "SELECT current_setting('checkpoint_threshold')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("读取批量写入配置失败: {e}"))?;
        conn.execute_batch("SET checkpoint_threshold = '256MB'")
            .map_err(|e| format!("设置批量写入检查点阈值失败: {e}"))?;
        Ok(Self {
            checkpoint_threshold,
        })
    }

    pub fn checkpoint_and_finish<T>(
        self,
        conn: &Connection,
        result: Result<T, String>,
    ) -> Result<T, String> {
        // Close all appenders/transactions first; include deferred checkpoint work in this operation.
        let result = result.and_then(|value| {
            conn.execute_batch("CHECKPOINT")
                .map_err(|e| format!("批量写入最终落盘失败: {e}"))?;
            Ok(value)
        });
        self.finish(conn, result)
    }

    pub fn finish<T>(self, conn: &Connection, result: Result<T, String>) -> Result<T, String> {
        let restored = conn
            .execute("SET checkpoint_threshold = ?", [self.checkpoint_threshold])
            .map_err(|e| format!("恢复批量写入检查点阈值失败: {e}"));
        match (result, restored) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(restore)) => Err(format!("{error}; {restore}")),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn restores_checkpoint_threshold_after_success_and_error() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("SET checkpoint_threshold = '32MB'")
            .unwrap();
        let previous: String = conn
            .query_row("SELECT current_setting('checkpoint_threshold')", [], |r| {
                r.get(0)
            })
            .unwrap();
        for result in [Ok(7), Err("interrupted".to_string())] {
            let expected = result.clone();
            let actual = super::BulkWriteSettings::run(&conn, || {
                let current: String = conn
                    .query_row("SELECT current_setting('checkpoint_threshold')", [], |r| {
                        r.get(0)
                    })
                    .unwrap();
                assert_ne!(current, previous);
                result
            });
            assert_eq!(actual, expected);
            let restored: String = conn
                .query_row("SELECT current_setting('checkpoint_threshold')", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(restored, previous);
        }
    }
}
