use std::{fs, path::Path};

// Keep this file in place: unlinking a held lock would let another process lock
// a different inode while the current rebuild is still using its staging files.
pub(super) fn lock_rebuild_directory(source_dir: &Path) -> Result<Option<fs::File>, String> {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(source_dir.join(".cyq_chen.rebuild.lock"))
        .map_err(|e| format!("打开新筹码重建锁失败:{e}"))?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(error) => Err(format!("锁定新筹码重建目录失败:{error}")),
    }
}

// The caller must hold the directory lock throughout cleanup and any rebuild.
pub(super) fn remove_stale_rebuilds(source_dir: &Path) -> Result<usize, String> {
    let mut removed = 0;
    for entry in fs::read_dir(source_dir).map_err(|e| format!("读取新筹码临时目录失败:{e}"))?
    {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix(".cyq_chen.db.rebuild-") else {
            continue;
        };
        let Some(id) = rest
            .strip_suffix(".tmp.wal")
            .or_else(|| rest.strip_suffix(".tmp.tmp"))
            .or_else(|| rest.strip_suffix(".tmp"))
        else {
            continue;
        };
        let Some((pid, timestamp)) = id.split_once('-') else {
            continue;
        };
        if pid.is_empty()
            || timestamp.is_empty()
            || !pid.bytes().all(|b| b.is_ascii_digit())
            || !timestamp.bytes().all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let path = entry.path();
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        if kind.is_symlink() {
            continue;
        }
        let result = if kind.is_dir() && name.ends_with(".tmp.tmp") {
            fs::remove_dir_all(&path)
        } else if kind.is_file() {
            fs::remove_file(&path)
        } else {
            continue;
        };
        result.map_err(|e| format!("清理新筹码重建残留失败, path={}: {e}", path.display()))?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_is_locked_and_preserves_official_assets() {
        let dir = std::env::temp_dir().join(format!(
            "cyq-cleanup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        for name in [
            ".cyq_chen.db.rebuild-123-456.tmp",
            ".cyq_chen.db.rebuild-123-456.tmp.wal",
            "cyq_chen.db",
            "cyq_chen.db.wal",
            "chip_change_rule.toml",
            ".unrelated.tmp",
            ".cyq_chen.db.rebuild-backup.tmp",
        ] {
            fs::write(dir.join(name), "keep").unwrap();
        }
        let spill = dir.join(".cyq_chen.db.rebuild-123-456.tmp.tmp");
        fs::create_dir(&spill).unwrap();
        fs::write(spill.join("block"), "spill").unwrap();
        let lock = lock_rebuild_directory(&dir).unwrap().unwrap();
        assert!(lock_rebuild_directory(&dir).unwrap().is_none());
        assert_eq!(remove_stale_rebuilds(&dir).unwrap(), 3);
        assert!(dir.join("cyq_chen.db").exists());
        assert!(dir.join("cyq_chen.db.wal").exists());
        assert!(dir.join(".unrelated.tmp").exists());
        assert!(dir.join(".cyq_chen.db.rebuild-backup.tmp").exists());
        drop(lock);
        assert!(lock_rebuild_directory(&dir).unwrap().is_some());
        fs::remove_dir_all(dir).unwrap();
    }
}
