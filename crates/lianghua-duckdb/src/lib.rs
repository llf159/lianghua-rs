//! DuckDB API with process-wide instance reservations; cloned connections share a lease.
pub use duckdb::*;
use lianghua_model::resource::{DeviceMemory, MemoryLease};
use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

#[derive(Debug)]
pub struct Connection {
    // Drop the native connection before returning its reservation.
    inner: duckdb::Connection,
    budget: Arc<MemoryLease>,
}

impl Connection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_flags(path, Config::default())
    }
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_flags(Config::default())
    }
    pub fn open_in_memory_with_flags(config: Config) -> Result<Self> {
        Self::open_with_flags(":memory:", config)
    }
    pub fn open_with_flags(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        let device = DeviceMemory::current();
        Self::open_with_budget(
            path,
            config,
            (device.available / 16).min(8 << 30),
            device.cpus,
        )
    }
    /// Explicit stage settings participate in the same pool as default instances.
    pub fn open_with_budget(
        path: impl AsRef<Path>,
        config: Config,
        memory_bytes: u64,
        threads: usize,
    ) -> Result<Self> {
        let device = DeviceMemory::current();
        let budget = Arc::new(
            lianghua_model::resource::process_pool()
                .reserve(memory_bytes.min(device.working_bytes()), 16 << 20)
                .map_err(|error| {
                    Error::ToSqlConversionFailure(std::io::Error::other(error).into())
                })?,
        );
        let threads = threads.clamp(1, device.cpus);
        let config = config
            .max_memory(&format!("{}B", budget.bytes))?
            .threads(threads as i64)?;
        if std::env::var("LIANGHUA_RESOURCE_PROFILE").as_deref() == Ok("1") {
            eprintln!(
                "数据库资源：path={} 缓存={}MiB 线程={}",
                path.as_ref().display(),
                budget.bytes >> 20,
                threads
            );
        }
        let inner = duckdb::Connection::open_with_flags(path, config)?;
        Ok(Self { inner, budget })
    }
    pub fn try_clone(&self) -> Result<Self> {
        Ok(Self {
            inner: self.inner.try_clone()?,
            budget: self.budget.clone(),
        })
    }
    pub fn memory_budget(&self) -> u64 {
        self.budget.bytes
    }
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        Transaction::new(self)
    }
    pub fn unchecked_transaction(&self) -> Result<Transaction<'_>> {
        Transaction::new_unchecked(self)
    }
}
impl Deref for Connection {
    type Target = duckdb::Connection;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    inner: duckdb::Transaction<'a>,
    connection: &'a Connection,
}
impl<'a> Transaction<'a> {
    pub fn new(connection: &'a mut Connection) -> Result<Self> {
        Self::new_unchecked(connection)
    }
    pub fn new_unchecked(connection: &'a Connection) -> Result<Self> {
        Ok(Self {
            inner: duckdb::Transaction::new_unchecked(&connection.inner)?,
            connection,
        })
    }
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
    pub fn rollback(self) -> Result<()> {
        self.inner.rollback()
    }
    pub fn finish(self) -> Result<()> {
        self.inner.finish()
    }
    pub fn drop_behavior(&self) -> DropBehavior {
        self.inner.drop_behavior()
    }
    pub fn set_drop_behavior(&mut self, behavior: DropBehavior) {
        self.inner.set_drop_behavior(behavior);
    }
}
impl Deref for Transaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.connection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clone_keeps_instance_budget_and_transactions_keep_atomicity() {
        let mut connection = Connection::open_in_memory().unwrap();
        let cloned = connection.try_clone().unwrap();
        assert!(Arc::ptr_eq(&connection.budget, &cloned.budget));
        connection
            .execute_batch("CREATE TABLE t(i INTEGER)")
            .unwrap();
        {
            let transaction = connection.transaction().unwrap();
            transaction.execute("INSERT INTO t VALUES (1)", []).unwrap();
        }
        assert_eq!(
            cloned
                .query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let transaction = connection.transaction().unwrap();
        transaction.execute("INSERT INTO t VALUES (2)", []).unwrap();
        transaction.commit().unwrap();
        drop(connection);
        assert_eq!(
            cloned
                .query_row("SELECT i FROM t", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}
