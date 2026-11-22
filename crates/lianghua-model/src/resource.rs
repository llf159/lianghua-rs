//! Shared working-set reservations. Limits are planning ceilings, not an RSS allocator.
use std::sync::{Arc, Condvar, Mutex, OnceLock};

#[derive(Clone, Debug)]
pub struct DeviceMemory {
    pub total: u64,
    pub available: u64,
    pub rss: u64,
    pub cpus: usize,
    pub source: String,
}

impl DeviceMemory {
    /// Hot-path decisions reuse a short-lived sample; stage reservations sample afresh.
    pub fn current() -> Self {
        static SAMPLE: OnceLock<Mutex<Option<(std::time::Instant, DeviceMemory)>>> =
            OnceLock::new();
        let mut sample = SAMPLE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if sample
            .as_ref()
            .is_none_or(|(time, _)| time.elapsed() >= std::time::Duration::from_secs(1))
        {
            *sample = Some((std::time::Instant::now(), Self::detect()));
        }
        sample.as_ref().unwrap().1.clone()
    }
    pub fn detect() -> Self {
        let mut snapshot = Self {
            total: 2 << 30,
            available: 1 << 30,
            rss: 0,
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
            source: "fallback: platform memory unavailable".into(),
        };
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
                let value = |key: &str| {
                    text.lines()
                        .find_map(|line| {
                            line.strip_prefix(key)?
                                .split_whitespace()
                                .next()?
                                .parse::<u64>()
                                .ok()
                        })
                        .map(|kb| kb.saturating_mul(1024))
                };
                if let (Some(total), Some(available)) = (value("MemTotal:"), value("MemAvailable:"))
                {
                    snapshot.total = total;
                    snapshot.available = available;
                    snapshot.source = "MemAvailable".into();
                }
            }
            if let Ok(text) = std::fs::read_to_string("/proc/self/status") {
                snapshot.rss = text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("VmRSS:")?
                            .split_whitespace()
                            .next()?
                            .parse::<u64>()
                            .ok()
                    })
                    .unwrap_or(0)
                    .saturating_mul(1024);
            }
            // Resolve the process's mount and membership, including namespaced mounts.
            if let (Ok(groups), Ok(mounts)) = (
                std::fs::read_to_string("/proc/self/cgroup"),
                std::fs::read_to_string("/proc/self/mountinfo"),
            ) {
                for group in groups.lines() {
                    let mut fields = group.splitn(3, ':');
                    let _ = fields.next();
                    let controllers = fields.next().unwrap_or("");
                    let membership = fields.next().unwrap_or("/");
                    let v2 = controllers.is_empty();
                    if !v2 && !controllers.split(',').any(|c| c == "memory") {
                        continue;
                    }
                    for mount in mounts.lines() {
                        let Some((left, right)) = mount.split_once(" - ") else {
                            continue;
                        };
                        let fields: Vec<_> = left.split_whitespace().collect();
                        let fs: Vec<_> = right.split_whitespace().collect();
                        if fields.len() < 5
                            || fs.len() < 3
                            || (v2 && fs[0] != "cgroup2")
                            || (!v2
                                && (fs[0] != "cgroup" || !fs[2].split(',').any(|c| c == "memory")))
                        {
                            continue;
                        }
                        let root = std::path::Path::new(fields[3]);
                        let mountpoint = std::path::Path::new(fields[4]);
                        let relative = std::path::Path::new(membership)
                            .strip_prefix(root)
                            .unwrap_or(std::path::Path::new(""));
                        // Do not traverse outside the mounted hierarchy via /../ in proc output.
                        if relative
                            .components()
                            .any(|c| matches!(c, std::path::Component::ParentDir))
                        {
                            continue;
                        }
                        let mut path = mountpoint.join(relative);
                        loop {
                            let limit_name = if v2 {
                                "memory.max"
                            } else {
                                "memory.limit_in_bytes"
                            };
                            let usage_name = if v2 {
                                "memory.current"
                            } else {
                                "memory.usage_in_bytes"
                            };
                            if let (Ok(limit), Ok(usage)) = (
                                std::fs::read_to_string(path.join(limit_name)),
                                std::fs::read_to_string(path.join(usage_name)),
                            ) {
                                if let (Ok(limit), Ok(usage)) =
                                    (limit.trim().parse::<u64>(), usage.trim().parse::<u64>())
                                {
                                    snapshot.constrain(limit, usage, "cgroup");
                                }
                            }
                            if v2 {
                                if let (Ok(limit), Ok(usage)) = (
                                    std::fs::read_to_string(path.join("memory.high")),
                                    std::fs::read_to_string(path.join("memory.current")),
                                ) {
                                    if let (Ok(limit), Ok(usage)) =
                                        (limit.trim().parse::<u64>(), usage.trim().parse::<u64>())
                                    {
                                        snapshot.constrain(limit, usage, "cgroup.high");
                                    }
                                }
                                if let Ok(quota) = std::fs::read_to_string(path.join("cpu.max")) {
                                    let values: Vec<_> = quota.split_whitespace().collect();
                                    if values.len() == 2 {
                                        if let (Ok(quota), Ok(period)) =
                                            (values[0].parse::<u64>(), values[1].parse::<u64>())
                                        {
                                            if period > 0 {
                                                snapshot.cpus = snapshot
                                                    .cpus
                                                    .min(quota.div_ceil(period).max(1) as usize);
                                            }
                                        }
                                    }
                                }
                            }
                            if path == mountpoint || !path.pop() {
                                break;
                            }
                        }
                    }
                }
            }
        }
        #[cfg(target_os = "windows")]
        unsafe {
            use windows_sys::Win32::System::{
                JobObjects::*, ProcessStatus::*, SystemInformation::*, Threading::GetCurrentProcess,
            };
            let mut status = MEMORYSTATUSEX::default();
            status.dwLength = std::mem::size_of_val(&status) as u32;
            if GlobalMemoryStatusEx(&mut status) != 0 {
                snapshot.total = status.ullTotalPhys;
                snapshot.available = status.ullAvailPhys;
                snapshot.source = "GlobalMemoryStatusEx".into();
            }
            let mut process = PROCESS_MEMORY_COUNTERS::default();
            process.cb = std::mem::size_of_val(&process) as u32;
            if K32GetProcessMemoryInfo(GetCurrentProcess(), &mut process, process.cb) != 0 {
                snapshot.rss = process.WorkingSetSize as u64;
            }
            let mut job = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
            if QueryInformationJobObject(
                std::ptr::null_mut(),
                JobObjectExtendedLimitInformation,
                (&mut job as *mut JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&job) as u32,
                std::ptr::null_mut(),
            ) != 0
            {
                if job.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_PROCESS_MEMORY != 0 {
                    snapshot.constrain(
                        job.ProcessMemoryLimit as u64,
                        process.PagefileUsage as u64,
                        "Job process limit",
                    );
                }
                if job.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_JOB_MEMORY != 0 {
                    let mut usage = JOBOBJECT_LIMIT_VIOLATION_INFORMATION::default();
                    let used = if QueryInformationJobObject(
                        std::ptr::null_mut(),
                        JobObjectLimitViolationInformation,
                        (&mut usage as *mut JOBOBJECT_LIMIT_VIOLATION_INFORMATION).cast(),
                        std::mem::size_of_val(&usage) as u32,
                        std::ptr::null_mut(),
                    ) != 0
                    {
                        usage.JobMemory
                    } else {
                        job.PeakJobMemoryUsed as u64
                    };
                    snapshot.constrain(job.JobMemoryLimit as u64, used, "Job memory limit");
                }
            }
        }

        snapshot.available = snapshot.available.min(snapshot.total);
        snapshot
    }

    pub fn constrain(&mut self, limit: u64, used: u64, source: &str) {
        if limit < self.total || limit.saturating_sub(used) < self.available {
            self.total = self.total.min(limit);
            self.available = self.available.min(limit.saturating_sub(used));
            self.source.push_str(&format!("+{source}"));
        }
    }

    pub fn working_bytes(&self) -> u64 {
        // Leave 35% for WebView, native allocations not tracked by DuckDB, and the OS.
        self.available.saturating_mul(65) / 100
    }
}

#[derive(Debug)]
pub struct MemoryPool {
    limit: u64,
    used: Mutex<u64>,
}

impl MemoryPool {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Mutex::new(0),
        })
    }
    pub fn reserve(self: &Arc<Self>, wanted: u64, minimum: u64) -> Result<MemoryLease, String> {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        let bytes = wanted.min(self.limit.saturating_sub(*used));
        if bytes < minimum {
            return Err(format!(
                "内存预算不足：需要至少 {} MiB，剩余 {} MiB；请结束其他重任务后重试",
                minimum >> 20,
                bytes >> 20
            ));
        }
        *used += bytes;
        Ok(MemoryLease {
            pool: self.clone(),
            bytes,
        })
    }
    pub fn remaining(&self) -> u64 {
        self.limit
            .saturating_sub(*self.used.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

#[derive(Debug)]
pub struct MemoryLease {
    pool: Arc<MemoryPool>,
    pub bytes: u64,
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        *self.pool.used.lock().unwrap_or_else(|e| e.into_inner()) -= self.bytes;
    }
}

pub fn process_pool() -> &'static Arc<MemoryPool> {
    static POOL: OnceLock<Arc<MemoryPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let snapshot = DeviceMemory::detect();
        eprintln!(
            "资源预算：来源={} 可用={}MiB RSS={}MiB 工作集={}MiB CPU={}",
            snapshot.source,
            snapshot.available >> 20,
            snapshot.rss >> 20,
            snapshot.working_bytes() >> 20,
            snapshot.cpus
        );
        MemoryPool::new(snapshot.working_bytes())
    })
}

/// Reserve fresh headroom only; existing RSS is already excluded by MemAvailable.
pub fn reserve(divisor: u64, maximum: u64, minimum: u64) -> Result<MemoryLease, String> {
    let snapshot = DeviceMemory::detect();
    process_pool().reserve(
        (snapshot.working_bytes() / divisor.max(1)).min(maximum),
        minimum,
    )
}

#[derive(Clone, Copy, Debug)]
pub struct ResourcePlan {
    pub bytes: u64,
    pub cpus: usize,
}
impl ResourcePlan {
    pub fn detect() -> Self {
        let snapshot = DeviceMemory::current();
        let task_bytes = crate::task::current_controls().iter().find_map(|control| {
            control
                .memory
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .map(|lease| lease.bytes)
        });
        Self {
            bytes: task_bytes
                .unwrap_or_else(|| snapshot.working_bytes().min(process_pool().remaining()))
                .min(snapshot.working_bytes()),
            cpus: snapshot.cpus,
        }
    }
    pub fn batch(&self, bytes_per_item: usize, maximum: usize) -> usize {
        ((self.bytes / 32) as usize / bytes_per_item.max(1)).clamp(1, maximum.max(1))
    }
    pub fn workers(&self, requested: usize, bytes_per_worker: usize) -> usize {
        let effective = requested
            .max(1)
            .min(self.cpus)
            .min((self.bytes as usize / bytes_per_worker.max(1)).max(1));
        if effective < requested {
            eprintln!(
                "计算并发调整：请求={requested} 有效={effective} CPU={} 工作集={}MiB 单工作项估算={}MiB",
                self.cpus,
                self.bytes >> 20,
                bytes_per_worker >> 20
            );
        }
        effective
    }
    pub fn network_workers(&self, requested: usize) -> usize {
        let effective = requested
            .max(1)
            .min((self.bytes as usize / (16 << 20)).max(1));
        if effective < requested {
            eprintln!(
                "下载并发调整：请求={requested} 有效={effective} 工作集={}MiB",
                self.bytes >> 20
            );
        }
        effective
    }
}

/// Cancellation contexts already propagate through compute/download pools.
/// Keep their working-set lease in that same context, and return it on unwind.
pub struct TaskMemory {
    control: Arc<crate::task::TaskControl>,
}
impl TaskMemory {
    pub fn enter(control: Arc<crate::task::TaskControl>, background: bool) -> Result<Self, String> {
        let lease = Arc::new(reserve(if background { 16 } else { 4 }, 8 << 30, 16 << 20)?);
        eprintln!("任务工作集预算={}MiB", lease.bytes >> 20);
        *control.memory.write().unwrap_or_else(|e| e.into_inner()) = Some(lease);
        Ok(Self { control })
    }
}
impl Drop for TaskMemory {
    fn drop(&mut self) {
        self.control
            .memory
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }
}

/// Byte credits stay with the payload until its buffers are consumed or dropped.
#[derive(Debug)]
pub struct ByteBudget {
    lease: MemoryLease,
    used: Mutex<u64>,
    released: Condvar,
}
impl ByteBudget {
    pub fn new(divisor: u64, maximum: u64) -> Result<Arc<Self>, String> {
        Ok(Arc::new(Self {
            lease: reserve(divisor, maximum, 1 << 20)?,
            used: Mutex::new(0),
            released: Condvar::new(),
        }))
    }
    pub fn acquire(self: &Arc<Self>, bytes: u64) -> Result<Arc<BytePermit>, String> {
        // A single oversized stock takes exclusive credits; it must never wait
        // for a capacity that is mathematically impossible to obtain.
        let bytes = bytes.min(self.lease.bytes);
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        while used.saturating_add(bytes) > self.lease.bytes {
            crate::task::checkpoint()?;
            used = self
                .released
                .wait_timeout(used, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *used += bytes;
        Ok(Arc::new(BytePermit {
            budget: self.clone(),
            bytes,
        }))
    }
}
#[derive(Debug)]
pub struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: u64,
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        *self.budget.used.lock().unwrap_or_else(|e| e.into_inner()) -= self.bytes;
        self.budget.released.notify_all();
    }
}

/// Recomputable cache. Revisions remain part of the caller's key.
pub struct MemoryCache<K, V> {
    entries: std::collections::HashMap<K, (V, MemoryLease)>,
    pressure_checked: std::time::Instant,
    pressure_reads: u8,
}
impl<K: Eq + std::hash::Hash + Clone, V> Default for MemoryCache<K, V> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            pressure_checked: std::time::Instant::now(),
            pressure_reads: 0,
        }
    }
}
impl<K: Eq + std::hash::Hash + Clone, V> MemoryCache<K, V> {
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.pressure_reads = self.pressure_reads.wrapping_add(1);
        if self.pressure_reads == 0
            && self.pressure_checked.elapsed() >= std::time::Duration::from_secs(1)
        {
            let snapshot = DeviceMemory::current();
            if snapshot.available < snapshot.total / 16 {
                self.entries.clear();
            }
            self.pressure_checked = std::time::Instant::now();
        }
        self.entries.get(key).map(|(value, _)| value)
    }
    pub fn insert(&mut self, key: K, value: V, bytes: u64) {
        let bytes = bytes.max(256);
        self.entries.remove(&key);
        let maximum = (ResourcePlan::detect().bytes / 64).min(128 << 20);
        if bytes > maximum {
            return;
        }
        while self
            .entries
            .values()
            .map(|(_, lease)| lease.bytes)
            .sum::<u64>()
            + bytes
            > maximum
        {
            let Some(key) = self.entries.keys().next().cloned() else {
                break;
            };
            self.entries.remove(&key);
        }
        if let Ok(lease) = process_pool().reserve(bytes, bytes) {
            self.entries.insert(key, (value, lease));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_payload_is_exclusive_and_cancelled_wait_releases_nothing_twice() {
        let pool = MemoryPool::new(8);
        let queue = Arc::new(ByteBudget {
            lease: pool.reserve(8, 8).unwrap(),
            used: Mutex::new(0),
            released: Condvar::new(),
        });
        let oversized = queue.acquire(16).unwrap();
        assert_eq!(*queue.used.lock().unwrap(), 8);
        let control = Arc::new(crate::task::TaskControl::default());
        control.request().unwrap();
        let _active = crate::task::ActiveTask::enter_isolated(vec![control]);
        assert!(queue.acquire(1).is_err());
        drop(oversized);
        let next = queue.acquire(4).unwrap();
        assert_eq!(*queue.used.lock().unwrap(), 4);
        drop(next);
        assert_eq!(*queue.used.lock().unwrap(), 0);
        drop(queue);
        assert_eq!(pool.remaining(), 8);
    }
    #[test]
    fn concurrent_reservations_release_on_error_and_last_owner() {
        let pool = MemoryPool::new(100);
        let first = Arc::new(pool.reserve(70, 10).unwrap());
        let last = first.clone();
        assert_eq!(pool.reserve(70, 10).unwrap().bytes, 30);
        assert!(pool.reserve(100, 40).is_err());
        drop(first);
        assert_eq!(pool.remaining(), 30);
        drop(last);
        assert_eq!(pool.remaining(), 100);
    }
    #[test]
    fn constrained_device_never_gains_a_minimum_floor() {
        let mut device = DeviceMemory {
            total: 8 << 30,
            available: 4 << 30,
            rss: 1 << 30,
            cpus: 8,
            source: "test".into(),
        };
        device.constrain(1 << 30, (1 << 30) - (32 << 20), "test-cgroup");
        assert!(device.working_bytes() < 32 << 20);
        device.constrain(1 << 30, 2 << 30, "ancestor");
        assert_eq!(device.working_bytes(), 0);
        let small = ResourcePlan {
            bytes: 64 << 20,
            cpus: 8,
        };
        let large = ResourcePlan {
            bytes: 8 << 30,
            cpus: 8,
        };
        assert!(small.batch(1 << 20, 128) < large.batch(1 << 20, 128));
        assert!(small.workers(8, 64 << 20) < large.workers(8, 64 << 20));
    }
}
