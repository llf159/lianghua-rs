//! Adaptive sizing for memory-intensive work; limits never act as task admission credits.
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
    /// Hot-path sizing decisions reuse a short-lived sample.
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
                .map(|plan| plan.bytes)
        });
        Self {
            bytes: task_bytes
                .unwrap_or_else(|| snapshot.working_bytes())
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
}

/// Cancellation contexts already propagate through compute/download pools.
/// Keep a sizing plan in that same context without reserving memory.
pub struct TaskMemory {
    control: Arc<crate::task::TaskControl>,
}
impl TaskMemory {
    pub fn enter(control: Arc<crate::task::TaskControl>, background: bool) -> Self {
        let snapshot = DeviceMemory::detect();
        let plan = ResourcePlan {
            bytes: snapshot.working_bytes() / if background { 4 } else { 1 },
            cpus: snapshot.cpus,
        };
        eprintln!("任务工作集计划={}MiB（不预扣内存）", plan.bytes >> 20);
        *control.memory.write().unwrap_or_else(|e| e.into_inner()) = Some(plan);
        Self { control }
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

/// Local queue backpressure follows the payload until its buffers are consumed or dropped.
#[derive(Debug)]
pub struct ByteBudget {
    capacity: u64,
    used: Mutex<u64>,
    released: Condvar,
}
impl ByteBudget {
    pub fn new(divisor: u64, maximum: u64) -> Arc<Self> {
        Arc::new(Self {
            // A tiny queue must still make progress. This is a backpressure
            // threshold, not an allocation or a minimum-memory admission test.
            capacity: (ResourcePlan::detect().bytes / divisor.max(1))
                .min(maximum)
                .max(1 << 20),
            used: Mutex::new(0),
            released: Condvar::new(),
        })
    }
    pub fn acquire(self: &Arc<Self>, bytes: u64) -> Result<Arc<BytePermit>, String> {
        // A single oversized stock takes exclusive credits; it must never wait
        // for a capacity that is mathematically impossible to obtain.
        let bytes = bytes.min(self.capacity);
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        while used.saturating_add(bytes) > self.capacity {
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
    entries: std::collections::HashMap<K, (V, u64)>,
    bytes: u64,
}
impl<K: Eq + std::hash::Hash + Clone, V> Default for MemoryCache<K, V> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            bytes: 0,
        }
    }
}
impl<K: Eq + std::hash::Hash + Clone, V> MemoryCache<K, V> {
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|(value, _)| value)
    }
    pub fn insert(&mut self, key: K, value: V, bytes: u64) {
        let bytes = bytes.max(256);
        if let Some((_, old_bytes)) = self.entries.remove(&key) {
            self.bytes -= old_bytes;
        }
        let maximum = (ResourcePlan::detect().bytes / 64).min(128 << 20);
        if bytes > maximum {
            return;
        }
        while self.bytes.saturating_add(bytes) > maximum {
            let Some(key) = self.entries.keys().next().cloned() else {
                break;
            };
            if let Some((_, old_bytes)) = self.entries.remove(&key) {
                self.bytes -= old_bytes;
            }
        }
        self.entries.insert(key, (value, bytes));
        self.bytes += bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_payload_is_exclusive_and_cancelled_wait_releases_nothing_twice() {
        let queue = Arc::new(ByteBudget {
            capacity: 8,
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
    }
    #[test]
    fn independent_queues_do_not_consume_task_headroom() {
        let control = Arc::new(crate::task::TaskControl::default());
        *control.memory.write().unwrap() = Some(ResourcePlan { bytes: 0, cpus: 1 });
        let _active = crate::task::ActiveTask::enter_isolated(vec![control]);
        let first = ByteBudget::new(32, 256 << 20);
        assert_eq!(first.capacity, 1 << 20);
        let _payload = first.acquire(first.capacity).unwrap();
        let second = ByteBudget::new(32, 256 << 20);
        let _other_payload = second.acquire(second.capacity).unwrap();
        assert_eq!(ResourcePlan::detect().bytes, 0);
    }
    #[test]
    fn cache_replacement_and_eviction_keep_byte_accounting() {
        let mut cache = MemoryCache::default();
        cache.insert("a", 1, 256);
        cache.insert("a", 2, 512);
        assert_eq!(cache.bytes, 512);
        assert_eq!(cache.get(&"a"), Some(&2));
        let maximum = (ResourcePlan::detect().bytes / 64).min(128 << 20);
        cache.insert("b", 3, maximum);
        assert_eq!(cache.bytes, maximum);
        assert!(cache.get(&"a").is_none());
        assert_eq!(cache.get(&"b"), Some(&3));
        cache.insert("b", 4, maximum + 1);
        assert_eq!(cache.bytes, 0);
        assert!(cache.get(&"b").is_none());
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
