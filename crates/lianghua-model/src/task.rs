use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TaskState {
    Running,
    Stop,
    Finishing,
}

pub struct TaskControl(AtomicU8);

impl Default for TaskControl {
    fn default() -> Self {
        Self(AtomicU8::new(TaskState::Running as u8))
    }
}

impl TaskControl {
    pub fn request(&self) -> Result<(), String> {
        self.0
            .compare_exchange(
                TaskState::Running as u8,
                TaskState::Stop as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|_| "任务已停止或正在结束，不能重复操作".into())
    }
    pub fn state(&self) -> TaskState {
        match self.0.load(Ordering::SeqCst) {
            0 => TaskState::Running,
            1 => TaskState::Stop,
            _ => TaskState::Finishing,
        }
    }
    pub fn seal(&self) -> TaskState {
        match self.0.swap(TaskState::Finishing as u8, Ordering::SeqCst) {
            0 => TaskState::Running,
            1 => TaskState::Stop,
            _ => TaskState::Finishing,
        }
    }
    pub fn check(&self) -> Result<(), String> {
        match self.state() {
            TaskState::Stop => Err("任务已停止，保留已提交结果".into()),
            TaskState::Finishing => Err("任务已结束".into()),
            TaskState::Running => Ok(()),
        }
    }
}

// 原有串行计算线程共享停止标记；独立下载及其线程池通过局部上下文覆盖。
static ACTIVE: RwLock<Vec<Arc<TaskControl>>> = RwLock::new(Vec::new());

thread_local! {
    static LOCAL_CONTROLS: std::cell::RefCell<Option<Vec<Arc<TaskControl>>>> = const { std::cell::RefCell::new(None) };
}

pub fn current_controls() -> Vec<Arc<TaskControl>> {
    LOCAL_CONTROLS
        .with(|local| local.borrow().clone())
        .unwrap_or_else(|| ACTIVE.read().unwrap_or_else(|e| e.into_inner()).clone())
}

pub struct ActiveTask {
    local: bool,
    previous: Option<Vec<Arc<TaskControl>>>,
}
impl ActiveTask {
    pub fn enter(controls: Vec<Arc<TaskControl>>) -> Self {
        *ACTIVE.write().unwrap_or_else(|e| e.into_inner()) = controls;
        Self {
            local: false,
            previous: None,
        }
    }
    pub fn enter_isolated(controls: Vec<Arc<TaskControl>>) -> Self {
        Self {
            local: true,
            previous: LOCAL_CONTROLS.with(|local| local.replace(Some(controls))),
        }
    }
}
impl Drop for ActiveTask {
    fn drop(&mut self) {
        if self.local {
            LOCAL_CONTROLS.with(|local| local.replace(self.previous.take()));
        } else {
            ACTIVE.write().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

pub fn checkpoint() -> Result<(), String> {
    LOCAL_CONTROLS.with(|local| {
        let local = local.borrow();
        if let Some(controls) = local.as_ref() {
            for control in controls {
                control.check()?;
            }
        } else {
            for control in ACTIVE.read().unwrap_or_else(|e| e.into_inner()).iter() {
                control.check()?;
            }
        }
        Ok(())
    })
}

pub fn wait(duration: std::time::Duration) -> Result<(), String> {
    let deadline = std::time::Instant::now() + duration;
    loop {
        checkpoint()?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        std::thread::sleep(remaining.min(std::time::Duration::from_millis(100)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stopping_stage_or_parent_preserves_other_controls_and_rejects_late_requests() {
        let parent = Arc::new(TaskControl::default());
        let stage = Arc::new(TaskControl::default());
        let active = ActiveTask::enter(vec![parent.clone(), stage.clone()]);
        assert!(checkpoint().is_ok());
        let independent = Arc::new(TaskControl::default());
        let other = independent.clone();
        std::thread::spawn(move || {
            let _active = ActiveTask::enter_isolated(vec![other.clone()]);
            assert!(checkpoint().is_ok());
            let inherited = current_controls();
            other.request().unwrap();
            std::thread::spawn(move || {
                let _active = ActiveTask::enter_isolated(inherited);
                assert!(checkpoint().is_err());
            })
            .join()
            .unwrap();
            assert!(checkpoint().is_err());
        })
        .join()
        .unwrap();
        assert!(checkpoint().is_ok());
        stage.request().unwrap();
        assert!(checkpoint().is_err());
        assert!(parent.check().is_ok());
        {
            let _isolated = ActiveTask::enter_isolated(vec![Arc::new(TaskControl::default())]);
            assert!(checkpoint().is_ok());
        }
        assert!(stage.seal() == TaskState::Stop);
        assert!(stage.request().is_err());
        drop(active);
        assert!(checkpoint().is_ok());

        let next = Arc::new(TaskControl::default());
        let active = ActiveTask::enter(vec![parent.clone(), next.clone()]);
        parent.request().unwrap();
        assert!(checkpoint().is_err());
        assert!(next.check().is_ok());
        drop(active);
        assert!(checkpoint().is_ok());
    }
}
