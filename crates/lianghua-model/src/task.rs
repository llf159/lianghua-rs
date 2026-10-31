use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TaskState {
    Running,
    Stop,
    Rollback,
    Finishing,
}

pub struct TaskControl(AtomicU8);

impl Default for TaskControl {
    fn default() -> Self {
        Self(AtomicU8::new(TaskState::Running as u8))
    }
}

impl TaskControl {
    pub fn request(&self, rollback: bool) -> Result<(), String> {
        self.0
            .compare_exchange(
                TaskState::Running as u8,
                if rollback {
                    TaskState::Rollback
                } else {
                    TaskState::Stop
                } as u8,
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
            2 => TaskState::Rollback,
            _ => TaskState::Finishing,
        }
    }
    pub fn seal(&self) -> TaskState {
        match self.0.swap(TaskState::Finishing as u8, Ordering::SeqCst) {
            0 => TaskState::Running,
            1 => TaskState::Stop,
            2 => TaskState::Rollback,
            _ => TaskState::Finishing,
        }
    }
    pub fn check(&self) -> Result<(), String> {
        match self.state() {
            TaskState::Stop => Err("任务已停止，保留已提交结果".into()),
            TaskState::Rollback => Err("任务已请求撤回".into()),
            _ => Ok(()),
        }
    }
}

// 可写任务由应用统一串行调度；工作线程共享当前父任务和阶段的停止标记。
static ACTIVE: RwLock<Vec<Arc<TaskControl>>> = RwLock::new(Vec::new());

pub struct ActiveTask;
impl ActiveTask {
    pub fn enter(controls: Vec<Arc<TaskControl>>) -> Self {
        *ACTIVE.write().unwrap_or_else(|e| e.into_inner()) = controls;
        Self
    }
}
impl Drop for ActiveTask {
    fn drop(&mut self) {
        ACTIVE.write().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

pub fn checkpoint() -> Result<(), String> {
    for control in ACTIVE.read().unwrap_or_else(|e| e.into_inner()).iter() {
        control.check()?;
    }
    Ok(())
}
