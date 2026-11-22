use rayon::{ThreadPool, ThreadPoolBuilder};

/// Keep outer stock parallelism within the task's resident working-set estimate.
pub fn compute_pool(bytes_per_worker: usize) -> Result<ThreadPool, String> {
    let plan = lianghua_model::resource::ResourcePlan::detect();
    let controls = lianghua_model::task::current_controls();
    ThreadPoolBuilder::new()
        .num_threads(plan.workers(plan.cpus, bytes_per_worker))
        .spawn_handler(move |worker| {
            let controls = controls.clone();
            std::thread::Builder::new().spawn(move || {
                let _active = lianghua_model::task::ActiveTask::enter_isolated(controls);
                worker.run();
            })?;
            Ok(())
        })
        .build()
        .map_err(|error| format!("创建资源自适应计算线程池失败: {error}"))
}
