//! Named dispatch leases a generation; timers only observe their original owner.
use super::*;
use bingo_plugin_host::callback::Executor;

pub(super) struct Resolver(pub Weak<Manager>);
#[async_trait]
impl bingo_plugin_host::callback::Resolver for Resolver {
    async fn acquire(&self, plugin: &str) -> Result<Arc<dyn Executor>, String> {
        let manager = self.0.upgrade().ok_or("module manager is unavailable")?;
        // Host calls carry invocation ancestry across the session mailbox. Waiting
        // here would deadlock a session.start callback inside the active reload.
        if generation::current(plugin).is_some()
            || !bingo_plugin_host::invocation::current().is_empty()
        {
            return acquire(&manager, plugin).await;
        }
        manager.prepare().await?;
        let _guard = manager.reloading.lock().await;
        acquire(&manager, plugin).await
    }
}
pub(super) async fn prepare(manager: &Manager) -> Result<(), String> {
    manager.poll().await;
    let Some(owner) = manager.this.get().and_then(Weak::upgrade) else {
        return Err("module manager is unavailable".into());
    };
    run(manager, async move { owner.reload_stale().await })
        .await
        .map_err(|error| error.message)
}
pub(super) async fn run<T: Send + 'static>(
    manager: &Manager,
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> Result<T, KernelError> {
    let (finished, waiting) = tokio::sync::oneshot::channel();
    let work = tokio::spawn(bingo_sdk::invocation::carry(async move {
        let _ = finished.send(future.await);
    }));
    {
        let mut tasks = manager
            .tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| !task.is_finished());
        tasks.push(work);
    }
    // Cancelling a caller drops only its wait, not a half-published reload.
    waiting.await.map_err(reload::failed)
}

async fn acquire(manager: &Manager, plugin: &str) -> Result<Arc<dyn Executor>, String> {
    switching::running(manager).map_err(|error| error.message)?;
    let generation = generation::current(plugin);
    let eligible = |module: &&Arc<Module>| {
        module.found.name == plugin
            && generation
                .as_ref()
                .is_none_or(|id| &module.generation == id)
            && module.admission.allowed()
            && module
                .engine
                .as_ref()
                .is_some_and(|engine| !engine.stopped().is_cancelled())
    };
    let mut module = manager
        .modules
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .rev()
        .find(eligible)
        .cloned();
    if module.is_none() && generation.is_some() {
        module = manager
            .switches
            .retired
            .lock()
            .await
            .iter()
            .find(eligible)
            .cloned();
    }
    let module = module.ok_or_else(|| format!("{plugin}: callback runtime is unavailable"))?;
    Ok(Arc::new(crate::dollar::callback::Native {
        generation: module.generation.clone(),
        module: crate::dollar::callback::Owner::Acquired(module),
        manager: manager.this.get().cloned().unwrap_or_default(),
    }))
}
