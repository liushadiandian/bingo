//! Retiring a generation removes future dispatch without closing a turn's links.
use super::*;
use bingo_sdk::plugin::PluginSwitch;

#[derive(Default)]
pub(super) struct State {
    pub off: RwLock<BTreeSet<String>>,
    pub retired: tokio::sync::Mutex<Vec<Arc<Module>>>,
    pub requests: Mutex<BTreeMap<String, Arc<Request>>>,
    pub changed: tokio::sync::Notify,
}
pub(super) struct Request {
    pub enabled: bool,
}
pub(super) fn begin(manager: &Manager, name: &str, enabled: bool) -> Arc<Request> {
    let request = Arc::new(Request { enabled });
    manager
        .switches
        .requests
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(name.into(), request.clone());
    manager.switches.changed.notify_waiters();
    request
}
fn current(manager: &Manager, name: &str, request: &Arc<Request>) -> bool {
    manager
        .switches
        .requests
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(name)
        .is_some_and(|latest| Arc::ptr_eq(latest, request))
}
pub(super) fn finish(manager: &Manager, name: &str, request: &Arc<Request>) {
    let mut requests = manager
        .switches
        .requests
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if requests
        .get(name)
        .is_some_and(|latest| Arc::ptr_eq(latest, request))
    {
        requests.remove(name);
    }
}
pub(super) fn running(manager: &Manager) -> Result<(), KernelError> {
    if manager.stop.is_cancelled() {
        Err(reload::failed("module manager stopped"))
    } else {
        Ok(())
    }
}
pub(super) fn module_kind(manager: &Manager, name: &str) -> PluginSwitch {
    manager
        .modules()
        .iter()
        .find(|module| {
            module
                .admission
                .status(&module.found, PluginRuntime::Module, "bingo.mods")
                .id
                == name
        })
        .map_or_else(
            || next("module unavailable"),
            |module| classify(manager, module),
        )
}
fn classify(manager: &Manager, module: &Arc<Module>) -> PluginSwitch {
    if manager
        .dollar
        .get()
        .is_some_and(|dollar| dollar.has_tools(&module.found.name))
        || module.found.manifest.uses.iter().any(|name| name == "tool")
        || module
            .admission
            .decision()
            .ok()
            .flatten()
            .is_some_and(|grant| grant.uses.contains("tool"))
    {
        return next("tools");
    }
    let matcher = crate::link::Link(module.clone()).matcher();
    if let Some(entry) = catalogue::ALL.iter().find(|entry| {
        entry.shapes_request
            && matcher
                .patterns
                .iter()
                .any(|pattern| pattern.matches(&entry.name))
    }) {
        return next(&entry.name);
    }
    PluginSwitch::Now
}
pub(super) fn wish(manager: &Manager, name: &str, enabled: bool) {
    let mut off = manager
        .switches
        .off
        .write()
        .unwrap_or_else(|e| e.into_inner());
    if enabled {
        off.remove(name);
    } else {
        off.insert(name.into());
    }
}
pub(super) async fn switch_module(
    manager: &Manager,
    name: &str,
    request: &Arc<Request>,
    deferred: tokio::sync::oneshot::Sender<()>,
) -> Result<PluginSwitch, KernelError> {
    let enabled = request.enabled;
    if enabled {
        tokio::select! {
            biased;
            _ = manager.stop.cancelled() => return Err(reload::failed("module manager stopped")),
            ready = defer_retirement(manager, name, request, deferred) => if !ready {
                let why = "running turn still holds the previous generation";
                manager.notices.push(Notice::warn("PLUGIN_SWITCH", format!("{name}: on at next start ({why})")));
                return Ok(next(why));
            }
        }
    }
    let _guard = manager.reloading.lock().await;
    running(manager)?;
    if !current(manager, name, request) {
        return Ok(PluginSwitch::Now);
    }
    let old = resident(manager, name)?;
    let kind = classify(manager, &old);
    if kind != PluginSwitch::Now {
        return Ok(kind);
    }
    let found = switch_found(manager, &old);
    let config = manager
        .settings
        .get(&old.found.name)
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let module = Module::load(
        found,
        config,
        manager.limits.clone(),
        &manager.env,
        !enabled,
    )
    .await;
    // A previously unloaded module has no hook metadata until register has run.
    let kind = classify(manager, &module);
    if enabled && kind != PluginSwitch::Now {
        module.close().await;
        return Ok(kind);
    }
    module.admission.decision().map_err(reload::failed)?;
    activate_switch(manager, old, module, enabled).await?;
    Ok(PluginSwitch::Now)
}
fn resident(manager: &Manager, name: &str) -> Result<Arc<Module>, KernelError> {
    manager
        .modules()
        .into_iter()
        .find(|module| {
            module
                .admission
                .status(&module.found, PluginRuntime::Module, "bingo.mods")
                .id
                == name
        })
        .ok_or_else(|| reload::failed("module unavailable"))
}
async fn defer_retirement(
    manager: &Manager,
    name: &str,
    request: &Arc<Request>,
    deferred: tokio::sync::oneshot::Sender<()>,
) -> bool {
    let bare = name.strip_prefix("plugins:").unwrap_or(name);
    if tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wait_retired(manager, name, bare, request),
    )
    .await
    .is_err()
    {
        let _ = deferred.send(());
        return tokio::time::timeout(
            std::time::Duration::from_secs(25),
            wait_retired(manager, name, bare, request),
        )
        .await
        .is_ok();
    }
    true
}
async fn activate_switch(
    manager: &Manager,
    old: Arc<Module>,
    module: Arc<Module>,
    enabled: bool,
) -> Result<(), KernelError> {
    let name = module.found.name.as_str();
    let owned = old.engine.is_some();
    if owned && let Some(dollar) = manager.dollar.get() {
        dollar.unregister(name);
    }
    {
        let mut retired = manager.switches.retired.lock().await;
        manager.replace(&old, module.clone());
        wish(manager, name, enabled);
        if owned {
            retired.push(old);
        }
    }
    manager.publish_callbacks(module.clone());
    let host = manager
        .host
        .get()
        .ok_or_else(|| reload::failed("module host unavailable"))?;
    host.catalog(CatalogKind::Plugins).await?;
    module.admission.decision().map_err(reload::failed)?;
    manager.restart_sessions(&module, host).await?;
    manager.baseline(false);
    Ok(())
}
fn switch_found(manager: &Manager, old: &Module) -> discovery::Found {
    let mut found = old.found.clone();
    if found.origin == PluginOrigin::Project {
        let authority = manager.host.get().and_then(|host| host.service(&APPROVALS));
        found.state = discovery::project_state(&found, authority.as_deref());
    }
    found
}
async fn wait_retired(manager: &Manager, id: &str, name: &str, request: &Arc<Request>) {
    loop {
        let changed = manager.switches.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !current(manager, id, request) {
            return;
        }
        if !manager
            .switches
            .retired
            .lock()
            .await
            .iter()
            .any(|module| module.found.name == name)
        {
            return;
        }
        changed.await;
    }
}
pub(super) async fn reap(manager: &Manager) {
    let _guard = manager.reloading.lock().await;
    reap_locked(manager).await;
}
pub(super) async fn reap_locked(manager: &Manager) {
    let ready: Vec<_> = manager
        .switches
        .retired
        .lock()
        .await
        .iter()
        .filter(|module| Arc::strong_count(module) == 1)
        .map(|module| {
            // Retired acquisition checks this token under the same list lock.
            // Leave cleanup ownership in the list if this future is cancelled.
            if let Some(engine) = &module.engine {
                engine.stopped().cancel();
            }
            module.clone()
        })
        .collect();
    for module in ready {
        module.close().await;
        cleanup(manager, &module).await;
        manager
            .switches
            .retired
            .lock()
            .await
            .retain(|entry| !Arc::ptr_eq(entry, &module));
        manager.switches.changed.notify_waiters();
    }
}

pub(super) async fn cleanup(manager: &Manager, module: &Module) {
    manager
        .callbacks
        .remove_generation(&module.found.name, &module.generation);
    manager.clear_signals(module).await;
    if let Some(dollar) = manager.dollar.get() {
        dollar.reload(&module.found.name, &module.generation).await;
    }
}
fn next(why: &str) -> PluginSwitch {
    PluginSwitch::NextStart { why: why.into() }
}

#[cfg(test)]
mod tests;
