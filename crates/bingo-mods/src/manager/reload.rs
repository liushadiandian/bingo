//! Polling marks a generation stale; a turn boundary replaces it.
use super::*;
use bingo_plugin_host::stamps::{Stamps, stamps};
use serde_json::json;
use std::time::Duration;

pub(super) struct Watch {
    pub(super) before: Stamps,
    stale: bool,
}
impl Manager {
    pub(crate) async fn prepare(&self) -> Result<(), String> {
        acquisition::prepare(self).await
    }
    pub(super) fn watch(self: &Arc<Self>) {
        self.baseline(true);
        let weak = Arc::downgrade(self);
        let stop = self.stop.clone();
        self.tasks.lock().unwrap_or_else(|error| error.into_inner()).push(tokio::spawn(async move {
            let mut clock = tokio::time::interval_at(tokio::time::Instant::now() + Duration::from_secs(1), Duration::from_secs(1));
            clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    _ = clock.tick() => if let Some(manager) = weak.upgrade() { switching::reap(&manager).await; manager.poll().await; manager.reload_if_idle().await; } else { break; },
                }
            }
        }));
    }
    pub(super) async fn poll(&self) {
        let mut roots: Vec<_> = self
            .modules()
            .iter()
            .filter(|module| module.found.origin != PluginOrigin::Project)
            .map(|module| module.found.root.clone())
            .collect();
        roots.extend(self.directories.explicit.iter().cloned());
        roots.push(self.env.config_dir.join("plugins"));
        roots.sort();
        roots.dedup();
        for root in roots {
            let scan = root.clone();
            let after = tokio::task::spawn_blocking(move || stamps(&scan).unwrap_or_default())
                .await
                .unwrap_or_default();
            let mut watches = self
                .watches
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(watch) = watches.get_mut(&root)
                && watch.before != after
                && !watch.stale
            {
                watch.stale = true;
                self.notices.push(Notice::warn(
                    "PLUGIN_STALE",
                    format!(
                        "{}: module changed; reload waits for turn.complete",
                        root.display()
                    ),
                ));
            }
        }
    }
    pub(super) async fn reload_stale(&self) {
        let stale = self
            .watches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|watch| watch.stale);
        if stale && let Err(error) = self.rescan(false).await {
            self.notices
                .push(Notice::warn("PLUGIN_RELOAD", error.message));
        }
    }
    async fn reload_if_idle(&self) {
        if let Some(host) = self.host.get()
            && let Ok(sessions) = host.sessions(SessionFilter::default()).await
            && sessions.iter().all(|session| !session.busy)
        {
            self.reload_stale().await;
        }
    }
    pub(super) fn baseline(&self, project: bool) {
        let mut roots: Vec<_> = self
            .modules()
            .iter()
            .map(|module| module.found.root.clone())
            .collect();
        roots.extend(self.directories.explicit.iter().cloned());
        roots.push(self.env.config_dir.join("plugins"));
        roots.sort();
        roots.dedup();
        let mut watches: BTreeMap<_, _> = roots
            .into_iter()
            .map(|root| {
                let before = stamps(&root).unwrap_or_default();
                (
                    root,
                    Watch {
                        before,
                        stale: false,
                    },
                )
            })
            .collect();
        if !project {
            let previous = self
                .watches
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for module in self
                .modules()
                .iter()
                .filter(|module| module.found.origin == PluginOrigin::Project)
            {
                if let Some(watch) = previous.get(&module.found.root) {
                    watches.insert(
                        module.found.root.clone(),
                        Watch {
                            before: watch.before.clone(),
                            stale: watch.stale,
                        },
                    );
                }
            }
        }
        *self
            .watches
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = watches;
    }
    pub async fn reload_root(&self, root: &Path) -> Result<(), KernelError> {
        let manager = self
            .this
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| failed("module manager is unavailable"))?;
        let root = root.to_path_buf();
        acquisition::run(self, async move { manager.reload_one(&root).await }).await?
    }
    async fn reload_one(&self, root: &Path) -> Result<(), KernelError> {
        let _guard = self.reloading.lock().await;
        switching::running(self)?;
        let root = root.canonicalize().map_err(failed)?;
        let old = self
            .modules()
            .into_iter()
            .find(|module| module.found.root.canonicalize().ok().as_ref() == Some(&root))
            .ok_or_else(|| failed("module directory is not loaded"))?;
        let mut found = discovery::read(&root, &self.notices)
            .ok_or_else(|| failed("module manifest could not be read"))?;
        if bingo_plugin_host::runtime::runtime_of(&found.manifest, true)
            != Ok(PluginRuntime::Module)
        {
            return Err(failed(format!(
                "{}: runtime changed; restart required",
                found.name
            )));
        }
        found.origin = old.found.origin;
        if found.origin == PluginOrigin::Project {
            let authority = self.host.get().and_then(|host| host.service(&APPROVALS));
            found.state = discovery::project_state(&found, authority.as_deref());
        }
        let result = self.install_generation(found, Some(&old)).await;
        self.catalogues_changed();
        let name = old.found.name.clone();
        drop(old);
        switching::reap_locked(self).await;
        result?;
        self.baseline(false);
        self.notices.push(Notice::warn(
            "PLUGIN_RELOADED",
            format!("the {name} plugin reloaded"),
        ));
        Ok(())
    }
    pub(super) fn replace(&self, old: &Arc<Module>, module: Arc<Module>) {
        if let Some(slot) = self
            .modules
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .iter_mut()
            .find(|slot| Arc::ptr_eq(slot, old))
        {
            *slot = module;
        }
    }
    pub(super) async fn registration_settled(&self, module: &Module) -> Result<(), KernelError> {
        // An unrelated catalogue may own the claim. A cancelled claimant must
        // fail this reload, not hold every subsequent reload indefinitely.
        tokio::select! {
            biased;
            _ = self.stop.cancelled() => return Err(failed("module manager stopped")),
            result = tokio::time::timeout(HOOK_DEADLINE, module.admission.settled()) => {
                result.map_err(|_| failed(format!("{}: plugin registration deadline exceeded", module.found.name)))?
                    .map_err(failed)?;
            }
        }
        Ok(())
    }
    pub(super) async fn restart_sessions(
        &self,
        module: &Arc<Module>,
        host: &HostHandle,
    ) -> Result<(), KernelError> {
        if !module.admission.allowed() {
            return Ok(());
        }
        for session in host.sessions(SessionFilter::default()).await? {
            if !self
                .subscriptions
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .contains(&session.id)
            {
                continue;
            }
            let cx = HookContext {
                item: None,
                session: session.id.clone(),
                turn: None,
                cwd: session.cwd.into(),
                provider: None,
                model: None,
                host: host.clone(),
            };
            render::under_input(
                false,
                crate::link::Link(module.clone()).observe(
                    catalogue::SESSION_START,
                    &cx,
                    &json!({"session":session.id}),
                ),
            )
            .await;
        }
        Ok(())
    }
}
pub(super) fn failed(error: impl std::fmt::Display) -> KernelError {
    KernelError::new(ErrorCode::InvalidInput, error.to_string())
}

pub(super) struct Boundary(pub Weak<Manager>);
#[async_trait]
impl Interceptor for Boundary {
    fn id(&self) -> &str {
        "bingo.mods.reload"
    }
    fn matcher(&self) -> LinkMatcher {
        LinkMatcher {
            render: None,
            patterns: [
                "turn.complete",
                "session.start",
                "session.end",
                "session.attach",
                "session.detach",
            ]
            .into_iter()
            .filter_map(|pattern| catalogue::Pattern::parse(pattern).ok())
            .collect(),
            prefilter: Value::Null,
        }
    }
    fn owns_deadline(&self) -> bool {
        true
    }
    async fn observe(&self, entry: &catalogue::Entry, cx: &HookContext, input: &Value) {
        if let Some(manager) = self.0.upgrade() {
            if let Some(dollar) = manager.dollar.get() {
                if entry == catalogue::SESSION_END {
                    dollar.end(&cx.session).await;
                }
                if [catalogue::SESSION_ATTACH, catalogue::SESSION_DETACH].contains(&entry)
                    && let Ok(input) = serde_json::from_value(input.clone())
                    && let Err(reason) = dollar
                        .surface(&cx.host, input, entry == catalogue::SESSION_ATTACH)
                        .await
                {
                    manager.notices.push(Notice::warn("MODS_SURFACE", reason));
                }
            }
            let mut subscriptions = manager
                .subscriptions
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if entry == catalogue::SESSION_START {
                subscriptions.insert(cx.session.clone());
            }
            if entry == catalogue::SESSION_END {
                subscriptions.remove(&cx.session);
            }
        }
    }
    async fn chain(
        &self,
        _: &catalogue::Entry,
        _: &HookContext,
        input: Value,
        next: Next<'_, Value, Value>,
    ) -> Result<Value, KernelError> {
        let result = next.run(input).await;
        if let Some(manager) = self.0.upgrade() {
            manager.reload_stale().await;
        }
        result
    }
}
