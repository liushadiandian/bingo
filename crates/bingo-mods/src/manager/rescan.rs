//! Reconcile discovery through the same admission and restart path as an edit.
use super::*;
use bingo_plugin_host::{discovery::Found, stamps::stamps};

impl Manager {
    pub(super) async fn rescan(&self, project: bool) -> Result<PluginReload, KernelError> {
        let _guard = self.reloading.lock().await;
        switching::running(self)?;
        let old = self.modules();
        let (found, mut report) = self.discovered(project, &old);
        for module in &old {
            let replacement = found.iter().find(|found| found.root == module.found.root);
            let changed = replacement.is_none_or(|found| {
                module.found != *found
                    || (project || found.origin != PluginOrigin::Project)
                        && self.changed(&found.root)
            });
            if changed {
                self.remove_module(module).await;
                if replacement.is_none() {
                    report.removed.push(module.found.name.clone());
                }
            }
        }
        for found in found {
            let previous = old.iter().find(|module| module.found.root == found.root);
            if !project && found.origin == PluginOrigin::Project {
                continue;
            }
            if previous.is_some_and(|module| module.found == found && !self.changed(&found.root)) {
                continue;
            }
            let name = found.name.clone();
            match self.install_generation(found, None).await {
                Ok(()) => {
                    if previous.is_some() {
                        report.reloaded.push(name);
                    } else {
                        report.added.push(name);
                    }
                }
                Err(error) => {
                    let text = format!("{name}: {}", error.message);
                    self.notices.push(Notice::warn("PLUGIN_RELOAD", &text));
                    report.errors.push(text);
                }
            }
        }
        drop(old);
        switching::reap_locked(self).await;
        self.baseline(project);
        self.report_changes(&report);
        self.catalogues_changed();
        Ok(report)
    }
    fn discovered(&self, project: bool, old: &[Arc<Module>]) -> (Vec<Found>, PluginReload) {
        let notices = Notices::default();
        let mut found = self.directories.read(project, &notices, self.host.get());
        let mut report = PluginReload::default();
        for notice in notices.drain() {
            report.errors.push(notice.text.clone());
            self.notices.push(notice);
        }
        if !project {
            found.retain(|found| found.origin != PluginOrigin::Project);
            for module in old
                .iter()
                .filter(|module| module.found.origin == PluginOrigin::Project)
            {
                if found.iter().any(|found| {
                    found.root == module.found.root && found.origin == PluginOrigin::Explicit
                }) {
                    continue;
                }
                if module.found.state.is_on() {
                    found.retain(|found| found.name != module.found.name);
                }
                found.push(module.found.clone());
            }
        }
        found.retain(|found| {
            bingo_plugin_host::runtime::runtime_of(&found.manifest, true)
                == Ok(PluginRuntime::Module)
        });
        (found, report)
    }
    fn report_changes(&self, report: &PluginReload) {
        for name in report.added.iter().chain(&report.reloaded) {
            self.notices.push(Notice::warn(
                "PLUGIN_RELOADED",
                format!("the {name} plugin reloaded"),
            ));
        }
        for name in &report.removed {
            self.notices.push(Notice::warn(
                "PLUGIN_UNLOADED",
                format!("the {name} plugin unloaded"),
            ));
        }
    }
    fn changed(&self, root: &Path) -> bool {
        let after = stamps(root).unwrap_or_default();
        self.watches
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(root)
            .is_none_or(|watch| watch.before != after)
    }
    async fn remove_module(&self, module: &Arc<Module>) {
        {
            let mut retired = self.switches.retired.lock().await;
            self.modules
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|entry| !Arc::ptr_eq(entry, module));
            retired.push(module.clone());
        }
        if let Some(dollar) = self.dollar.get() {
            dollar.retire(&module.generation).await;
        }
        self.clear_signals(module).await;
    }
    pub(super) async fn clear_signals(&self, module: &Module) {
        if let Some(host) = self.host.get() {
            for session in host
                .sessions(SessionFilter::default())
                .await
                .unwrap_or_default()
            {
                if let Some(dollar) = self.dollar.get()
                    && let Ok(state) = dollar.snapshot(host, &session.id).await
                {
                    for kind in state
                        .signals
                        .get(&module.found.name)
                        .into_iter()
                        .flat_map(|values| values.iter())
                        .filter_map(|(kind, signal)| {
                            (signal.generation() == Some(&module.generation)).then_some(kind)
                        })
                    {
                        let _ = generation::scope(
                            &module.found.name,
                            &module.generation,
                            host.signal(&session.id, &module.found.name, kind, Value::Null),
                        )
                        .await;
                    }
                }
            }
        }
    }
    async fn handoff_signals(
        &self,
        host: &HostHandle,
        old: &Module,
        module: &Module,
    ) -> Result<(), KernelError> {
        if !module.admission.allowed() {
            self.clear_signals(old).await;
            return Ok(());
        }
        for session in host.sessions(SessionFilter::default()).await? {
            host.transfer_signals(
                &session.id,
                &module.found.name,
                &old.generation,
                &module.generation,
            )
            .await?;
        }
        Ok(())
    }
    pub(super) async fn install_generation(
        &self,
        found: Found,
        old: Option<&Arc<Module>>,
    ) -> Result<(), KernelError> {
        let config = self
            .settings
            .get(&found.name)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let off = self
            .switches
            .off
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&found.name);
        let module = Module::load(found, config, self.limits.clone(), &self.env, off).await;
        if let Some(old) = old {
            let mut retired = self.switches.retired.lock().await;
            self.replace(old, module.clone());
            retired.push(old.clone());
        } else {
            self.modules
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .push(module.clone());
        }
        if let Some(old) = old
            && let Some(dollar) = self.dollar.get()
        {
            dollar.retire(&old.generation).await;
        }
        self.publish_callbacks(module.clone());
        let host = self
            .host
            .get()
            .ok_or_else(|| reload::failed("module host is unavailable"))?;
        host.catalog(CatalogKind::Plugins).await?;
        if let Some(old) = old {
            self.handoff_signals(host, old, &module).await?;
        }
        module.admission.decision().map_err(reload::failed)?;
        self.restart_sessions(&module, host).await?;
        Ok(())
    }
}
