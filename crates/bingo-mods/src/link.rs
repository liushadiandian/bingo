//! A mod is one interceptor; its registered hooks compose within that link.
use crate::{chain::Chain, module::Module};
use async_trait::async_trait;
use bingo_sdk::*;
use serde_json::Value;
use std::sync::{Arc, Mutex};

use bingo_plugin_host::invocation;

pub(crate) struct Link(pub Arc<Module>);
#[async_trait]
impl Interceptor for Link {
    fn id(&self) -> &str {
        &self.0.found.name
    }
    fn matcher(&self) -> LinkMatcher {
        LinkMatcher {
            render: Some(
                self.0
                    .engine
                    .as_ref()
                    .into_iter()
                    .flat_map(|engine| engine.hooks())
                    .filter(|hook| {
                        catalogue::Pattern::parse(&hook.pattern)
                            .is_ok_and(|pattern| pattern.matches("ui.render"))
                    })
                    .map(|hook| hook.matcher.clone())
                    .collect(),
            ),
            patterns: self
                .0
                .engine
                .as_ref()
                .into_iter()
                .flat_map(|engine| engine.hooks())
                .filter_map(|hook| catalogue::Pattern::parse(&hook.pattern).ok())
                .collect(),
            prefilter: Value::Null,
        }
    }
    fn owns_deadline(&self) -> bool {
        true
    }
    async fn chain(
        &self,
        entry: &catalogue::Entry,
        cx: &HookContext,
        input: Value,
        next: Next<'_, Value, Value>,
    ) -> Result<Value, KernelError> {
        let Some(engine) = &self.0.engine else {
            return next.run(input).await;
        };
        let mut hooks = Vec::new();
        for (index, hook) in engine.hooks().iter().enumerate() {
            if !catalogue::Pattern::parse(&hook.pattern)
                .is_ok_and(|pattern| pattern.matches(&entry.name))
            {
                continue;
            }
            let skipped = invocation::current()
                .iter()
                .any(|calling| calling.plugin == self.id() && calling.hook == index);
            if skipped {
                let _ = cx
                    .host
                    .notice(
                        Level::Warn,
                        "INTERCEPTOR_SKIPPED",
                        &format!(
                            "{} ({}): re-entry: calling hook skipped",
                            self.id(),
                            entry.name
                        ),
                    )
                    .await;
            } else {
                hooks.push(index);
            }
        }
        let chain = Chain {
            module: self.0.clone(),
            engine: engine.clone(),
            entry: entry.clone(),
            cx: cx.clone(),
            tier: next.tier(),
            hooks,
            reads: Default::default(),
            generation: ItemId::mint().to_string(),
            kernel: Arc::new(Mutex::new(Some(next))),
        };
        let request = (entry == catalogue::UI_RENDER)
            .then(|| input["requestId"].as_str().map(str::to_string))
            .flatten();
        let result = chain.at(0, input).await;
        if let Some(request) = request {
            self.0
                .store
                .subscribe(&cx.host, cx.session.clone(), request, chain.reads.entries())
                .await
                .map_err(|error| failed(error.message))?;
        }
        result
    }
    async fn observe(&self, entry: &catalogue::Entry, cx: &HookContext, input: &Value) {
        if let Err(error) = self
            .chain(
                entry,
                cx,
                input.clone(),
                Next::at(
                    self.0
                        .admission
                        .decision()
                        .ok()
                        .flatten()
                        .map_or(Tier::Tighten, |grant| grant.tier),
                    |_| Box::pin(async { Ok(Value::Null) }),
                ),
            )
            .await
        {
            let _ = cx
                .host
                .notice(
                    Level::Warn,
                    "INTERCEPTOR_SKIPPED",
                    &format!("{} ({}): {}", self.id(), entry.name, error.message),
                )
                .await;
        }
        if entry == catalogue::SESSION_START
            && let Some(started) = self.0.started.get()
        {
            started.send_modify(|sessions| {
                sessions.insert(cx.session.clone());
            });
        }
    }
}
pub(crate) async fn operation(
    module: &Module,
    cx: &HookContext,
    hook: usize,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    // Keep the large host future off the stack when grant scopes compose.
    let operation = Box::pin(generation::scope(
        &module.found.name,
        &module.generation,
        invocation::from(&module.found.name, hook, async {
            tokio::time::timeout(
                HOOK_DEADLINE,
                bingo_plugin_host::ops::call(
                    &cx.host,
                    &module.found.name,
                    &module.store,
                    method,
                    params,
                ),
            )
            .await
            .map_err(|_| format!("{method}: host operation deadline exceeded"))?
        }),
    ));
    match module.admission.decision()? {
        Some(grant) => bingo_plugin_host::grant::scope(&module.found.name, grant, operation).await,
        None => operation.await,
    }
}

pub(crate) fn failed(error: impl std::fmt::Display) -> KernelError {
    KernelError::new(ErrorCode::Internal, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    struct Held;
    #[async_trait]
    impl WireService for Held {
        async fn call(&self, _: &str, _: Value) -> Result<Value, ServiceError> {
            futures::future::pending().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn a_dollar_await_has_a_wall_bound_while_own_time_is_paused() {
        let home = tempfile::tempdir().unwrap();
        let found = bingo_plugin_host::discovery::Found {
            state: PluginState::On,
            name: "fixture".into(),
            root: home.path().into(),
            origin: PluginOrigin::Explicit,
            manifest: serde_json::from_value(
                json!({"name":"fixture","version":"1","module":"main.ts"}),
            )
            .unwrap(),
        };
        let module = Module::load(
            found,
            Value::Null,
            crate::engine::Limits::default(),
            bingo_plugin_host::state::Store::new(&Env::rooted(home.path()).data_dir, "fixture"),
            true,
        )
        .await;
        let host = testing::TestHost::default();
        host.on_catalog(|kind| async move {
            Ok(Catalog {
                kind,
                entries: vec![CatalogEntry {
                    id: "fixture".into(),
                    label: "fixture".into(),
                    meta: json!({"state":{"kind":"on"},"tier":"rewrite","uses":["held"]}),
                }],
            })
        });
        host.handle().open_service("held", Arc::new(Held)).unwrap();
        let cx = HookContext {
            item: None,
            session: SessionId::from_raw("host"),
            turn: None,
            cwd: home.path().into(),
            provider: None,
            model: None,
            host: host.handle(),
        };
        let start = tokio::time::Instant::now();
        let error = operation(
            &module,
            &cx,
            0,
            "service/call",
            json!({"key":"held","method":"wait","params":null}),
        )
        .await
        .unwrap_err();
        assert!(error.contains("deadline exceeded"), "{error}");
        assert_eq!(tokio::time::Instant::now() - start, HOOK_DEADLINE);
    }
}
