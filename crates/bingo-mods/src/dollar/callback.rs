//! A callback runs on its module thread with the ordinary hook meter.
use crate::{manager::Manager, module::Module};
use bingo_plugin_host::callback::Executor;
use bingo_sdk::*;
use serde_json::{Value, json};
use std::sync::{Arc, Weak};

pub(crate) struct Native {
    pub module: Owner,
    pub generation: ItemId,
    pub manager: Weak<Manager>,
}
pub(crate) enum Owner {
    Watching(Weak<Module>),
    Acquired(Arc<Module>),
}
impl Owner {
    fn get(&self) -> Option<Arc<Module>> {
        match self {
            Self::Watching(module) => module.upgrade(),
            Self::Acquired(module) => Some(module.clone()),
        }
    }
}
#[async_trait::async_trait]
impl Executor for Native {
    fn stopped(&self) -> CancellationToken {
        self.module
            .get()
            .and_then(|module| module.engine.as_ref().map(|engine| engine.stopped()))
            .unwrap_or_else(|| {
                let stop = CancellationToken::new();
                stop.cancel();
                stop
            })
    }
    fn is_current(&self) -> bool {
        self.manager
            .upgrade()
            .is_some_and(|manager| manager.has_generation(&self.generation))
    }
    fn generation(&self) -> Option<ItemId> {
        Some(self.generation.clone())
    }
    async fn prepare(&self) -> Result<(), String> {
        self.manager
            .upgrade()
            .ok_or("module manager is unavailable")?
            .prepare()
            .await
    }
    async fn call(&self, id: &str, input: Value, cx: &HookContext) -> Result<Value, String> {
        let module = self
            .module
            .get()
            .ok_or("module generation is unavailable")?;
        if !module.admission.allowed() {
            return Err("module is not admitted".into());
        }
        if id == "__ui__" && input["event"] == "open" {
            let ui = cx
                .host
                .service_wire("bingo.plugin.ui")
                .ok_or("ui is unavailable")?;
            return generation::scope(
                &module.found.name,
                &module.generation,
                bingo_plugin_host::ops::scope(
                    &cx.host,
                    &module.found.name,
                    &module.store,
                    ui.call(
                        "choose",
                        json!({"session":cx.session,"id":input["handler"]}),
                    ),
                ),
            )
            .await
            .map_err(|error| error.to_string());
        }
        let engine = module.engine.as_ref().ok_or("module is unavailable")?;
        let site = json!({"callback":id,"session":cx.session,"turn":cx.turn,"cwd":cx.cwd,
            "plugin":module.identity()});
        let mut call = engine
            .call("callback", input, site)
            .map_err(|error| error.to_string())?;
        loop {
            tokio::select! {
                biased;
                Some(operation) = call.operations.recv() => {
                    let result = call.reads.scope(crate::link::operation(&module, cx, usize::MAX,
                        &operation.method, operation.params)).await;
                    let _ = operation.reply.send(result);
                }
                answer = &mut call.result => return answer.map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string()),
            }
        }
    }
}
