//! A loaded module and the registration decision for its current generation.
use crate::engine::{Engine, Limits};
use bingo_plugin_host::{admission::Admission, discovery::Found, manifest::Launch, state::Store};
use bingo_sdk::*;
use serde_json::Value;
use std::sync::{Arc, OnceLock};

pub(crate) struct Module {
    pub found: Found,
    pub generation: ItemId,
    pub admission: Admission,
    pub engine: Option<Arc<Engine>>,
    pub store: Store,
    pub started: OnceLock<crate::testing::Started>,
}
impl Module {
    pub async fn load(
        found: Found,
        config: Value,
        limits: Limits,
        store: Store,
        switched_off: bool,
    ) -> Arc<Self> {
        let admission = Admission::default();
        let engine = if !found.state.is_on() {
            admission.stop(found.state.clone());
            None
        } else if switched_off {
            admission.stop(PluginState::Off {
                why: SWITCHED_OFF.into(),
            });
            None
        } else if let Some(schema) = &found.manifest.config
            && let Err(reason) = bingo_plugin_host::config::validate(schema, &config)
        {
            admission.refuse(format!("{}: {reason}", found.name));
            None
        } else if let Launch::Module(module) = &found.manifest.launch {
            match Engine::start(&found.root, module, config, limits).await {
                Ok(engine) => Some(engine),
                Err(error) => {
                    admission.refuse(format!("{}: {error}", found.name));
                    None
                }
            }
        } else {
            None
        };
        Arc::new(Self {
            generation: ItemId::mint(),
            store,
            found,
            admission,
            engine,
            started: OnceLock::new(),
        })
    }
    pub fn identity(&self) -> crate::dollar::table::PluginData {
        crate::dollar::table::PluginData {
            name: self.found.name.clone(),
            root: self.found.root.clone(),
            version: Some(self.found.manifest.version.clone()),
        }
    }
    pub async fn close(&self) {
        if let Some(engine) = &self.engine {
            engine.close().await;
        }
    }
}
