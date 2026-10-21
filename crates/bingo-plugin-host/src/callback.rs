//! Runtime callbacks have one owner and end with that owner's generation.
use bingo_sdk::{CancellationToken, HookContext, ServiceKey};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

pub const KEY: ServiceKey<Registry> = ServiceKey::new("bingo.plugin-host.callbacks");
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    fn stopped(&self) -> CancellationToken;
    fn is_current(&self) -> bool {
        true
    }
    fn generation(&self) -> Option<bingo_sdk::ItemId> {
        None
    }
    async fn prepare(&self) -> Result<(), String>;
    async fn call(&self, id: &str, input: Value, cx: &HookContext) -> Result<Value, String>;
}
/// Resolves named dispatch even while its previous runtime is being replaced.
#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    async fn acquire(&self, plugin: &str) -> Result<Arc<dyn Executor>, String>;
}
#[derive(Default)]
struct Owner {
    executors: Vec<Arc<dyn Executor>>,
    resolver: Option<Arc<dyn Resolver>>,
}
#[derive(Default)]
pub struct Registry(RwLock<BTreeMap<String, Owner>>);
impl Registry {
    pub fn insert(&self, plugin: String, executor: Arc<dyn Executor>) {
        self.publish(plugin, executor, None);
    }
    pub fn insert_resolved(
        &self,
        plugin: String,
        executor: Arc<dyn Executor>,
        resolver: Arc<dyn Resolver>,
    ) {
        self.publish(plugin, executor, Some(resolver));
    }
    fn publish(
        &self,
        plugin: String,
        executor: Arc<dyn Executor>,
        resolver: Option<Arc<dyn Resolver>>,
    ) {
        let mut owners = self.0.write().unwrap_or_else(|error| error.into_inner());
        let owner = owners.entry(plugin).or_default();
        owner
            .executors
            .retain(|entry| entry.generation() != executor.generation());
        owner.executors.push(executor);
        owner.resolver = resolver;
    }
    pub fn remove_generation(&self, plugin: &str, generation: &bingo_sdk::ItemId) {
        let mut owners = self.0.write().unwrap_or_else(|error| error.into_inner());
        if let Some(owner) = owners.get_mut(plugin) {
            owner
                .executors
                .retain(|entry| entry.generation().as_ref() != Some(generation));
            if owner.executors.is_empty() && owner.resolver.is_none() {
                owners.remove(plugin);
            }
        }
    }
    pub fn remove(&self, plugin: &str) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(plugin);
    }
    pub fn get(&self, plugin: &str) -> Result<Arc<dyn Executor>, String> {
        let executor = self
            .0
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(plugin)
            .and_then(|entries| {
                entries
                    .executors
                    .iter()
                    .rev()
                    .find(|executor| {
                        bingo_sdk::generation::current(plugin).map_or_else(
                            || executor.is_current(),
                            |id| executor.generation() == Some(id),
                        ) && !executor.stopped().is_cancelled()
                    })
                    .cloned()
            })
            .ok_or_else(|| format!("{plugin}: callback runtime is unavailable"))?;
        Ok(Arc::new(Bound {
            plugin: plugin.into(),
            executor,
        }))
    }
    pub async fn prepared(&self, plugin: &str) -> Result<Arc<dyn Executor>, String> {
        let resolver = self
            .0
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(plugin)
            .and_then(|owner| owner.resolver.clone());
        if let Some(resolver) = resolver {
            return Ok(Arc::new(Bound {
                plugin: plugin.into(),
                executor: resolver.acquire(plugin).await?,
            }));
        }
        self.get(plugin)?.prepare().await?;
        self.get(plugin)
    }
}

/// A retained handle restores its owner's scope when invoked by a timer.
struct Bound {
    plugin: String,
    executor: Arc<dyn Executor>,
}
#[async_trait::async_trait]
impl Executor for Bound {
    fn stopped(&self) -> CancellationToken {
        self.executor.stopped()
    }
    fn generation(&self) -> Option<bingo_sdk::ItemId> {
        self.executor.generation()
    }
    fn is_current(&self) -> bool {
        self.executor.is_current()
    }
    async fn prepare(&self) -> Result<(), String> {
        self.executor.prepare().await
    }
    async fn call(&self, id: &str, input: Value, cx: &HookContext) -> Result<Value, String> {
        let owner = self.generation();
        if self.stopped().is_cancelled()
            || bingo_sdk::generation::current(&self.plugin)
                .is_some_and(|current| Some(current) != owner)
        {
            return Err(format!(
                "{}: callback generation mismatch or stopped",
                self.plugin
            ));
        }
        let call = self.executor.call(id, input, cx);
        match owner {
            Some(generation) => bingo_sdk::generation::scope(&self.plugin, &generation, call).await,
            None => call.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bingo_sdk::*;
    struct Probe {
        generation: ItemId,
        registry: Arc<Registry>,
    }
    #[async_trait::async_trait]
    impl Executor for Probe {
        fn stopped(&self) -> CancellationToken {
            CancellationToken::new()
        }
        fn generation(&self) -> Option<ItemId> {
            Some(self.generation.clone())
        }
        async fn prepare(&self) -> Result<(), String> {
            Ok(())
        }
        async fn call(&self, _: &str, _: Value, _: &HookContext) -> Result<Value, String> {
            Ok(serde_json::json!(
                self.registry.get("fixture")?.generation()
            ))
        }
    }
    #[tokio::test]
    async fn retained_callback_handle_keeps_its_generation_without_ambient_scope() {
        let registry = Arc::new(Registry::default());
        let old = ItemId::mint();
        let new = ItemId::mint();
        registry.insert(
            "fixture".into(),
            Arc::new(Probe {
                generation: old.clone(),
                registry: registry.clone(),
            }),
        );
        let timer = registry.get("fixture").unwrap();
        registry.insert(
            "fixture".into(),
            Arc::new(Probe {
                generation: new.clone(),
                registry: registry.clone(),
            }),
        );
        let cx = HookContext {
            session: SessionId::mint(),
            turn: None,
            item: None,
            cwd: ".".into(),
            provider: None,
            model: None,
            host: testing::NoHost::handle(),
        };
        let answer = timer.call("callback-1", Value::Null, &cx).await.unwrap();
        assert_eq!(
            answer,
            serde_json::json!(old),
            "timer dispatched into the replacement"
        );
        let mismatch =
            generation::scope("fixture", &new, timer.call("callback-1", Value::Null, &cx)).await;
        assert!(
            mismatch.is_err(),
            "foreign generation accepted callback handle"
        );
        registry.remove("fixture");
    }
}
