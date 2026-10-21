//! One shelf per plugin, with serialized compare-and-swap writes.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use bingo_jsonrpc::codec::{INVALID_PARAMS, RpcError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const INTERNAL_ERROR: i64 = -32603;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StateValue {
    pub version: u64,
    pub value: Value,
}
use bingo_shelf::{Codec, CodecError, Shelf};
use serde_json::{Value, json};

pub type Version = u64;

#[derive(Debug, PartialEq, Eq)]
pub struct Conflict(pub Option<Version>);

pub fn swap(held: Option<Version>, if_version: Option<Version>) -> Result<Version, Conflict> {
    if if_version.is_some() && if_version != held {
        return Err(Conflict(held));
    }
    held.unwrap_or_default()
        .checked_add(1)
        .ok_or(Conflict(held))
}

struct Json;
impl Codec for Json {
    type Record = (String, BTreeMap<String, StateValue>);
    const EXT: &'static str = "json";
    fn encode(record: &Self::Record) -> Result<String, CodecError> {
        serde_json::to_string(&record.1).map_err(|e| CodecError(e.to_string()))
    }
    fn decode(id: &str, text: &str) -> Result<Self::Record, CodecError> {
        serde_json::from_str(text)
            .map(|value| (id.into(), value))
            .map_err(|e| CodecError(e.to_string()))
    }
}

#[derive(Clone)]
pub struct Store {
    root: PathBuf,
    lock: Arc<tokio::sync::Mutex<()>>,
    subscribed: Arc<std::sync::Mutex<crate::subscribed::Subscribed>>,
}
impl Store {
    pub fn new(data_dir: &std::path::Path, plugin: &str) -> Self {
        Self {
            root: data_dir.join("plugins").join(plugin).join("state"),
            lock: Arc::default(),
            subscribed: Arc::default(),
        }
    }

    /// Share serialized file access, but start fresh render subscriptions.
    pub fn fork(&self) -> Self {
        Self {
            root: self.root.clone(),
            lock: self.lock.clone(),
            subscribed: Arc::default(),
        }
    }

    pub async fn subscribe(
        &self,
        host: &bingo_sdk::HostHandle,
        session: bingo_sdk::SessionId,
        request: String,
        reads: Vec<Read>,
    ) -> Result<(), RpcError> {
        self.subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .subscribe(session.clone(), request.clone(), reads.clone());
        for read in reads {
            if self
                .get(read.scope, read.key)
                .await?
                .map(|entry| entry.version)
                != read.version
            {
                host.invalidate(&session, vec![request])
                    .await
                    .map_err(failed)?;
                break;
            }
        }
        Ok(())
    }
    pub fn forget(&self, session: &bingo_sdk::SessionId, request: &str) {
        self.subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .forget(session, request);
    }
    pub async fn changed(
        &self,
        host: &bingo_sdk::HostHandle,
        scope: &str,
        key: &str,
    ) -> Result<(), RpcError> {
        let requests = self
            .subscribed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .changed(scope, key);
        for (session, requests) in requests {
            host.invalidate(&session, requests).await.map_err(failed)?;
        }
        Ok(())
    }

    pub async fn get(&self, scope: String, key: String) -> Result<Option<StateValue>, RpcError> {
        self.access(scope, move |_, values, _| Ok(values.get(&key).cloned()))
            .await
    }

    pub async fn set(
        &self,
        scope: String,
        key: String,
        value: Value,
        expected: Option<Version>,
    ) -> Result<StateValue, RpcError> {
        writable()?;
        self.access(scope, move |(shelf, scope), values, other_bytes| {
            let held = values.get(&key).map(|entry| entry.version);
            let version = swap(held, expected).map_err(|Conflict(version)| {
                let mut error = RpcError::new(-32009, "plugin state version conflict");
                error.data = Some(json!({"version": version}));
                error
            })?;
            let entry = StateValue { version, value };
            values.insert(key, entry.clone());
            let bytes = serde_json::to_vec(values).map_err(failed)?.len();
            if bytes.saturating_add(other_bytes) > 4 * 1024 * 1024 {
                return Err(RpcError::new(
                    -32010,
                    "plugin state exceeds the 4 MiB per-plugin limit",
                ));
            }
            shelf
                .save(scope, &(scope.into(), values.clone()))
                .map_err(failed)?;
            Ok(entry)
        })
        .await
    }

    pub async fn keys(&self, scope: String) -> Result<Vec<String>, RpcError> {
        self.access(scope, |_, values, _| Ok(values.keys().cloned().collect()))
            .await
    }
    pub async fn delete(&self, scope: String, key: String) -> Result<bool, RpcError> {
        writable()?;
        self.access(scope, move |(shelf, scope), values, _| {
            let removed = values.remove(&key).is_some();
            if removed {
                shelf
                    .save(scope, &(scope.into(), values.clone()))
                    .map_err(failed)?;
            }
            Ok(removed)
        })
        .await
    }

    pub async fn remove(&self, scope: String) -> Result<(), RpcError> {
        self.access(scope, |(shelf, scope), _, _| {
            shelf.delete(scope).map_err(failed)
        })
        .await
    }

    async fn access<T: Send + 'static>(
        &self,
        scope: String,
        operation: impl FnOnce(
            (&Shelf<Json>, &str),
            &mut BTreeMap<String, StateValue>,
            usize,
        ) -> Result<T, RpcError>
        + Send
        + 'static,
    ) -> Result<T, RpcError> {
        validate(&scope)?;
        let guard = self.lock.clone().lock_owned().await;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let shelf = Shelf::<Json>::new(root);
            let loaded = shelf.load();
            if let Some(error) = loaded.unreadable.first() {
                return Err(failed(&error.why));
            }
            let other_bytes = loaded
                .entries
                .iter()
                .filter(|(id, _)| id != &scope)
                .try_fold(0usize, |total, (_, values)| {
                    serde_json::to_vec(values)
                        .map(|bytes| total.saturating_add(bytes.len()))
                        .map_err(failed)
                })?;
            let mut values = loaded
                .entries
                .into_iter()
                .find(|(id, _)| id == &scope)
                .map(|(_, values)| values)
                .unwrap_or_default();
            operation((&shelf, &scope), &mut values, other_bytes)
        })
        .await
        .map_err(failed)?
    }
}
fn validate(scope: &str) -> Result<(), RpcError> {
    if !scope.is_empty()
        && scope.len() <= 120
        && scope
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Ok(());
    }
    Err(RpcError::new(INVALID_PARAMS, "invalid plugin state scope"))
}
fn failed(error: impl std::fmt::Display) -> RpcError {
    RpcError::new(INTERNAL_ERROR, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generation_views_share_storage_even_after_a_writer_is_dropped() {
        let root = tempfile::tempdir().unwrap();
        let owner = Store::new(root.path(), "fixture");
        let old = owner.fork();
        let write = old.lock.clone().try_lock_owned().unwrap();
        drop(old);
        let replacement = owner.fork();
        assert!(Arc::ptr_eq(&owner.lock, &replacement.lock));
        assert!(replacement.lock.try_lock().is_err());
        assert!(!Arc::ptr_eq(&owner.subscribed, &replacement.subscribed));
        let caller = replacement.clone();
        assert!(Arc::ptr_eq(&caller.subscribed, &replacement.subscribed));
        drop(write);
        assert!(replacement.lock.try_lock().is_ok());
    }

    #[test]
    fn compare_and_swap_only_accepts_the_held_version() {
        assert_eq!(swap(None, None), Ok(1));
        assert_eq!(swap(Some(1), None), Ok(2));
        assert_eq!(swap(Some(1), Some(1)), Ok(2));
        assert_eq!(swap(None, Some(0)), Err(Conflict(None)));
        assert_eq!(swap(Some(2), Some(1)), Err(Conflict(Some(2))));
        assert_eq!(swap(Some(u64::MAX), None), Err(Conflict(Some(u64::MAX))));
    }
}

#[cfg(test)]
mod quota_contract {
    use super::*;
    #[tokio::test]
    async fn the_four_mib_limit_counts_every_scope_and_preserves_the_old_value() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), "owner");
        let large = json!("x".repeat(2 * 1024 * 1024));
        store
            .set("global".into(), "large".into(), large.clone(), None)
            .await
            .unwrap();
        store
            .set("session".into(), "small".into(), json!(1), None)
            .await
            .unwrap();
        let error = store
            .set("session".into(), "small".into(), large, Some(1))
            .await
            .expect_err("4 MiB budget was not enforced");
        assert!(error.message.contains("4 MiB"), "{}", error.message);
        assert_eq!(
            store.get("session".into(), "small".into()).await.unwrap(),
            Some(StateValue {
                version: 1,
                value: json!(1)
            })
        );
        let error = store
            .set("session".into(), "small".into(), json!(2), Some(0))
            .await
            .unwrap_err();
        assert_eq!(error.data, Some(json!({"version":1})));
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Read {
    pub plugin: String,
    pub scope: String,
    pub key: String,
    pub version: Option<Version>,
}
#[derive(Clone, Default)]
pub struct ReadLog(Arc<std::sync::Mutex<Vec<Read>>>);
tokio::task_local! { static READS: ReadLog; }
impl ReadLog {
    pub async fn scope<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        READS.scope(self.clone(), future).await
    }
    pub fn entries(&self) -> Vec<Read> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    pub fn record(read: Read) {
        let _ = READS.try_with(|log| {
            log.0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(read)
        });
    }
}
#[cfg(test)]
mod read_contract {
    use super::*;
    #[tokio::test]
    async fn a_hook_read_log_names_the_owner_scope_key_and_held_version() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), "owner");
        store
            .set("global".into(), "key".into(), json!(1), None)
            .await
            .unwrap();
        let host = bingo_sdk::testing::NoHost::handle();
        let ops = crate::host::Operations {
            plugin: "owner",
            host: &host,
            state: &store,
        };
        let log = ReadLog::default();
        log.scope(ops.answer("host/state/get", json!({"scope":"global","key":"key"})))
            .await
            .unwrap();
        assert_eq!(
            log.entries(),
            vec![Read {
                plugin: "owner".into(),
                scope: "global".into(),
                key: "key".into(),
                version: Some(1)
            }]
        );
    }
}

tokio::task_local! { static RENDERING: bool; }
fn writable() -> Result<(), RpcError> {
    if RENDERING.try_with(|active| *active).unwrap_or(false) {
        Err(RpcError::new(
            INVALID_PARAMS,
            "state writes are refused during ui.render",
        ))
    } else {
        Ok(())
    }
}
pub async fn rendering<T>(active: bool, future: impl std::future::Future<Output = T>) -> T {
    RENDERING
        .scope(
            active || RENDERING.try_with(|active| *active).unwrap_or(false),
            future,
        )
        .await
}
#[cfg(test)]
mod render_contract {
    use super::*;
    #[tokio::test]
    async fn state_set_during_render_is_refused_in_words() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::new(root.path(), "counter");
        let result = rendering(
            true,
            store.set("global".into(), "count".into(), json!(1), None),
        )
        .await;
        assert!(result.is_err_and(|error| error.message.contains("render")));
        assert!(
            store
                .get("global".into(), "count".into())
                .await
                .unwrap()
                .is_none()
        );
    }
}
