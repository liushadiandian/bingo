//! Registered names resolve their callback after the owner's reload check.
use super::params::{RegisterCommand, RegisterTool};
use bingo_plugin_host::{
    callback::{self, Registry},
    ops::Caller,
    tool_name::tool_name,
};
use bingo_sdk::*;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
enum Spec {
    Tool(ToolSpec),
    Command(CommandSpec),
}
#[derive(Clone)]
struct Named<T> {
    plugin: String,
    callback: String,
    spec: T,
    executor: Arc<dyn callback::Executor>,
    registry: Arc<Registry>,
}
type RegistrationKey = (String, Option<ItemId>, String);
#[derive(Default)]
pub struct Registrations(Mutex<BTreeMap<RegistrationKey, Named<Spec>>>);
impl Registrations {
    pub fn tool(&self, caller: &Caller, input: RegisterTool) -> Result<String, String> {
        let name = tool_name(&caller.plugin, &input.spec.name);
        let mut spec = input.spec;
        spec.name = name.clone();
        self.insert(caller, input.callback, Spec::Tool(spec))?;
        caller.host.catalog_changed(CatalogKind::Tools);
        Ok(name)
    }
    pub fn command(&self, caller: &Caller, input: RegisterCommand) -> Result<String, String> {
        if let Some(taken) = input.spec.foreign_handler(&caller.plugin) {
            return Err(format!(
                "{taken} answers another plugin's controls; a mod may name only its own {}",
                CommandSpec::ui_name(&caller.plugin)
            ));
        }
        let name = input.spec.name.clone();
        self.insert(caller, input.callback, Spec::Command(input.spec))?;
        Ok(name)
    }
    fn insert(&self, caller: &Caller, callback: String, spec: Spec) -> Result<(), String> {
        let registry = caller
            .host
            .service(&callback::KEY)
            .ok_or("callback registry is unavailable")?;
        let executor = registry.get(&caller.plugin)?;
        let named = Named {
            plugin: caller.plugin.clone(),
            callback: callback.clone(),
            spec,
            executor,
            registry,
        };
        let mut entries = self.0.lock().unwrap_or_else(|error| error.into_inner());
        entries.retain(|_, entry| !entry.executor.stopped().is_cancelled());
        entries.insert(
            (caller.plugin.clone(), named.executor.generation(), callback),
            named,
        );
        Ok(())
    }
    pub fn remove(&self, plugin: &str) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(owner, _, _), _| owner != plugin);
    }
    pub fn has_tools(&self, plugin: &str) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|entry| {
                entry.plugin == plugin && matches!(entry.spec, Spec::Tool(_)) && entry.available()
            })
    }
    pub fn contents(&self, plugin: &str) -> plugin::PluginContents {
        let entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut contents = plugin::PluginContents::default();
        for entry in entries
            .values()
            .filter(|entry| entry.plugin == plugin && entry.available())
        {
            match &entry.spec {
                Spec::Command(spec) if !spec.hidden => contents.commands.push(spec.clone()),
                Spec::Tool(spec) => contents.tools.push(spec.name.clone()),
                _ => {}
            }
        }
        contents
    }
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter_map(|entry| match &entry.spec {
                Spec::Tool(spec) if entry.available() => {
                    Some(Arc::new(RegisteredTool(entry.with_spec(spec.clone()))) as Arc<dyn Tool>)
                }
                _ => None,
            })
            .collect()
    }
    pub fn commands(&self) -> Vec<Arc<dyn Command>> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter_map(|entry| match &entry.spec {
                Spec::Command(spec) if entry.available() => {
                    Some(Arc::new(RegisteredCommand(entry.with_spec(spec.clone())))
                        as Arc<dyn Command>)
                }
                _ => None,
            })
            .collect()
    }
}
impl<T> Named<T> {
    fn available(&self) -> bool {
        self.executor.is_current() && !self.executor.stopped().is_cancelled()
    }
    fn with_spec<S>(&self, spec: S) -> Named<S> {
        Named {
            plugin: self.plugin.clone(),
            callback: self.callback.clone(),
            spec,
            executor: self.executor.clone(),
            registry: self.registry.clone(),
        }
    }
    async fn run(&self, input: Value, cx: &HookContext) -> Result<Value, String> {
        let executor = self.registry.prepared(&self.plugin).await?;
        executor.call(&self.callback, input, cx).await
    }
}
struct RegisteredTool(Named<ToolSpec>);
#[async_trait::async_trait]
impl Tool for RegisteredTool {
    fn spec(&self) -> ToolSpec {
        self.0.spec.clone()
    }
    async fn call(&self, input: Value, cx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let context = HookContext {
            item: Some(cx.item.clone()),
            session: cx.session.clone(),
            turn: Some(cx.turn.clone()),
            cwd: cx.cwd.clone(),
            provider: None,
            model: None,
            host: cx.host.clone(),
        };
        let answer = self
            .0
            .run(input, &context)
            .await
            .map_err(ToolError::Failed)?;
        serde_json::from_value(answer).map_err(|error| ToolError::Failed(error.to_string()))
    }
}
struct RegisteredCommand(Named<CommandSpec>);
#[async_trait::async_trait]
impl Command for RegisteredCommand {
    fn spec(&self) -> CommandSpec {
        self.0.spec.clone()
    }
    async fn run(&self, args: &str, cx: &CommandContext) -> Result<CommandOutcome, KernelError> {
        let context = HookContext {
            item: None,
            session: cx.session.clone(),
            turn: None,
            cwd: cx.cwd.clone(),
            provider: None,
            model: None,
            host: cx.host.clone(),
        };
        let result = tokio::select! {
            biased;
            _ = cx.cancel.cancelled() => return Err(failed("command cancelled")),
            result = self.0.run(Value::String(args.into()), &context) => result.map_err(failed)?,
        };
        serde_json::from_value(result).map_err(failed)
    }
}
fn failed(error: impl std::fmt::Display) -> KernelError {
    KernelError::new(ErrorCode::Internal, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bingo_plugin_host::state::Store;

    fn caller(root: &std::path::Path) -> Caller {
        Caller {
            plugin: "aaa".into(),
            host: testing::TestHost::default().handle(),
            state: Store::new(root, "aaa"),
        }
    }
    fn named(name: &str, aliases: &[&str]) -> RegisterCommand {
        serde_json::from_value(serde_json::json!({
            "session": "s",
            "spec": {"name": name, "aliases": aliases, "hint": "", "args": {"kind": "none"},
                "family": "fixture"},
            "callback": "command/1",
        }))
        .unwrap()
    }

    #[test]
    fn a_mod_cannot_register_another_plugins_handler() {
        let root = tempfile::tempdir().unwrap();
        let registrations = Registrations::default();
        for (name, aliases) in [("counter:ui", &[][..]), ("hello", &["counter:ui"][..])] {
            let refused = registrations
                .command(&caller(root.path()), named(name, aliases))
                .unwrap_err();
            assert!(refused.contains("counter:ui"), "{refused}");
        }
        for own in ["aaa:ui", "hello"] {
            let refused = registrations
                .command(&caller(root.path()), named(own, &[]))
                .unwrap_err();
            assert!(!refused.contains("controls"), "{own}: {refused}");
        }
        assert!(registrations.commands().is_empty());
    }

    struct Generation {
        id: ItemId,
        current: std::sync::atomic::AtomicBool,
        stopped: CancellationToken,
    }
    impl Generation {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                id: ItemId::mint(),
                current: std::sync::atomic::AtomicBool::new(true),
                stopped: CancellationToken::new(),
            })
        }
        fn retire(&self) {
            self.current
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }
    #[async_trait::async_trait]
    impl callback::Executor for Generation {
        fn stopped(&self) -> CancellationToken {
            self.stopped.clone()
        }
        fn generation(&self) -> Option<ItemId> {
            Some(self.id.clone())
        }
        fn is_current(&self) -> bool {
            self.current.load(std::sync::atomic::Ordering::SeqCst)
        }
        async fn prepare(&self) -> Result<(), String> {
            Ok(())
        }
        async fn call(&self, _: &str, _: Value, _: &HookContext) -> Result<Value, String> {
            Ok(Value::Null)
        }
    }
    #[tokio::test]
    async fn retiring_names_disappear_and_late_registration_cannot_replace_new_names() {
        let root = tempfile::tempdir().unwrap();
        let host = testing::TestHost::default();
        let registry = Arc::new(Registry::default());
        host.services()
            .add(
                callback::KEY.name().into(),
                service::Service {
                    value: Arc::new(registry.clone()),
                    wire: None,
                },
            )
            .unwrap();
        let caller = Caller {
            host: host.handle(),
            ..caller(root.path())
        };
        let registrations = Registrations::default();
        let old = Generation::new();
        registry.insert("aaa".into(), old.clone());
        registrations.command(&caller, named("old", &[])).unwrap();
        old.retire();
        assert!(registrations.commands().is_empty());
        assert!(registrations.contents("aaa").commands.is_empty());
        assert!(
            !old.stopped.is_cancelled(),
            "retired callback is still leased"
        );
        let new = Generation::new();
        registry.insert("aaa".into(), new.clone());
        registrations.command(&caller, named("new", &[])).unwrap();
        generation::scope("aaa", &old.id, async {
            registrations
                .command(&caller, named("late-old", &[]))
                .unwrap();
        })
        .await;
        let commands = registrations.commands();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].spec().name, "new");
        new.retire();
        assert!(registrations.commands().is_empty());
    }
}
