#![allow(clippy::unwrap_used, clippy::expect_used)]
use bingo_core::{Host, HostConfig};
use bingo_mods::ModsPlugin;
use bingo_sdk::*;
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::Notify;

const SOURCE: &str = r#"
export function register(on) {
  on('session.start', async $ => {
    await $.gate.wait({phase:'start'});
    await $.tool.register({name:'Echo',description:'Offline echo',inputSchema:{type:'object'}},
      async () => ({parts:[{type:'text',text:'nested'}],isError:false}));
    await $.command.register({name:'ledger'}, async $ => {
      await $.gate.wait({phase:'callback'});
      return {kind:'applied',message:'old'};
    });
    await $.command.register({name:'invoke-ledger',instant:true}, async $ => {
      const answer = await $.tool.call({call:{callId:'nested',name:'plugin__fixture__Echo',input:{}}});
      return {kind:'applied',message:answer.parts[0].text};
    });
    await $.gate.wait({phase:'ready'});
  });
}
"#;

async fn finish<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("callback lifecycle barrier did not complete")
}

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
    exited: Notify,
}
impl Gate {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    async fn wait(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            let _exit = Exit(&self.exited);
            self.release.notified().await;
        }
    }
}

struct Exit<'a>(&'a Notify);
impl Drop for Exit<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Default)]
struct Gates {
    source: Gate,
    start: Gate,
    callback: Gate,
    timer: Gate,
    state: Option<tokio::sync::Barrier>,
    state_stop: CancellationToken,
    ready: Notify,
    inspect_dispatch: AtomicBool,
    dispatch_pending: AtomicBool,
    dispatched: Notify,
}
#[async_trait::async_trait]
impl WireService for Gates {
    fn face(&self) -> Option<Face> {
        Some(Face::new("gate").method::<Value, Value>("wait", false))
    }
    async fn call(&self, _: &str, input: Value) -> Result<Value, ServiceError> {
        match input["phase"].as_str() {
            Some("start") => self.start.wait().await,
            Some("callback") => self.callback.wait().await,
            Some("timer") => self.timer.wait().await,
            Some("state") => {
                tokio::select! {
                    _ = self.state.as_ref().unwrap().wait() => {},
                    _ = self.state_stop.cancelled() => {},
                }
            }
            Some("ready") => self.ready.notify_one(),
            other => panic!("unknown barrier: {other:?}"),
        }
        Ok(Value::Null)
    }
}

struct Source {
    inner: Arc<dyn CommandSource>,
    gates: Arc<Gates>,
}
#[async_trait::async_trait]
impl CommandSource for Source {
    fn id(&self) -> &str {
        self.inner.id()
    }
    async fn commands(&self, cwd: &Path) -> Vec<Arc<dyn Command>> {
        let commands = self.inner.commands(cwd).await;
        // Keep the real command object resolved before a replacement begins.
        self.gates.source.wait().await;
        commands
            .into_iter()
            .map(|inner| {
                Arc::new(ObservedCommand {
                    inner,
                    gates: self.gates.clone(),
                }) as Arc<dyn Command>
            })
            .collect()
    }
}
struct ObservedCommand {
    inner: Arc<dyn Command>,
    gates: Arc<Gates>,
}
#[async_trait::async_trait]
impl Command for ObservedCommand {
    fn spec(&self) -> CommandSpec {
        self.inner.spec()
    }
    async fn run(&self, args: &str, cx: &CommandContext) -> Result<CommandOutcome, KernelError> {
        let mut running = std::pin::pin!(self.inner.run(args, cx));
        if self.gates.inspect_dispatch.swap(false, Ordering::SeqCst) {
            let first = futures::poll!(&mut running);
            self.gates
                .dispatch_pending
                .store(first.is_pending(), Ordering::SeqCst);
            self.gates.dispatched.notify_one();
            if let Poll::Ready(result) = first {
                return result;
            }
        }
        running.await
    }
}

struct Capture {
    plugin: ModsPlugin,
    gates: Arc<Gates>,
}
#[async_trait::async_trait]
impl Plugin for Capture {
    fn manifest(&self) -> &'static PluginManifest {
        self.plugin.manifest()
    }
    fn register(&self, outer: &mut Registrar) -> Result<(), PluginError> {
        let mut inner = Registrar::new(
            outer.plugin_id(),
            outer.config::<Value>()?,
            outer.env().clone(),
            outer.switched_off().clone(),
        )
        .with_sources(outer.sources().clone());
        self.plugin.register(&mut inner)?;
        outer.provider(Arc::new(Offline));
        for contribution in inner.into_contributions() {
            outer.add(match contribution {
                Contribution::Commands(inner) => Contribution::Commands(Arc::new(Source {
                    inner,
                    gates: self.gates.clone(),
                })),
                other => other,
            });
        }
        Ok(())
    }
    async fn start(&self, host: HostHandle) -> Result<(), PluginError> {
        self.plugin.start(host).await
    }
    async fn stop(&self) -> Result<(), PluginError> {
        self.plugin.stop().await
    }
}

struct Offline;
#[async_trait::async_trait]
impl Provider for Offline {
    fn id(&self) -> &str {
        "offline"
    }
    fn endpoint(&self, _: &str) -> EndpointCapabilities {
        EndpointCapabilities::default()
    }
    async fn stream(
        &self,
        _: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ProviderError> {
        let beginning = futures::stream::iter([
            Ok(ModelEvent::TextStart {
                id: "offline".into(),
            }),
            Ok(ModelEvent::TextDelta {
                id: "offline".into(),
                delta: "waiting".into(),
            }),
        ]);
        Ok(Box::pin(beginning.chain(futures::stream::pending())))
    }
}

struct Fixture {
    _home: tempfile::TempDir,
    root: PathBuf,
    source: String,
    plugin: ModsPlugin,
    host: Arc<Host>,
    attachment: Attachment,
    gates: Arc<Gates>,
}
impl Fixture {
    async fn new(driver: Driver) -> Self {
        Self::with_source(driver, SOURCE, Arc::new(Gates::default())).await
    }
    async fn with_source(driver: Driver, source: &str, gates: Arc<Gates>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("fixture");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(
            root.join("plugin.json"),
            json!({"name":"fixture","version":"1","module":"main.ts","uses":["command","tool","gate","clock","state"]}).to_string(),
        ).unwrap();
        std::fs::write(root.join("main.ts"), source).unwrap();
        let plugin = ModsPlugin::default()
            .with_dirs(vec![root.clone()])
            .with_project(home.path().into());
        let host = Host::build(
            vec![Box::new(Capture {
                plugin: plugin.clone(),
                gates: gates.clone(),
            })],
            HostConfig::new(Env::rooted(home.path()))
                .with_layer("offline", json!({"provider":"offline","model":"offline"})),
        )
        .await
        .unwrap();
        host.open_service("gate", gates.clone()).unwrap();
        let attachment = open(&host, home.path(), driver).await;
        finish(gates.ready.notified()).await;
        Self {
            _home: home,
            root,
            source: source.into(),
            plugin,
            host,
            attachment,
            gates,
        }
    }
    fn submit(&self, line: &str) -> IntentId {
        let intent = IntentId::mint();
        self.attachment
            .handle
            .submit(intent.clone(), Input::text(line, Origin::surface("rpc")));
        intent
    }
    async fn ack(&mut self, expected: &IntentId) -> IntentOutcome {
        finish(async {
            while let Some(frame) = self.attachment.events.next().await {
                if let Event::IntentAck { intent, outcome } = frame.event
                    && &intent == expected
                {
                    return outcome;
                }
            }
            panic!("command acknowledgement stream ended")
        })
        .await
    }
    async fn command(&mut self, line: &str) -> IntentOutcome {
        let intent = self.submit(line);
        self.ack(&intent).await
    }
    async fn invoke(&mut self) -> IntentOutcome {
        let intent = self.submit("/invoke-ledger");
        let interaction = finish(async {
            while let Some(frame) = self.attachment.events.next().await {
                if let Event::InteractionOpened { interaction } = frame.event {
                    return interaction;
                }
            }
            panic!("nested tool permission was never requested");
        })
        .await;
        assert!(
            matches!(interaction.kind, InteractionKind::Permission {tool, ..}
            if tool == "plugin__fixture__Echo")
        );
        self.attachment.handle.answer(
            IntentId::mint(),
            interaction.id,
            Answer::AllowOnce,
            Activation::Programmatic,
        );
        self.ack(&intent).await
    }
    async fn replacing(&self) -> tokio::task::JoinHandle<Result<(), KernelError>> {
        self.gates.start.arm();
        std::fs::write(
            self.root.join("main.ts"),
            self.source.replace("'old'", "'new'"),
        )
        .unwrap();
        let plugin = self.plugin.clone();
        let root = self.root.clone();
        let reload = tokio::spawn(async move { plugin.reload(&root).await });
        finish(self.gates.start.entered.notified()).await;
        reload
    }
    async fn dispatch_during_reload(&self) {
        self.gates.inspect_dispatch.store(true, Ordering::SeqCst);
        self.gates.source.release.notify_one();
        finish(self.gates.dispatched.notified()).await;
        assert!(
            self.gates.dispatch_pending.load(Ordering::SeqCst),
            "command failed instead of waiting for replacement startup"
        );
    }
    fn stopped(&self) -> CancellationToken {
        HostHandle(self.host.clone())
            .service(&bingo_plugin_host::callback::KEY)
            .unwrap()
            .get("fixture")
            .unwrap()
            .stopped()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.gates.state_stop.cancel();
        for gate in [
            &self.gates.source,
            &self.gates.start,
            &self.gates.callback,
            &self.gates.timer,
        ] {
            gate.armed.store(false, Ordering::SeqCst);
            gate.release.notify_one();
        }
    }
}

async fn open(host: &Host, home: &Path, driver: Driver) -> Attachment {
    host.open(
        SessionSelector::Create {
            spec: SessionSpec {
                cwd: home.into(),
                driver,
                ..Default::default()
            },
        },
        ClientIdentity {
            name: "fixture".into(),
            surface: "test".into(),
        },
        OpenOptions::default(),
    )
    .await
    .unwrap()
}
fn applied(outcome: IntentOutcome, message: &str) {
    assert_eq!(
        outcome,
        IntentOutcome::Applied {
            result: json!({"message":message})
        }
    );
}
async fn retired(stopped: CancellationToken) {
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    finish(stopped.cancelled()).await;
    tokio::time::resume();
}

#[tokio::test]
async fn a_resolved_slash_command_waits_for_replacement_startup() {
    let mut fixture = Fixture::new(Driver::Log).await;
    fixture.gates.source.arm();
    let intent = fixture.submit("/ledger");
    finish(fixture.gates.source.entered.notified()).await;
    let reload = fixture.replacing().await;
    fixture.dispatch_during_reload().await;
    fixture.gates.start.release.notify_one();
    finish(reload).await.unwrap().unwrap();
    applied(fixture.ack(&intent).await, "new");
    applied(fixture.command("/ledger").await, "new");
    fixture.host.shutdown().await;
}

#[tokio::test]
async fn an_acquired_slash_callback_finishes_on_its_original_generation() {
    let mut fixture = Fixture::new(Driver::Log).await;
    let stopped = fixture.stopped();
    fixture.gates.callback.arm();
    let intent = fixture.submit("/ledger");
    finish(fixture.gates.callback.entered.notified()).await;
    let reload = fixture.replacing().await;
    fixture.gates.start.release.notify_one();
    finish(reload).await.unwrap().unwrap();
    assert!(
        !stopped.is_cancelled(),
        "reload stopped an acquired callback"
    );
    fixture.gates.callback.release.notify_one();
    applied(fixture.ack(&intent).await, "old");
    retired(stopped).await;
    applied(fixture.command("/ledger").await, "new");
    fixture.host.shutdown().await;
}

#[tokio::test]
async fn cancellation_still_ends_a_command_waiting_for_reload() {
    let mut fixture = Fixture::new(Driver::Log).await;
    fixture.gates.source.arm();
    let intent = fixture.submit("/ledger");
    finish(fixture.gates.source.entered.notified()).await;
    let reload = fixture.replacing().await;
    fixture.dispatch_during_reload().await;
    fixture
        .attachment
        .handle
        .interrupt(IntentId::mint(), InterruptScope::Head);
    assert!(matches!(
        fixture.ack(&intent).await,
        IntentOutcome::Rejected { error } if error.code == ErrorCode::Interrupted
    ));
    assert!(
        !reload.is_finished(),
        "interrupt released the reload barrier"
    );
    fixture.gates.start.release.notify_one();
    finish(reload).await.unwrap().unwrap();
    applied(fixture.command("/ledger").await, "new");
    fixture.host.shutdown().await;
}

#[tokio::test]
async fn a_slash_callback_can_reenter_the_host_to_invoke_a_registered_tool() {
    let mut fixture = Fixture::new(Driver::default()).await;
    fixture.submit("hold the offline turn");
    finish(async {
        while let Some(frame) = fixture.attachment.events.next().await {
            if matches!(frame.event, Event::ItemDelta { .. }) {
                return;
            }
        }
        panic!("offline stream never opened");
    })
    .await;
    applied(fixture.invoke().await, "nested");
    let reload = fixture.replacing().await;
    fixture.gates.start.release.notify_one();
    finish(reload).await.unwrap().unwrap();
    applied(fixture.invoke().await, "nested");
    fixture.host.shutdown().await;
}

#[tokio::test]
async fn reload_cancels_old_timers_without_cancelling_acquired_commands() {
    let gates = Arc::new(Gates::default());
    gates.timer.arm();
    let source = SOURCE.replace("    await $.gate.wait({phase:'ready'});",
        "    await $.clock.after(0, async $ => $.gate.wait({phase:'timer'}));\n    await $.gate.wait({phase:'ready'});");
    let mut fixture = Fixture::with_source(Driver::Log, &source, gates.clone()).await;
    finish(gates.timer.entered.notified()).await;
    let stopped = fixture.stopped();
    gates.callback.arm();
    let intent = fixture.submit("/ledger");
    finish(gates.callback.entered.notified()).await;
    let reload = fixture.replacing().await;
    finish(gates.timer.exited.notified()).await;
    gates.start.release.notify_one();
    finish(reload).await.unwrap().unwrap();
    assert!(
        !stopped.is_cancelled(),
        "timer cancellation stopped the acquired command"
    );
    gates.callback.release.notify_one();
    applied(fixture.ack(&intent).await, "old");
    retired(stopped).await;
    applied(fixture.command("/ledger").await, "new");
    fixture.host.shutdown().await;
}

const SHARED_STATE: &str = r#"
async function write($, generation) {
  const held = await $.state.get('shared');
  await $.gate.wait({phase:'state'});
  let won = false;
  try { await $.state.set('shared', generation, held.version); won = true; }
  catch (error) { if (!error.message.includes('version conflict')) throw error; }
  await $.gate.wait({phase:'state'});
  await $.state.set(generation, won);
}
export function register(on) {
  const generation = 'old';
  on('session.start', async $ => {
    await $.gate.wait({phase:'start'});
    if (generation === "old") await $.state.set('shared', 'initial');
    else await write($, generation);
    await $.command.register({name:'ledger'}, async $ => {
      if (generation === "old") {
        await $.gate.wait({phase:'callback'});
        await write($, generation);
        return {kind:'applied',message:generation};
      }
      const values = await Promise.all(['shared', "old", "new"].map(key => $.state.get(key)));
      return {kind:'applied',message:JSON.stringify(values)};
    });
    await $.gate.wait({phase:'ready'});
  });
}
"#;

#[tokio::test]
async fn acquired_callback_and_replacement_startup_share_atomic_state() {
    let gates = Arc::new(Gates {
        state: Some(tokio::sync::Barrier::new(2)),
        ..Default::default()
    });
    let mut fixture = Fixture::with_source(Driver::Log, SHARED_STATE, gates.clone()).await;
    gates.callback.arm();
    let intent = fixture.submit("/ledger");
    finish(gates.callback.entered.notified()).await;
    let reload = fixture.replacing().await;
    gates.start.release.notify_one();
    gates.callback.release.notify_one();
    applied(fixture.ack(&intent).await, "old");
    finish(reload).await.unwrap().unwrap();
    let IntentOutcome::Applied { result } = fixture.command("/ledger").await else {
        panic!("replacement could not read the shared state");
    };
    let values: Vec<Value> = serde_json::from_str(result["message"].as_str().unwrap()).unwrap();
    assert_eq!(values[0]["version"], 2);
    assert_eq!(values[1]["version"], 1);
    assert_eq!(values[2]["version"], 1);
    let old_won = values[1]["value"].as_bool().unwrap();
    assert_eq!(values[2]["value"], !old_won);
    assert_eq!(values[0]["value"], if old_won { "old" } else { "new" });
    fixture.host.shutdown().await;
}

const STARTUP: &str = r#"export function register(on) {
  on('session.start', async $ => {
    for (let i = 0; i < 64; i++) await $.state.set('first', i);
    await $.command.register({name:'ready',hint:'Ready',aliases:[],args:{kind:'none'},instant:true},
      async () => ({kind:'applied'}));
  });
  on('session.*', async $ => {
    for (let i = 0; i < 64; i++) await $.state.set('last', i);
    await $.state.set('ready', true);
  });
}"#;

struct Startup {
    _home: tempfile::TempDir,
    root: PathBuf,
    environment: Arc<bingo_mods::testing::Environment>,
    modules: ModsPlugin,
    host: Arc<Host>,
    attachment: Attachment,
}
impl Startup {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("startup");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(
            root.join("plugin.json"),
            r#"{"name":"startup","version":"1","module":"main.ts","uses":["state","command"]}"#,
        )
        .unwrap();
        std::fs::write(root.join("main.ts"), STARTUP).unwrap();
        let environment = Arc::new(bingo_mods::testing::Environment::default());
        let modules = ModsPlugin::default()
            .with_dirs(vec![root.clone()])
            .with_project(home.path().into())
            .with_testing(environment.clone());
        let host = Host::build(
            vec![Box::new(modules.clone())],
            HostConfig::new(Env::rooted(home.path())),
        )
        .await
        .unwrap();
        let attachment = open(&host, home.path(), Driver::Log).await;
        finish(environment.wait_for_start(&attachment.session))
            .await
            .unwrap();
        Self {
            _home: home,
            root,
            environment,
            modules,
            host,
            attachment,
        }
    }
    async fn call(&self, method: &str, input: Value) -> Value {
        self.modules
            .testing_call("startup", method, input)
            .await
            .unwrap()
    }
    async fn ready(&mut self) {
        let intent: IntentId = serde_json::from_value(
            self.call(
                "host/command/run",
                json!({"session":self.attachment.session,"name":"ready","args":{}}),
            )
            .await,
        )
        .unwrap();
        let mut notices = Vec::new();
        let outcome = finish(async {
            while let Some(frame) = self.attachment.events.next().await {
                match &frame.event {
                    Event::Notice { text, .. } => notices.push(text.clone()),
                    Event::ItemCompleted { item } => {
                        if let ItemBody::Notice { text, .. } = &item.body {
                            notices.push(text.clone());
                        }
                    }
                    _ => (),
                }
                if let Event::IntentAck {
                    intent: seen,
                    outcome,
                } = frame.event
                    && seen == intent
                {
                    return outcome;
                }
            }
            panic!("startup command acknowledgement stream ended");
        })
        .await;
        assert!(
            matches!(outcome, IntentOutcome::Applied { .. }),
            "{outcome:?}; notices: {notices:?}"
        );
        let state = self
            .call(
                "host/state/get",
                json!({"scope":self.attachment.session,"key":"ready"}),
            )
            .await;
        assert_eq!(state["value"], true);
    }
}

#[tokio::test]
async fn testing_reload_waits_for_startup_command_registration() {
    let mut startup = Startup::new().await;
    for _ in 0..2 {
        startup
            .call(
                "host/state/set",
                json!({"scope":startup.attachment.session,"key":"ready","value":false}),
            )
            .await;
        startup.modules.reload(&startup.root).await.unwrap();
        finish(
            startup
                .environment
                .wait_for_start(&startup.attachment.session),
        )
        .await
        .unwrap();
        startup
            .host
            .session_state(&startup.attachment.session)
            .await
            .unwrap();
        startup.ready().await;
    }
    startup.host.shutdown().await;
}
