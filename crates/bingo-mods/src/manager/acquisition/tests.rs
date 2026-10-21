use super::*;
use bingo_plugin_host::callback::Executor;

struct Fixture {
    _home: tempfile::TempDir,
    manager: Arc<Manager>,
    host: testing::TestHost,
    root: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().join("fixture");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(
            root.join("plugin.json"),
            r#"{"name":"fixture","version":"1","module":"main.ts","uses":[]}"#,
        )
        .unwrap();
        std::fs::write(root.join("main.ts"), "export function register(on) {}").unwrap();
        let manager = Arc::new(Manager::new(
            Env::rooted(home.path()),
            vec![root.clone()],
            Default::default(),
            Default::default(),
            Limits::default(),
        ));
        manager.this.set(Arc::downgrade(&manager)).unwrap();
        let host = testing::TestHost::default();
        host.on_catalog({
            let manager = Arc::downgrade(&manager);
            move |kind| {
                let manager = manager.upgrade().unwrap();
                async move {
                    for module in manager.modules() {
                        if module.engine.is_some() {
                            module.admission.finish(Ok(PluginGrant {
                                tier: Tier::Rewrite,
                                uses: Default::default(),
                            }));
                        }
                    }
                    Ok(Catalog {
                        kind,
                        entries: vec![],
                    })
                }
            }
        });
        manager.host.set(host.handle()).unwrap();
        manager.rescan(true).await.unwrap();
        Self {
            _home: home,
            manager,
            host,
            root,
        }
    }
    fn hold_registration(&self) -> tokio::sync::mpsc::UnboundedReceiver<Arc<Module>> {
        let (claimed, claims) = tokio::sync::mpsc::unbounded_channel();
        self.host.on_catalog({
            let manager = Arc::downgrade(&self.manager);
            move |kind| {
                let manager = manager.upgrade().unwrap();
                let claimed = claimed.clone();
                async move {
                    for module in manager.modules() {
                        if module
                            .admission
                            .claim(&module.found, PluginRuntime::Module, None)
                            .is_some()
                        {
                            // A concurrent catalogue owns this registration. This
                            // read must not mistake its pending claim for admission.
                            claimed.send(module).unwrap();
                        }
                    }
                    Ok(Catalog {
                        kind,
                        entries: vec![],
                    })
                }
            }
        });
        claims
    }
    fn edit(&self) {
        std::fs::write(
            self.root.join("main.ts"),
            "export function register(on) {} // new",
        )
        .unwrap();
    }
    async fn acquired(&self) -> Arc<dyn Executor> {
        self.manager.callbacks.prepared("fixture").await.unwrap()
    }
}

#[tokio::test]
async fn acquisition_waits_for_reload_without_a_live_executor() {
    let fixture = Fixture::new().await;
    let old = fixture
        .manager
        .callbacks
        .get("fixture")
        .unwrap()
        .generation();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    fixture.host.on_sessions({
        let entered = entered.clone();
        let release = release.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                Ok(vec![])
            }
        }
    });
    fixture.edit();
    let reload = {
        let manager = fixture.manager.clone();
        tokio::spawn(async move { manager.rescan(true).await })
    };
    entered.notified().await;
    assert!(fixture.manager.callbacks.get("fixture").is_err());
    let mut acquisition = Box::pin(fixture.manager.callbacks.prepared("fixture"));
    assert!(
        futures::poll!(&mut acquisition).is_pending(),
        "reload gap failed acquisition"
    );
    fixture.host.on_sessions(|_| async { Ok(vec![]) });
    release.notify_one();
    reload.await.unwrap().unwrap();
    let acquired = acquisition.await.unwrap();
    assert_ne!(acquired.generation(), old);
    assert!(!acquired.stopped().is_cancelled());
    drop(acquired);
    fixture.manager.stop().await;
}

#[tokio::test]
async fn acquired_generation_survives_reload_until_its_handle_is_dropped() {
    let fixture = Fixture::new().await;
    let acquired = fixture.acquired().await;
    let old = acquired.generation();
    let stopped = acquired.stopped();
    fixture.edit();
    fixture.manager.rescan(true).await.unwrap();
    assert!(
        !stopped.is_cancelled(),
        "reload closed an acquired callback"
    );
    assert_ne!(fixture.acquired().await.generation(), old);
    drop(acquired);
    switching::reap(&fixture.manager).await;
    assert!(
        stopped.is_cancelled(),
        "released callback leaked its generation"
    );
    fixture.manager.stop().await;
}

#[tokio::test]
async fn scoped_acquisition_keeps_its_owner_and_nested_dispatch_does_not_wait() {
    let fixture = Fixture::new().await;
    let acquired = fixture.acquired().await;
    let old = acquired.generation().unwrap();
    fixture.edit();
    fixture.manager.rescan(true).await.unwrap();
    let guard = fixture.manager.reloading.lock().await;
    let retained = generation::scope("fixture", &old, fixture.acquired()).await;
    assert_eq!(retained.generation(), Some(old));
    let nested = bingo_sdk::invocation::from("other", 0, async {
        let manager = fixture.manager.clone();
        tokio::spawn(bingo_sdk::invocation::carry(async move {
            manager.callbacks.prepared("fixture").await
        }))
        .await
    });
    let new = tokio::time::timeout(std::time::Duration::from_secs(5), nested)
        .await
        .expect("nested callback waited on the reload it serves")
        .unwrap()
        .unwrap();
    assert_ne!(new.generation(), acquired.generation());
    drop(guard);
    drop((acquired, retained, new));
    fixture.manager.stop().await;
}

#[tokio::test]
async fn failed_or_removed_replacement_cannot_dispatch_into_a_leased_retiree() {
    for remove in [false, true] {
        let fixture = Fixture::new().await;
        let acquired = fixture.acquired().await;
        let stopped = acquired.stopped();
        if remove {
            std::fs::remove_file(fixture.root.join("plugin.json")).unwrap();
        } else {
            std::fs::write(fixture.root.join("main.ts"), "invalid module syntax !").unwrap();
        }
        let report = fixture.manager.rescan(true).await.unwrap();
        assert!(remove || !report.errors.is_empty());
        assert!(!stopped.is_cancelled());
        assert!(fixture.manager.callbacks.prepared("fixture").await.is_err());
        drop(acquired);
        switching::reap(&fixture.manager).await;
        assert!(stopped.is_cancelled());
        fixture.manager.stop().await;
    }
}

#[tokio::test]
async fn cancelling_a_reload_waiter_does_not_abandon_publication() {
    for mode in ["acquire", "rescan", "root"] {
        let fixture = Fixture::new().await;
        let old = fixture
            .manager
            .callbacks
            .get("fixture")
            .unwrap()
            .generation();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        fixture.host.on_sessions({
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(vec![])
                }
            }
        });
        fixture.edit();
        let caller = {
            let manager = fixture.manager.clone();
            let root = fixture.root.clone();
            tokio::spawn(async move {
                match mode {
                    "acquire" => {
                        manager.callbacks.prepared("fixture").await.unwrap();
                    }
                    "rescan" => {
                        manager.reload().await.unwrap();
                    }
                    _ => {
                        manager.reload_root(&root).await.unwrap();
                    }
                }
            })
        };
        entered.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        fixture.host.on_sessions(|_| async { Ok(vec![]) });
        release.notify_one();
        {
            let _finished = fixture.manager.reloading.lock().await;
            let acquired = fixture.manager.callbacks.get("fixture").unwrap();
            assert_ne!(
                acquired.generation(),
                old,
                "{mode}: replacement not published"
            );
            assert!(!acquired.stopped().is_cancelled());
        }
        fixture.manager.stop().await;
    }
}

#[tokio::test]
async fn reload_waits_for_another_tasks_registration_of_the_replacement() {
    let fixture = Fixture::new().await;
    let _old = fixture.manager.modules()[0].clone();
    let mut claims = fixture.hold_registration();
    fixture.edit();
    let mut reload = Box::pin(fixture.manager.rescan(true));
    let replacement = tokio::select! {
        biased;
        result = &mut reload => panic!("reload returned before admission: {result:?}"),
        module = claims.recv() => module.unwrap(),
    };
    assert!(futures::poll!(&mut reload).is_pending());
    assert!(!replacement.admission.allowed());
    fixture
        .manager
        .registered(
            "fixture",
            Ok(PluginGrant {
                tier: Tier::Rewrite,
                uses: Default::default(),
            }),
        )
        .await;
    reload.await.unwrap();
    assert!(replacement.admission.allowed());
    fixture.manager.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_abandoned_registration_cannot_hold_reload_or_shutdown_forever() {
    for stop in [false, true] {
        let fixture = Fixture::new().await;
        let _old = fixture.manager.modules()[0].clone();
        let mut claims = fixture.hold_registration();
        fixture.edit();
        let mut reload = Box::pin(fixture.manager.rescan(true));
        let _replacement = tokio::select! {
            biased;
            result = &mut reload => panic!("reload returned before admission: {result:?}"),
            module = claims.recv() => module.unwrap(),
        };
        assert!(futures::poll!(&mut reload).is_pending());
        if stop {
            fixture.manager.stop.cancel();
        } else {
            tokio::time::advance(HOOK_DEADLINE).await;
        }
        let report = reload.await.unwrap();
        let expected = if stop {
            "module manager stopped"
        } else {
            "registration deadline exceeded"
        };
        assert!(
            report.errors.iter().any(|error| error.contains(expected)),
            "{report:?}"
        );
        assert!(fixture.manager.reloading.try_lock().is_ok());
        fixture.manager.stop().await;
    }
}

#[tokio::test]
async fn retired_generations_cannot_forget_replacement_subscriptions() {
    for mode in ["rescan", "root", "remove-readd"] {
        let fixture = Fixture::new().await;
        fixture.host.on_sessions(|_| async { Ok(vec![]) });
        let old = fixture.manager.modules()[0].clone();
        let session = SessionId::mint();
        fixture.edit();
        match mode {
            "root" => fixture.manager.reload_root(&fixture.root).await.unwrap(),
            "remove-readd" => {
                let manifest = fixture.root.join("plugin.json");
                let source = std::fs::read(&manifest).unwrap();
                std::fs::remove_file(&manifest).unwrap();
                fixture.manager.rescan(true).await.unwrap();
                std::fs::write(&manifest, source).unwrap();
                fixture.manager.rescan(true).await.unwrap();
            }
            _ => {
                fixture.manager.rescan(true).await.unwrap();
            }
        }
        let replacement = fixture.manager.modules()[0].clone();
        assert_ne!(old.generation, replacement.generation);
        assert!(!old.engine.as_ref().unwrap().stopped().is_cancelled());
        replacement
            .store
            .subscribe(
                &fixture.host.handle(),
                session.clone(),
                "pane".into(),
                vec![bingo_plugin_host::state::Read {
                    plugin: "fixture".into(),
                    scope: "global".into(),
                    key: "count".into(),
                    version: None,
                }],
            )
            .await
            .unwrap();
        old.store.forget(&session, "pane");
        replacement
            .store
            .changed(&fixture.host.handle(), "global", "count")
            .await
            .unwrap();
        assert!(fixture.host.calls().iter().any(|call| matches!(call,
            testing::HostCall::Invalidate { session: seen, requests } if seen == &session && requests == &["pane"]
        )), "{mode}: retired generation forgot the replacement's subscription");
        fixture.manager.stop().await;
    }
}
