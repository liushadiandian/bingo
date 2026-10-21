use super::*;
use serde_json::json;

async fn fixture() -> (tempfile::TempDir, Arc<Manager>, Arc<Module>) {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("fixture");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(
        directory.join("plugin.json"),
        r#"{"name":"fixture","version":"1","module":"main.ts","uses":[]}"#,
    )
    .unwrap();
    std::fs::write(
        directory.join("main.ts"),
        "export function register(on) { on('session.start', () => {}); }",
    )
    .unwrap();
    let found = discovery::read(&directory, &Notices::default()).unwrap();
    let env = Env::rooted(root.path());
    let module = Module::load(found, json!({}), Limits::default(), &env, false).await;
    assert!(module.engine.is_some());
    module.admission.finish(Ok(PluginGrant {
        tier: Tier::Rewrite,
        uses: Default::default(),
    }));
    let manager = Arc::new(Manager::new(
        env,
        vec![],
        Default::default(),
        Default::default(),
        Limits::default(),
    ));
    manager.modules.write().unwrap().push(module.clone());
    (root, manager, module)
}

#[tokio::test]
async fn trust_granted_tool_authority_pins_even_before_lazy_registration() {
    let (_root, manager, module) = fixture().await;
    module.admission.finish(Ok(PluginGrant {
        tier: Tier::Rewrite,
        uses: ["tool".into()].into(),
    }));
    assert_eq!(module_kind(&manager, "fixture"), next("tools"));
    module.close().await;
}

#[tokio::test]
async fn cancelling_the_switch_waiter_does_not_lose_the_retiring_generation() {
    let (_root, manager, old) = fixture().await;
    let host = bingo_sdk::testing::TestHost::default();
    manager.host.set(host.handle()).unwrap();
    manager.this.set(Arc::downgrade(&manager)).unwrap();
    let retired = manager.switches.retired.lock().await;
    let caller = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.switch("fixture", false).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while manager.reloading.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    drop(retired);
    let _completed = manager.reloading.lock().await;
    assert!(manager.modules()[0].engine.is_none());
    assert!(
        manager
            .switches
            .retired
            .lock()
            .await
            .iter()
            .any(|entry| Arc::ptr_eq(entry, &old))
    );
    old.close().await;
}

#[tokio::test]
async fn retirement_does_not_hold_the_list_while_awaiting_host_cleanup() {
    let (_root, manager, module) = fixture().await;
    manager.modules.write().unwrap().clear();
    manager.switches.retired.lock().await.push(module);
    let host = bingo_sdk::testing::TestHost::default();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    host.on_sessions({
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
    manager.host.set(host.handle()).unwrap();
    let cleanup = {
        let manager = manager.clone();
        tokio::spawn(async move { reap(&manager).await })
    };
    entered.notified().await;
    let available = manager.switches.retired.try_lock().is_ok();
    release.notify_one();
    cleanup.await.unwrap();
    assert!(available, "retired list locked across host I/O");
}

#[tokio::test]
async fn enabling_a_held_generation_replies_without_holding_other_switches() {
    let (_root, manager, old) = fixture().await;
    let host = bingo_sdk::testing::TestHost::default();
    host.on_catalog(|kind| async move {
        Ok(Catalog {
            kind,
            entries: vec![],
        })
    });
    manager.host.set(host.handle()).unwrap();
    manager.this.set(Arc::downgrade(&manager)).unwrap();
    manager.switch("fixture", false).await.unwrap();
    tokio::time::pause();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(6),
        manager.switch("fixture", true),
    )
    .await;
    assert!(result.is_ok(), "enable held the command indefinitely");
    let result = result.unwrap().unwrap();
    assert!(format!("{result:?}").contains("AfterTurn"));
    assert!(matches!(
        manager.plugins().await[0].state,
        PluginState::Pending { now: false, .. }
    ));
    manager.switch("fixture", false).await.unwrap();
    drop(old);
    reap(&manager).await;
    // The later wish must win when the queued enable can finally proceed.
    finish_workers(&manager).await;
    assert!(manager.modules()[0].engine.is_none());
}

fn runtime(manager: &Arc<Manager>) -> Arc<crate::dollar::Runtime> {
    let host = bingo_sdk::testing::TestHost::default();
    host.on_catalog(|kind| async move {
        Ok(Catalog {
            kind,
            entries: vec![],
        })
    });
    let runtime = crate::dollar::Runtime::install(
        &host.handle(),
        manager.env.home.clone(),
        &Env::rooted(manager.env.home.clone()),
        std::time::Duration::from_secs(1),
        None,
    )
    .unwrap();
    manager.host.set(host.handle()).unwrap();
    manager.this.set(Arc::downgrade(manager)).unwrap();
    assert!(manager.dollar.set(runtime.clone()).is_ok());
    runtime
}
async fn process(runtime: &crate::dollar::Runtime, module: &Module) -> (String, SessionId) {
    let session = SessionId::mint();
    let input = serde_json::from_value(json!({"session":session,"argv":[
        bingo_process::shell::resolve().unwrap().path(), "-c", "cat"]}))
    .unwrap();
    let id = generation::scope("fixture", &module.generation, async {
        runtime
            .processes_for_test()
            .spawn(
                "fixture",
                &input,
                &module.found.root,
                Default::default(),
                module.engine.as_ref().unwrap().stopped(),
            )
            .unwrap()
    })
    .await;
    (id, session)
}

#[tokio::test]
async fn reloading_an_off_module_preserves_the_retired_generations_process() {
    for rescan in [false, true] {
        let (_root, manager, old) = fixture().await;
        let runtime = runtime(&manager);
        let (id, session) = process(&runtime, &old).await;
        manager.switch("fixture", false).await.unwrap();
        if rescan {
            std::fs::write(
                old.found.root.join("main.ts"),
                "export function register(on) {} // changed",
            )
            .unwrap();
            manager.rescan(true).await.unwrap();
        } else {
            manager.reload_root(&old.found.root).await.unwrap();
        }
        let alive = generation::scope(
            "fixture",
            &old.generation,
            runtime.processes_for_test().write(
                "fixture",
                &session,
                &id,
                Some("still alive\n".into()),
            ),
        )
        .await;
        drop(old);
        reap(&manager).await;
        assert!(
            alive.is_ok(),
            "reload destroyed a running generation: {alive:?}"
        );
    }
}

#[tokio::test]
async fn retirement_closes_the_engine_and_reaps_its_process() {
    let (_root, manager, old) = fixture().await;
    let runtime = runtime(&manager);
    let (id, session) = process(&runtime, &old).await;
    let stopped = old.engine.as_ref().unwrap().stopped();
    let identity = old.generation.clone();
    let mut output = Box::pin(generation::scope(
        "fixture",
        &identity,
        runtime.processes_for_test().read("fixture", &session, &id),
    ));
    assert!(futures::poll!(&mut output).is_pending());
    manager.switch("fixture", false).await.unwrap();
    assert!(!stopped.is_cancelled());
    drop(old);
    reap(&manager).await;
    assert!(stopped.is_cancelled(), "retired engine was not closed");
    let output = tokio::time::timeout(std::time::Duration::from_secs(10), output)
        .await
        .expect("retired process kept its output pipes open")
        .unwrap();
    assert!(output.stdout.is_empty());
    assert!(manager.switches.retired.lock().await.is_empty());
    assert!(
        runtime
            .processes_for_test()
            .write("fixture", &session, &id, Some("late".into()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn next_start_enable_does_not_change_reload_state() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.switch("fixture", false).await.unwrap();
    let root = old.found.root.clone();
    drop(old);
    reap(&manager).await;
    std::fs::write(
        root.join("main.ts"),
        "export function register(on) { on('prompt.compose', async ($, e, next) => next(e)); }",
    )
    .unwrap();
    assert!(matches!(
        manager.switch("fixture", true).await.unwrap(),
        PluginSwitch::NextStart { .. }
    ));
    assert!(
        manager.switches.off.read().unwrap().contains("fixture"),
        "rejected enable changed reload state"
    );
    manager.reload_root(&root).await.unwrap();
    assert!(manager.modules()[0].engine.is_none());
    manager.stop().await;
}

#[tokio::test]
async fn failed_enable_does_not_change_reload_state() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.switch("fixture", false).await.unwrap();
    drop(old);
    reap(&manager).await;
    let guard = manager.reloading.lock().await;
    let mut enable = Box::pin(manager.switch("fixture", true));
    assert!(futures::poll!(&mut enable).is_pending());
    manager.modules.write().unwrap().clear();
    drop(guard);
    assert!(enable.await.is_err());
    assert!(
        manager.switches.off.read().unwrap().contains("fixture"),
        "failed enable changed reload state"
    );
    manager.stop().await;
}

#[tokio::test]
async fn queued_enable_cannot_reopen_engines_after_stop() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.switch("fixture", false).await.unwrap();
    tokio::time::pause();
    assert_eq!(
        manager.switch("fixture", true).await.unwrap(),
        PluginSwitch::AfterTurn
    );
    manager.stop().await;
    drop(old);
    assert!(
        manager.switch("fixture", true).await.is_err(),
        "stopped manager accepted enable"
    );
    assert!(manager.modules().iter().all(|module| {
        module
            .engine
            .as_ref()
            .is_none_or(|engine| engine.stopped().is_cancelled())
    }));
}

async fn finish_workers(manager: &Manager) {
    let workers = std::mem::take(&mut *manager.tasks.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
}
#[tokio::test]
async fn deferred_switch_notifies_after_its_worker_finishes() {
    let (_root, manager, old) = fixture().await;
    let host = bingo_sdk::testing::TestHost::default();
    host.on_catalog(|kind| async move {
        Ok(Catalog {
            kind,
            entries: vec![],
        })
    });
    manager.host.set(host.handle()).unwrap();
    manager.this.set(Arc::downgrade(&manager)).unwrap();
    manager.switch("fixture", false).await.unwrap();
    finish_workers(&manager).await;
    let before = format!("{:?}", host.calls())
        .matches("CatalogChanged")
        .count();
    tokio::time::pause();
    assert_eq!(
        manager.switch("fixture", true).await.unwrap(),
        PluginSwitch::AfterTurn
    );
    drop(old);
    reap(&manager).await;
    finish_workers(&manager).await;
    assert!(manager.modules()[0].engine.is_some());
    let after = format!("{:?}", host.calls())
        .matches("CatalogChanged")
        .count();
    assert_eq!(
        after,
        before + 3,
        "background completion did not notify catalogues"
    );
    assert!(host.calls().iter().any(|call| matches!(
        call,
        bingo_sdk::testing::HostCall::CatalogChanged {
            kind: CatalogKind::Tools
        }
    )));
    manager.stop().await;
}

#[tokio::test]
async fn periodic_watch_reaps_a_released_generation() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    let stopped = old.engine.as_ref().unwrap().stopped();
    manager.switch("fixture", false).await.unwrap();
    assert!(!stopped.is_cancelled());
    drop(old);
    tokio::time::pause();
    manager.watch();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(3), stopped.cancelled())
        .await
        .is_ok();
    manager.stop().await;
    assert!(
        closed,
        "periodic watcher did not retire the released generation"
    );
}

#[tokio::test]
async fn retiring_an_old_generation_preserves_the_new_generations_process() {
    let (_root, manager, old) = fixture().await;
    let runtime = runtime(&manager);
    let new = Module::load(
        old.found.clone(),
        json!({}),
        Limits::default(),
        &manager.env,
        false,
    )
    .await;
    manager.publish_callbacks(old.clone());
    manager.publish_callbacks(new.clone());
    let old_id = old.generation.clone();
    manager.modules.write().unwrap()[0] = new.clone();
    manager.switches.retired.lock().await.push(old);
    let (id, session) = process(&runtime, &new).await;
    reap(&manager).await;
    let alive = generation::scope(
        "fixture",
        &new.generation,
        runtime.processes_for_test().write(
            "fixture",
            &session,
            &id,
            Some("new generation\n".into()),
        ),
    )
    .await;
    assert_eq!(
        manager.callbacks.get("fixture").unwrap().generation(),
        Some(new.generation.clone())
    );
    let old_callback = generation::scope("fixture", &old_id, async {
        manager.callbacks.get("fixture")
    })
    .await;
    assert!(old_callback.is_err());
    let foreign = generation::scope(
        "fixture",
        &old_id,
        runtime
            .processes_for_test()
            .write("fixture", &session, &id, Some("foreign".into())),
    )
    .await;
    assert!(foreign.is_err(), "old generation accessed the new handle");
    manager.stop().await;
    assert!(
        alive.is_ok(),
        "old retirement destroyed the new process: {alive:?}"
    );
}

#[tokio::test]
async fn retired_callbacks_require_their_generations_scope() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.publish_callbacks(old.clone());
    assert!(manager.callbacks.get("fixture").is_ok());
    manager.switch("fixture", false).await.unwrap();
    let ordinary = manager.callbacks.get("fixture");
    let captured = generation::scope("fixture", &old.generation, async {
        manager.callbacks.get("fixture")
    })
    .await;
    manager.stop().await;
    assert!(ordinary.is_err(), "retired callback accepted new dispatch");
    assert!(captured.is_ok(), "captured generation lost its callback");
}

#[tokio::test]
async fn failed_publication_keeps_the_committed_switch_on_reload() {
    let (_root, manager, old) = fixture().await;
    let host = bingo_sdk::testing::TestHost::default();
    host.on_catalog(|_| async { Err(reload::failed("catalog unavailable")) });
    manager.host.set(host.handle()).unwrap();
    manager.this.set(Arc::downgrade(&manager)).unwrap();
    assert!(manager.switch("fixture", false).await.is_err());
    assert!(manager.modules()[0].engine.is_none());
    assert!(manager.switches.off.read().unwrap().contains("fixture"));
    host.on_catalog(|kind| async move {
        Ok(Catalog {
            kind,
            entries: vec![],
        })
    });
    manager.reload_root(&old.found.root).await.unwrap();
    assert!(manager.modules()[0].engine.is_none());
    manager.stop().await;
}

#[tokio::test]
async fn retirement_wakes_waiters_without_a_polling_tick() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.switch("fixture", false).await.unwrap();
    let request = begin(&manager, "fixture", true);
    tokio::time::pause();
    let mut waiting = Box::pin(wait_retired(&manager, "fixture", "fixture", &request));
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(old);
    reap(&manager).await;
    assert!(
        futures::poll!(&mut waiting).is_ready(),
        "reap did not wake the switch"
    );
    manager.stop().await;
}

#[tokio::test]
async fn held_retirement_expires_to_next_start() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    manager.switch("fixture", false).await.unwrap();
    tokio::time::pause();
    assert_eq!(
        manager.switch("fixture", true).await.unwrap(),
        PluginSwitch::AfterTurn
    );
    let finished =
        tokio::time::timeout(std::time::Duration::from_secs(31), finish_workers(&manager)).await;
    assert!(finished.is_ok(), "retirement worker never expires");
    assert!(manager.modules()[0].engine.is_none());
    assert!(manager.switches.off.read().unwrap().contains("fixture"));
    assert!(manager.switches.requests.lock().unwrap().is_empty());
    drop(old);
    manager.stop().await;
}

#[tokio::test]
async fn a_new_switch_prunes_finished_workers_and_keeps_live_ones() {
    let (_root, manager, old) = fixture().await;
    runtime(&manager);
    let mut done = tokio::spawn(async {});
    (&mut done).await.unwrap();
    manager.tasks.lock().unwrap().push(done);
    let (release, waiting) = tokio::sync::oneshot::channel::<()>();
    manager.tasks.lock().unwrap().push(tokio::spawn(async {
        let _ = waiting.await;
    }));
    manager.switch("fixture", false).await.unwrap();
    assert_eq!(
        manager.tasks.lock().unwrap().len(),
        2,
        "finished worker retained"
    );
    release.send(()).unwrap();
    drop(old);
    manager.stop().await;
}
