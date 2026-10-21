//! Timers belong to a session and to one callback generation.
use bingo_plugin_host::callback::Executor;
use bingo_sdk::*;
use std::{sync::Arc, time::Duration};

#[derive(Default)]
pub struct Timers(std::sync::Mutex<Vec<Owned>>);
struct Owned {
    session: SessionId,
    generation: Option<ItemId>,
    task: tokio::task::JoinHandle<()>,
}
impl Timers {
    pub fn start(
        &self,
        cx: HookContext,
        executor: Arc<dyn Executor>,
        callback: String,
        period: Duration,
        every: bool,
    ) -> Result<String, String> {
        let id = IntentId::mint().to_string();
        let session = cx.session.clone();
        let stopped = executor.stopped();
        let start = tokio::time::Instant::now()
            .checked_add(period)
            .ok_or("timer deadline is out of range")?;
        let generation = executor.generation();
        let mut owned = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if !executor.is_current() || stopped.is_cancelled() {
            return Err("timer generation is no longer current".into());
        }
        owned.retain(|timer| !timer.task.is_finished());
        let task = tokio::spawn(async move {
            let mut clock = tokio::time::interval_at(start, period.max(Duration::from_millis(1)));
            clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { biased; _ = stopped.cancelled() => break, _ = clock.tick() => () }
                let result = tokio::select! {
                    biased;
                    _ = stopped.cancelled() => break,
                    result = executor.call(&callback, serde_json::Value::Null, &cx) => result,
                };
                if let Err(reason) = result {
                    let _ = cx.host.notice(Level::Warn, "MODS_TIMER", &reason).await;
                }
                if !every {
                    break;
                }
            }
        });
        owned.push(Owned {
            session,
            generation,
            task,
        });
        Ok(id)
    }
    pub async fn end(&self, session: &SessionId) {
        self.stop_matching(|timer| &timer.session == session).await;
    }
    pub async fn retire(&self, generation: &ItemId) {
        self.stop_matching(|timer| timer.generation.as_ref() == Some(generation))
            .await;
    }
    pub async fn stop(&self) {
        self.stop_matching(|_| true).await;
    }
    async fn stop_matching(&self, matches: impl Fn(&Owned) -> bool) {
        let stopped = {
            let mut owned = self.0.lock().unwrap_or_else(|error| error.into_inner());
            let (stop, keep): (Vec<_>, Vec<_>) = owned.drain(..).partition(|timer| matches(timer));
            *owned = keep;
            stop
        };
        for timer in &stopped {
            timer.task.abort();
        }
        for timer in stopped {
            let _ = timer.task.await;
        }
    }
}
impl Drop for Timers {
    fn drop(&mut self) {
        for timer in self
            .0
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .drain(..)
        {
            timer.task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    struct Counter {
        stop: CancellationToken,
        calls: tokio::sync::mpsc::UnboundedSender<SessionId>,
    }
    #[async_trait::async_trait]
    impl Executor for Counter {
        fn stopped(&self) -> CancellationToken {
            self.stop.clone()
        }
        async fn prepare(&self) -> Result<(), String> {
            Ok(())
        }
        async fn call(&self, _: &str, _: Value, cx: &HookContext) -> Result<Value, String> {
            assert!(cx.turn.is_none());
            self.calls.send(cx.session.clone()).unwrap();
            Ok(Value::Null)
        }
    }
    fn context(session: &str) -> HookContext {
        HookContext {
            session: SessionId::from_raw(session),
            turn: None,
            item: None,
            cwd: ".".into(),
            provider: None,
            model: None,
            host: testing::NoHost::handle(),
        }
    }
    async fn fired(calls: &mut tokio::sync::mpsc::UnboundedReceiver<SessionId>) -> SessionId {
        tokio::time::timeout(Duration::from_millis(1), calls.recv())
            .await
            .expect("timer did not fire")
            .unwrap()
    }
    #[tokio::test(start_paused = true)]
    async fn periods_are_exact_and_generation_end_and_session_end_cancel_timers() {
        let timers = Timers::default();
        let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let executor = Arc::new(Counter {
            stop: stop.clone(),
            calls: tx,
        });
        timers
            .start(
                context("one"),
                executor.clone(),
                "tick".into(),
                Duration::from_secs(1),
                true,
            )
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(999)).await;
        assert!(calls.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(fired(&mut calls).await, SessionId::from_raw("one"));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(fired(&mut calls).await, SessionId::from_raw("one"));
        timers.end(&SessionId::from_raw("one")).await;
        timers
            .start(
                context("two"),
                executor.clone(),
                "once".into(),
                Duration::from_secs(1),
                false,
            )
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(fired(&mut calls).await, SessionId::from_raw("two"));
        timers
            .start(
                context("three"),
                executor,
                "tick".into(),
                Duration::from_secs(1),
                true,
            )
            .unwrap();
        tokio::task::yield_now().await;
        stop.cancel();
        tokio::time::advance(Duration::from_secs(10)).await;
        timers.stop().await;
        assert!(calls.try_recv().is_err());
    }
}
