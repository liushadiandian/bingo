use super::*;
use futures::{
    FutureExt,
    task::{ArcWake, waker_ref},
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

fn grant() -> PluginGrant {
    PluginGrant {
        tier: Tier::Rewrite,
        uses: Default::default(),
    }
}
fn registration() -> PluginRegistration {
    PluginRegistration {
        name: "fixture".into(),
        version: "1".into(),
        execution: PluginExecution {
            origin: PluginOrigin::Explicit,
            runtime: PluginRuntime::Module,
            grant: grant(),
        },
    }
}
fn checking() -> Admission {
    let admission = Admission::default();
    assert!(admission.claim_registration(registration()).is_some());
    admission
}

#[derive(Default)]
struct Wakes(AtomicUsize);
impl ArcWake for Wakes {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn assert_wakes_every_waiter(
    admission: &Admission,
    transition: impl FnOnce(),
    expected: Result<Option<PluginGrant>, String>,
) {
    let wakes = Arc::new(Wakes::default());
    let waker = waker_ref(&wakes);
    let mut context = Context::from_waker(&waker);
    let mut first = std::pin::pin!(admission.settled());
    let mut second = std::pin::pin!(admission.settled());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    transition();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        first.as_mut().poll(&mut context),
        Poll::Ready(expected.clone())
    );
    assert_eq!(second.as_mut().poll(&mut context), Poll::Ready(expected));
}

#[test]
fn unclaimed_admission_is_ready_but_a_claim_in_progress_waits() {
    let admission = Admission::default();
    assert_eq!(admission.settled().now_or_never(), Some(Ok(None)));
    assert!(admission.claim_registration(registration()).is_some());
    assert_eq!(admission.decision(), Ok(None));
    assert!(admission.settled().now_or_never().is_none());
    assert!(admission.claim_registration(registration()).is_none());
}

#[test]
fn allowing_or_refusing_a_claim_wakes_every_waiter() {
    for result in [Ok(grant()), Err("registration refused".into())] {
        let admission = checking();
        let expected = result.clone().map(Some);
        assert_wakes_every_waiter(&admission, || admission.finish(result), expected.clone());
        assert_eq!(admission.settled().now_or_never(), Some(expected));
    }
}

#[test]
fn stopping_a_claim_wakes_every_waiter_without_admitting_it() {
    let admission = checking();
    let stopped = PluginState::Off {
        why: "test stop".into(),
    };
    assert_wakes_every_waiter(&admission, || admission.stop(stopped.clone()), Ok(None));
    assert_eq!(admission.state(), stopped);
    assert!(!admission.allowed());
}

#[test]
fn resetting_a_claim_wakes_every_waiter_and_permits_a_fresh_claim() {
    let admission = checking();
    assert_wakes_every_waiter(&admission, || admission.reset(), Ok(None));
    assert!(admission.claim_registration(registration()).is_some());
    assert_wakes_every_waiter(
        &admission,
        || admission.finish(Ok(grant())),
        Ok(Some(grant())),
    );
}

#[test]
fn completion_before_the_waiter_is_polled_is_not_lost() {
    for result in [Ok(grant()), Err("registration refused".into())] {
        let admission = checking();
        let waiting = admission.settled();
        let expected = result.clone().map(Some);
        admission.finish(result);
        assert_eq!(waiting.now_or_never(), Some(expected));
    }
}
