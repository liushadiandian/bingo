//! One decision per discovered generation, claimed before it is adjudicated.
use crate::{discovery::Found, grant::grant};
use bingo_sdk::*;
use std::sync::Mutex;

#[derive(Default)]
enum State {
    #[default]
    Pending,
    Checking(PluginGrant),
    Allowed(PluginGrant),
    Refused(Option<PluginGrant>, String),
    Stopped(PluginState),
}
#[derive(Default)]
pub struct Admission {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
impl Admission {
    pub fn stop(&self, state: PluginState) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = State::Stopped(state);
        self.changed.notify_waiters();
    }
    pub fn state(&self) -> PluginState {
        match &*self.state.lock().unwrap_or_else(|e| e.into_inner()) {
            State::Pending | State::Checking(_) => PluginState::Registering,
            State::Allowed(_) => PluginState::On,
            State::Refused(_, why) => PluginState::Failed { why: why.clone() },
            State::Stopped(state) => state.clone(),
        }
    }

    pub fn refuse(&self, reason: String) {
        self.finish(Err(reason));
    }
    pub fn claim(
        &self,
        found: &Found,
        runtime: PluginRuntime,
        trust: Option<&PluginTrust>,
    ) -> Option<PluginRegistration> {
        self.claim_registration(PluginRegistration {
            name: found.name.clone(),
            version: found.manifest.version.clone(),
            execution: PluginExecution {
                origin: found.origin,
                runtime,
                grant: grant(runtime, found.origin, trust, &found.manifest.uses),
            },
        })
    }
    pub fn claim_registration(
        &self,
        registration: PluginRegistration,
    ) -> Option<PluginRegistration> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*state, State::Pending) {
            return None;
        }
        *state = State::Checking(registration.execution.grant.clone());
        Some(registration)
    }
    pub fn reset(&self) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = State::Pending;
        self.changed.notify_waiters();
    }
    pub fn decision(&self) -> Result<Option<PluginGrant>, String> {
        Self::answer(&self.state.lock().unwrap_or_else(|e| e.into_inner()))
    }
    fn answer(state: &State) -> Result<Option<PluginGrant>, String> {
        match state {
            State::Pending | State::Checking(_) | State::Stopped(_) => Ok(None),
            State::Allowed(grant) => Ok(Some(grant.clone())),
            State::Refused(_, reason) => Err(reason.clone()),
        }
    }
    /// Wait for another task's registration, but not an unclaimed admission.
    pub async fn settled(&self) -> Result<Option<PluginGrant>, String> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let ready = {
                let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                (!matches!(*state, State::Checking(_))).then(|| Self::answer(&state))
            };
            if let Some(decision) = ready {
                return decision;
            }
            changed.await;
        }
    }
    pub fn finish(&self, result: Result<PluginGrant, String>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let previous = match &*state {
            State::Checking(grant) | State::Allowed(grant) => Some(grant.clone()),
            State::Refused(grant, _) => grant.clone(),
            State::Pending | State::Stopped(_) => None,
        };
        *state = match result {
            Ok(grant) => State::Allowed(grant),
            Err(reason) => State::Refused(previous, reason),
        };
        self.changed.notify_waiters();
    }
    pub fn allowed(&self) -> bool {
        matches!(
            *self.state.lock().unwrap_or_else(|e| e.into_inner()),
            State::Allowed(_)
        )
    }
    pub fn status(&self, found: &Found, runtime: PluginRuntime, from: &str) -> PluginStatus {
        let grant = self.decision().ok().flatten();
        PluginStatus {
            id: if found.origin == PluginOrigin::Project && !found.state.is_on() {
                format!("plugins:{}", found.name)
            } else {
                found.name.clone()
            },
            version: found.manifest.version.clone(),
            state: self.state(),
            from: from.into(),
            placement: Some(PluginPlacement {
                origin: found.origin,
                runtime,
            }),
            grant,
        }
    }
}

#[cfg(test)]
mod tests;
