//! What a key press asks the loop to do. `on_key` is pure over `Ui`; every
//! call that reaches the kernel or the terminal leaves as one of these, so a
//! key table is a test with no runtime in it.

use bingo_sdk::{Activation, Answer, Input, IntentId, InteractionId, SessionId, SessionSelector};

use crate::graphics::picture::Source;

#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    Submit(Input),
    /// Take one of this surface's own queued lines back out of the session it
    /// waits in and into the composer (ADR-0008 §2, amended M68). The kernel
    /// answers with the line, or says it is no longer there to take.
    Withdraw(IntentId),
    Interrupt,
    Answer {
        interaction: InteractionId,
        answer: Answer,
        activation: Activation,
    },
    /// Paint another session of the attached tree; the loop fetches its
    /// mailbox the first time.
    View(SessionId),
    /// Attach to another session; the loop closes the old attachment first.
    Open(SessionSelector),
    /// Fill the session picker from the host.
    ListSessions,
    /// Fill the switcher's stored rows from the host: one read per opening.
    ListStored,
    /// Put a selection on the system clipboard, falling back to OSC 52. The loop
    /// owns both roads.
    Copy(String),
    /// Ask the clipboard for text or a picture and insert it in the composer.
    /// Images become `[image N]` tokens (ADR-0040). The loop owns the read.
    PasteClipboard,
    /// Show a picture a click landed on in whatever this system opens pictures
    /// with (M56). The loop resolves the source to a file on this machine —
    /// the path the words named, or the one it writes the bytes to — and hands
    /// that over ([`crate::viewer`]).
    OpenPicture(Source),
    Exit,
}
