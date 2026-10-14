//! Read clipboard contents through platform tools without linking against a
//! display server. Bracketed terminal paste still works. On Windows, text
//! takes precedence over a picture and selections use the system clipboard;
//! other platforms keep their image paste and OSC 52 copy.
//!
//! Scratch files keep large values off command-line arguments and are removed
//! after each operation. No tool or empty clipboard is `None`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A clipboard tool that never answers must not hold the composer indefinitely.
const PATIENCE: Duration = Duration::from_secs(5);

pub enum Content {
    #[cfg(any(windows, test))]
    Text(String),
    Image(Vec<u8>),
}

/// Windows reads text first; other systems retain their image-only shortcut.
pub fn content() -> Option<Content> {
    #[cfg(windows)]
    {
        preferred(text, image)
    }
    #[cfg(not(windows))]
    image().map(Content::Image)
}

/// Prefer text even when a clipboard also advertises a rendered image.
#[cfg(any(windows, test))]
fn preferred(
    read_text: impl FnOnce() -> Option<String>,
    read_image: impl FnOnce() -> Option<Vec<u8>>,
) -> Option<Content> {
    match read_text().filter(|text| !text.is_empty()) {
        Some(text) => Some(Content::Text(text)),
        None => read_image()
            .filter(|bytes| !bytes.is_empty())
            .map(Content::Image),
    }
}

/// The PNG bytes on the clipboard, when there is a picture on it.
fn image() -> Option<Vec<u8>> {
    let file = scratch();
    let bytes = fetch(&file).then(|| fs::read(&file).ok()).flatten();
    let _ = fs::remove_file(&file);
    bytes.filter(|bytes| !bytes.is_empty())
}

#[cfg(windows)]
fn text() -> Option<String> {
    let file = text_scratch();
    let text = fetch_text(&file)
        .then(|| fs::read_to_string(&file).ok())
        .flatten();
    let _ = fs::remove_file(&file);
    text.filter(|text| !text.is_empty())
}

/// Tests must never replace the person's clipboard.
#[cfg(all(windows, not(test)))]
pub fn set_text(text: &str) -> bool {
    let file = text_scratch();
    let copied = fs::write(&file, text).is_ok() && write_text(&file);
    let _ = fs::remove_file(&file);
    copied
}

/// Keep OSC 52 for non-Windows platforms and for all tests.
#[cfg(any(not(windows), test))]
pub fn set_text(_text: &str) -> bool {
    false
}

#[cfg(windows)]
fn text_scratch() -> PathBuf {
    scratch().with_extension("txt")
}

fn scratch() -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("bingo-paste-{}-{unique}.png", std::process::id()))
}

/// A failed or timed-out tool is no clipboard content, never a console message.
fn bounded(mut command: Command) -> bool {
    let Ok(mut child) = command.stdin(Stdio::null()).stderr(Stdio::null()).spawn() else {
        return false;
    };
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if started.elapsed() < PATIENCE => {
                std::thread::sleep(Duration::from_millis(10));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn fetch(file: &Path) -> bool {
    // AppleScript is the one way to ask for a PNG rendering without a
    // framework; a clipboard with no picture on it errors inside the `try`
    // and leaves the file empty, which `image` reads as nothing.
    let path = file
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    let script = [
        format!("set f to open for access POSIX file \"{path}\" with write permission"),
        "try".to_string(),
        "write (the clipboard as «class PNGf») to f".to_string(),
        "end try".to_string(),
        "close access f".to_string(),
    ];
    let mut command = Command::new("osascript");
    for line in &script {
        command.arg("-e").arg(line);
    }
    command.stdout(Stdio::null());
    bounded(command)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn fetch(file: &Path) -> bool {
    // Wayland first, then X11; each tool writes the picture to its stdout.
    let attempts: [(&str, &[&str]); 2] = [
        ("wl-paste", &["-t", "image/png"]),
        (
            "xclip",
            &["-selection", "clipboard", "-t", "image/png", "-o"],
        ),
    ];
    attempts.into_iter().any(|(tool, args)| {
        let Ok(out) = fs::File::create(file) else {
            return false;
        };
        let mut command = Command::new(tool);
        command.args(args).stdout(out);
        bounded(command)
    })
}

#[cfg(windows)]
fn fetch(file: &Path) -> bool {
    // Clipboard image APIs require STA; pass the path via an environment
    // variable rather than interpolating an untrusted temporary directory.
    let script = r#"
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
try {
    $image = [System.Windows.Forms.Clipboard]::GetImage()
    if ($null -eq $image) { exit 1 }
    $image.Save($env:BINGO_CLIPBOARD_FILE, [System.Drawing.Imaging.ImageFormat]::Png)
} catch { exit 1 }
"#;
    let mut command = Command::new("powershell");
    command
        .args(["-NoProfile", "-NonInteractive", "-STA", "-Command", script])
        .env("BINGO_CLIPBOARD_FILE", file)
        .stdout(Stdio::null());
    bounded(command)
}

#[cfg(windows)]
fn fetch_text(file: &Path) -> bool {
    let script = r#"
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
try {
    if (![System.Windows.Forms.Clipboard]::ContainsText()) { exit 1 }
    $text = [System.Windows.Forms.Clipboard]::GetText()
    [IO.File]::WriteAllText($env:BINGO_CLIPBOARD_FILE, $text, [Text.UTF8Encoding]::new($false))
} catch { exit 1 }
"#;
    let mut command = Command::new("powershell");
    command
        .args(["-NoProfile", "-NonInteractive", "-STA", "-Command", script])
        .env("BINGO_CLIPBOARD_FILE", file)
        .stdout(Stdio::null());
    bounded(command)
}

#[cfg(all(windows, not(test)))]
fn write_text(file: &Path) -> bool {
    let script = r#"
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Windows.Forms
try {
    $text = [IO.File]::ReadAllText($env:BINGO_CLIPBOARD_FILE, [Text.Encoding]::UTF8)
    if ($text.Length -eq 0) { exit 1 }
    [System.Windows.Forms.Clipboard]::SetText($text)
} catch { exit 1 }
"#;
    let mut command = Command::new("powershell");
    command
        .args(["-NoProfile", "-NonInteractive", "-STA", "-Command", script])
        .env("BINGO_CLIPBOARD_FILE", file)
        .stdout(Stdio::null());
    bounded(command)
}

/// Prefer the system clipboard; only an unavailable or oversized copy goes by OSC 52.
/// The fallback's existing limit check reports oversized selections without copying them.
pub fn copy_to_clipboard(
    fallback: &mut Option<String>,
    text: String,
    set_system_clipboard: impl FnOnce(&str) -> bool,
) {
    // The existing OSC 52 cap counts base64 bytes, not source text bytes.
    if text.len().div_ceil(3).saturating_mul(4) > crate::select::LIMIT {
        *fallback = Some(text);
    } else {
        *fallback = (!set_system_clipboard(&text)).then_some(text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_copy_precedes_osc52_and_failure_falls_back() {
        let mut fallback = None;
        let mut copied = None;
        copy_to_clipboard(&mut fallback, "selected".into(), |text| {
            copied = Some(text.to_string());
            true
        });
        assert_eq!(copied.as_deref(), Some("selected"));
        assert_eq!(fallback, None);
        copy_to_clipboard(&mut fallback, "other".into(), |_| false);
        assert_eq!(fallback.as_deref(), Some("other"));
    }

    #[test]
    fn oversized_selection_never_reaches_system_clipboard() {
        let mut fallback = None;
        let text = "x".repeat(crate::select::LIMIT * 3 / 4 + 1);
        assert!(text.len() < crate::select::LIMIT);
        copy_to_clipboard(&mut fallback, text.clone(), |_| {
            panic!("system copy called")
        });
        assert_eq!(fallback, Some(text));
    }

    #[test]
    fn a_tool_that_does_not_exist_is_no_picture() {
        let mut command = Command::new("bingo-no-such-clipboard-tool");
        command.stdout(Stdio::null());
        assert!(!bounded(command));
    }

    #[test]
    fn a_scratch_file_that_was_never_written_reads_as_nothing() {
        let file = scratch();
        assert!(!file.exists());
        assert_eq!(fs::read(&file).ok().filter(|b| !b.is_empty()), None);
    }

    #[test]
    fn text_wins_when_both_formats_are_available() {
        let content = preferred(|| Some("copied text".into()), || panic!("image read"));
        assert!(matches!(content, Some(Content::Text(text)) if text == "copied text"));
    }

    #[test]
    fn image_follows_absent_or_empty_text() {
        for text in [None, Some(String::new())] {
            let content = preferred(|| text, || Some(vec![1, 2, 3]));
            assert!(matches!(content, Some(Content::Image(bytes)) if bytes == [1, 2, 3]));
        }
    }

    #[test]
    fn empty_clipboard_does_nothing() {
        assert!(preferred(|| Some(String::new()), || Some(Vec::new())).is_none());
    }
}
