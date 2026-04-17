//! Terminal spinners for slow async operations.
//!
//! On a TTY, `Spinner::start` renders a ticking spinner next to the given
//! message and replaces it with a final message on `finish`. On a non-TTY
//! (CI logs, redirects) the start message is printed as a plain line and the
//! finish message is printed on its own line, so logs still show what
//! happened without any control characters.

use std::io::IsTerminal;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

const TICK_STRINGS: &[&str] = &[
    "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏",
];

pub struct Spinner {
    pb: Option<ProgressBar>,
}

impl Spinner {
    pub fn start(message: impl Into<String>) -> Self {
        let message = message.into();
        if std::io::stderr().is_terminal() {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner:.cyan} {msg}")
                    .expect("valid spinner template")
                    .tick_strings(TICK_STRINGS),
            );
            pb.set_message(message);
            pb.enable_steady_tick(Duration::from_millis(100));
            Self { pb: Some(pb) }
        } else {
            eprintln!("{message}");
            Self { pb: None }
        }
    }

    pub fn finish(self, message: impl Into<String>) {
        let message = message.into();
        match self.pb {
            Some(pb) => pb.finish_with_message(message),
            None => eprintln!("{message}"),
        }
    }
}
