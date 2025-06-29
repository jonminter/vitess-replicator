use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::mpsc::{Receiver, RecvError},
};

#[derive(Debug)]
#[non_exhaustive]
pub struct ConsoleStreamProducerError {
    pub kind: ConsoleStreamProducerErrorKind,
}

impl Display for ConsoleStreamProducerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error streaming outgoing events to the console",)
    }
}

impl Error for ConsoleStreamProducerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ConsoleStreamProducerErrorKind::RecvFailed(e) => Some(e),
        }
    }
}

#[derive(Debug)]
pub enum ConsoleStreamProducerErrorKind {
    RecvFailed(RecvError),
}

pub(crate) fn run_console_stream_producer(
    incoming_json: Receiver<serde_json::Value>,
) -> Result<(), ConsoleStreamProducerError> {
    loop {
        let json = incoming_json
            .recv()
            .map_err(|e| ConsoleStreamProducerError {
                kind: ConsoleStreamProducerErrorKind::RecvFailed(e),
            })?;

        log::info!("Row Json -> {}", json);
    }
}
