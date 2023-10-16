use serde::{Deserialize, Serialize};
use std::{io::StdoutLock, sync::mpsc::Sender};

use crate::message::{Event, NodeId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
}

pub trait Node<S, Payload, Command> {
    fn from_init(
        state: S,
        init: Init,
        sender: Sender<Event<Payload, Command>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn handle(
        &mut self,
        event: Event<Payload, Command>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}
