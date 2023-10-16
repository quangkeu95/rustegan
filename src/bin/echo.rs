use rustegan::{
    message::Event,
    node::{Init, Node},
    *,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};
use std::sync::mpsc::Sender;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: usize,
}

impl Node<(), Payload, ()> for EchoNode {
    fn from_init(
        _state: (),
        _init: Init,
        _sender: Sender<Event<Payload, ()>>,
    ) -> anyhow::Result<Self> {
        Ok(EchoNode { id: 1 })
    }

    fn handle(&mut self, event: Event<Payload, ()>, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("got injected event when there's no event injection");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply.send(stdout).context("send response to echo")?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _, _>(())
}
