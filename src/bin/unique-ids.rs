use anyhow::Context;
use rustegan::{
    message::{Event, NodeId},
    node::{Init, Node},
    *,
};
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node: NodeId,
    id: usize,
}

impl Node<(), Payload, ()> for UniqueNode {
    fn from_init(
        _state: (),
        init: Init,
        _sender: std::sync::mpsc::Sender<Event<Payload, ()>>,
    ) -> anyhow::Result<Self> {
        Ok(UniqueNode {
            node: init.node_id,
            id: 1,
        })
    }

    fn handle(&mut self, event: Event<Payload, ()>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("got injected event when there's no event injection");
        };

        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node, self.id);
                reply.body.payload = Payload::GenerateOk { guid };
                reply.send(output).context("send response to generate")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _>(())
}
