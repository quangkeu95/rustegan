use std::io::{StdoutLock, Write};
use uuid::Uuid;

use crate::message::{Message, MessageId, NodeId, Payload};
use anyhow::bail;

pub struct Node {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    /// Unique message ID, we increase it when new message is generated
    pub msg_id: MessageId,
}

impl Node {
    pub fn new<N: Into<NodeId>>(node_id: N) -> Self {
        Node {
            node_id: node_id.into(),
            node_ids: Vec::new(),
            msg_id: 0,
        }
    }

    pub fn handle(&mut self, message: Message, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        match message.body.payload.clone() {
            Payload::Echo { echo } => {
                // let reply = Message {
                //     src: message.dest,
                //     dest: message.src,
                //     body: MessageBody {
                //         msg_id: Some(self.msg_id),
                //         in_reply_to: message.body.msg_id,
                //         payload: Payload::EchoOk { echo },
                //     },
                // };
                let reply = message.into_reply(self.msg_id, Payload::EchoOk { echo });

                self.reply(reply, stdout)?;
            }
            Payload::Init { node_id, node_ids } => {
                self.node_id = node_id;
                self.node_ids = node_ids;

                // let reply = Message {
                //     src: message.dest,
                //     dest: message.src,
                //     body: MessageBody {
                //         msg_id: Some(self.msg_id),
                //         in_reply_to: message.body.msg_id,
                //         payload: Payload::InitOk,
                //     },
                // };
                let reply = message.into_reply(self.msg_id, Payload::InitOk);

                self.reply(reply, stdout)?;
            }
            Payload::InitOk => bail!("receive init_ok message"),
            Payload::Generate => {
                let id = Uuid::new_v4().to_string();
                let reply = message.into_reply(self.msg_id, Payload::GenerateOk { id });

                self.reply(reply, stdout)?;
            }
            Payload::GenerateOk { id } => bail!("receive generate_ok message"),
            _ => {}
        }
        Ok(())
    }

    fn reply(&mut self, message: Message, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *stdout, &message)?;
        stdout.write_all(b"\n")?;
        self.msg_id += 1;
        Ok(())
    }
}
