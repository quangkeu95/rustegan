use std::io::{StdoutLock, Write};

use crate::message::{Message, MessageBody, MessageId, NodeId, Payload};
use anyhow::bail;

pub struct Node {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
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
        match message.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: message.dest,
                    dest: message.src,
                    body: MessageBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                serde_json::to_writer(&mut *stdout, &reply)?;
                stdout.write_all(b"\n")?;

                self.msg_id += 1;
            }
            Payload::Init { node_id, node_ids } => {
                // let mut stderr = std::io::stderr().lock();
                // stderr.write_all(b"receive init message")?;
                self.node_id = node_id;
                self.node_ids = node_ids;

                let reply = Message {
                    src: message.dest,
                    dest: message.src,
                    body: MessageBody {
                        msg_id: Some(self.msg_id),
                        in_reply_to: message.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };

                serde_json::to_writer(&mut *stdout, &reply)?;
                stdout.write_all(b"\n")?;
                self.msg_id += 1;
            }
            Payload::InitOk => bail!("receive init_ok message"),
            _ => {}
        }
        Ok(())
    }
}
