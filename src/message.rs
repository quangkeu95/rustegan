use std::io::{StdoutLock, Write};

use derive_more::{Display, From};
use serde::{Deserialize, Serialize};

pub enum Event<Payload, Command> {
    Message(Message<Payload>),
    Command(Command),
    EOF,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message<Payload> {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: MessageBody<Payload>,
}

impl<Payload> Message<Payload>
where
    Payload: Serialize,
{
    pub fn into_reply(self, msg_id: Option<&mut MessageId>) -> Self {
        Self {
            src: self.dest,
            dest: self.src,
            body: MessageBody {
                msg_id: msg_id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.msg_id,
                payload: self.body.payload,
            },
        }
    }

    pub fn send(self, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *stdout, &self)?;
        stdout.write_all(b"\n")?;
        Ok(())
    }
}

#[derive(Debug, Display, Deserialize, Serialize, Clone, From, Eq, PartialEq, Hash)]
pub struct NodeId(String);

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageBody<Payload> {
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    pub payload: Payload,
}

pub type MessageId = usize;
