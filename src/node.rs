use std::{
    collections::HashMap,
    io::{StdoutLock, Write},
};
use uuid::Uuid;

use crate::message::{BroadcastMessage, Message, MessageBody, MessageId, NodeId, Payload};
use anyhow::{anyhow, bail};

pub struct Node {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub topology: HashMap<NodeId, Vec<NodeId>>,
    pub neighbours: Vec<NodeId>,
    /// Unique message ID, we increase it when new message is generated
    pub msg_id: MessageId,
    /// Broadcast received messages
    pub broadcast_msgs: Vec<BroadcastMessage>,
}

impl Node {
    pub fn new<N: Into<NodeId>>(node_id: N) -> Self {
        Node {
            node_id: node_id.into(),
            node_ids: Vec::new(),
            topology: HashMap::new(),
            neighbours: Vec::new(),
            msg_id: 0,
            broadcast_msgs: Vec::new(),
        }
    }

    pub fn handle(&mut self, message: Message, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        match message.body.payload.clone() {
            Payload::Echo { echo } => {
                let reply = message.into_reply(self.msg_id, Payload::EchoOk { echo });

                self.reply(reply, stdout)?;
            }
            Payload::Init { node_id, node_ids } => {
                self.node_id = node_id;
                self.node_ids = node_ids;

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
            Payload::Broadcast {
                message: broadcast_message,
            } => {
                if !self.received_broadcast_message(broadcast_message) {
                    self.save_broadcast_message(broadcast_message);

                    // forward broadcast message to all neighbours
                    let neighbours = self.get_neighbours();

                    for neighbour in neighbours {
                        self.send(
                            &neighbour,
                            Payload::Broadcast {
                                message: broadcast_message,
                            },
                            stdout,
                        )?;
                    }

                    let reply = message.into_reply(self.msg_id, Payload::BroadcastOk);
                    self.reply(reply, stdout)?;
                }
            }
            Payload::Read => {
                let reply = message.into_reply(
                    self.msg_id,
                    Payload::ReadOk {
                        messages: self.broadcast_msgs.clone(),
                    },
                );
                self.reply(reply, stdout)?;
            }
            Payload::Topology { topology } => {
                self.save_topology(topology);
                let reply = message.into_reply(self.msg_id, Payload::TopologyOk);
                self.reply(reply, stdout)?;
            }
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

    fn send(
        &mut self,
        to: &NodeId,
        payload: Payload,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let msg = Message {
            src: self.node_id.clone(),
            dest: to.clone(),
            body: MessageBody {
                msg_id: Some(self.msg_id),
                in_reply_to: None,
                payload,
            },
        };

        self.reply(msg, stdout)
    }

    fn save_broadcast_message(&mut self, message: BroadcastMessage) {
        self.broadcast_msgs.push(message);
    }

    fn save_topology(&mut self, topology: HashMap<NodeId, Vec<NodeId>>) {
        self.topology.extend(topology);

        if let Some(neighbours) = self.topology.get(&self.node_id) {
            self.neighbours = neighbours.iter().cloned().collect();
        }
    }

    fn get_neighbours(&self) -> Vec<NodeId> {
        self.neighbours.clone()
    }

    fn received_broadcast_message(&self, message: BroadcastMessage) -> bool {
        self.broadcast_msgs.contains(&message)
    }
}
