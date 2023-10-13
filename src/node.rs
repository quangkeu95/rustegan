use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
    sync::mpsc::Sender,
    time::Duration,
};
use uuid::Uuid;

use crate::message::{
    self, BroadcastMessage, Command, Event, Message, MessageBody, MessageId, NodeId, Payload,
};
use anyhow::{anyhow, bail};

pub struct Node {
    pub node_id: NodeId,
    pub node_ids: Vec<NodeId>,
    pub topology: HashMap<NodeId, Vec<NodeId>>,
    pub neighbours: Vec<NodeId>,
    /// Unique message ID, we increase it when new message is generated
    pub msg_id: MessageId,
    /// Broadcast received messages
    pub broadcast_msgs: HashSet<BroadcastMessage>,
    /// Mapping for NodeId to known broadcast messages
    pub known: HashMap<NodeId, HashSet<BroadcastMessage>>,
}

impl Node {
    pub fn new(node_id: NodeId, node_ids: Vec<NodeId>, tx: Sender<Event>) -> Self {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = tx.send(Event::Command(Command::Gossip)) {
                break;
            }
        });
        Node {
            node_id,
            node_ids: node_ids.clone(),
            topology: HashMap::new(),
            neighbours: Vec::new(),
            msg_id: 0,
            broadcast_msgs: HashSet::new(),
            known: node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
        }
    }

    pub fn handle_init(
        &mut self,
        src: NodeId,
        dest: NodeId,
        in_reply_to: Option<MessageId>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let reply = Message {
            src: dest,
            dest: src,
            body: MessageBody {
                msg_id: Some(self.msg_id),
                in_reply_to,
                payload: Payload::InitOk,
            },
        };

        self.send(reply, stdout)
    }

    pub fn handle(&mut self, event: Event, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        match event {
            Event::EOF => {}
            Event::Command(command) => match command {
                Command::Gossip => {
                    let neighbours = self.get_neighbours();

                    for n in neighbours {
                        let known_to_n = &self.known[&n];
                        let seen = self
                            .broadcast_msgs
                            .iter()
                            .cloned()
                            .filter(|item| !known_to_n.contains(item))
                            .collect();

                        let msg = Message {
                            src: self.node_id.clone(),
                            dest: n,
                            body: MessageBody {
                                msg_id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen },
                            },
                        };

                        self.send(msg, stdout)?;
                    }
                }
            },
            Event::Message(message) => {
                match message.body.payload.clone() {
                    Payload::Echo { echo } => {
                        let reply = message.into_reply(self.msg_id, Payload::EchoOk { echo });

                        self.send(reply, stdout)?;
                    }
                    Payload::Init {
                        node_id: _,
                        node_ids: _,
                    } => {
                        // self.node_id = node_id;
                        // self.node_ids = node_ids;
                        // self.known = self
                        //     .node_ids
                        //     .clone()
                        //     .into_iter()
                        //     .map(|nid| (nid, HashSet::new()))
                        //     .collect();

                        // let reply = message.into_reply(self.msg_id, Payload::InitOk);

                        // self.send(reply, stdout)?;
                    }
                    Payload::InitOk => bail!("receive init_ok message"),
                    Payload::Generate => {
                        let id = Uuid::new_v4().to_string();
                        let reply = message.into_reply(self.msg_id, Payload::GenerateOk { id });

                        self.send(reply, stdout)?;
                    }
                    Payload::GenerateOk { id } => bail!("receive generate_ok message"),
                    Payload::Broadcast {
                        message: broadcast_message,
                    } => {
                        if !self.received_broadcast_message(broadcast_message) {
                            self.save_broadcast_message(broadcast_message);

                            // forward broadcast message to all neighbours
                            // skip the neighbour that send us the broadcast message
                            let neighbours = self
                                .get_neighbours()
                                .into_iter()
                                .filter(|nei| *nei != message.src);

                            for neighbour in neighbours {
                                let forward_message = Message {
                                    src: self.node_id.clone(),
                                    dest: neighbour,
                                    body: MessageBody {
                                        msg_id: Some(self.msg_id),
                                        in_reply_to: None,
                                        payload: Payload::Broadcast {
                                            message: broadcast_message,
                                        },
                                    },
                                };
                                self.send(forward_message, stdout)?;
                            }

                            let reply = message.into_reply(self.msg_id, Payload::BroadcastOk);
                            self.send(reply, stdout)?;
                        }
                    }
                    Payload::Read => {
                        let reply = message.into_reply(
                            self.msg_id,
                            Payload::ReadOk {
                                messages: self.broadcast_msgs.clone(),
                            },
                        );
                        self.send(reply, stdout)?;
                    }
                    Payload::Topology { topology } => {
                        self.save_topology(topology);
                        let reply = message.into_reply(self.msg_id, Payload::TopologyOk);
                        self.send(reply, stdout)?;
                    }
                    Payload::Gossip { seen } => {
                        // receive gossip message from other nodes
                        self.known
                            .get_mut(&message.dest)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.broadcast_msgs.extend(seen);
                    }
                    // Payload::Gossip { seen } => {
                    //     let neighbours = self.get_neighbours();

                    //     for n in neighbours {

                    //         let message = Message {
                    //             src: self.node_id.clone(),
                    //             dest:  n,
                    //             body: MessageBody {
                    //                 msg_id: None,
                    //                 in_reply_to: None,
                    //                 payload: Pay,
                    //             }
                    //         }
                    //     }

                    // },
                    Payload::EchoOk { .. }
                    | Payload::BroadcastOk
                    | Payload::ReadOk { .. }
                    | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }

    fn send(&mut self, message: Message, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *stdout, &message)?;
        stdout.write_all(b"\n")?;
        self.msg_id += 1;
        Ok(())
    }

    fn save_broadcast_message(&mut self, message: BroadcastMessage) {
        self.broadcast_msgs.insert(message);
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
