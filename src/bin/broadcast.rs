use anyhow::Context;
use rand::prelude::*;
use rustegan::{message::*, node::*, *};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: BroadcastMessage,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<BroadcastMessage>,
    },
    Topology {
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<BroadcastMessage>,
    },
}

pub type BroadcastMessage = usize;

pub enum Command {
    Gossip,
}

struct BroadcastNode {
    node: NodeId,
    id: usize,
    messages: HashSet<BroadcastMessage>,
    known: HashMap<NodeId, HashSet<BroadcastMessage>>,
    neighborhood: Vec<NodeId>,
}

impl Node<(), Payload, Command> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        sender: std::sync::mpsc::Sender<Event<Payload, Command>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = sender.send(Event::Command(Command::Gossip)) {
                break;
            }
        });

        Ok(Self {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            neighborhood: Vec::new(),
        })
    }

    fn handle(
        &mut self,
        event: Event<Payload, Command>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match event {
            Event::EOF => {}
            Event::Command(command) => match command {
                Command::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| known_to_n.contains(m));
                        // if we know that n knows m, we don't tell n that _we_ know m, so n will
                        // send us m for all eternity. so, we include a couple of extra `m`s so
                        // they gradually know all the things that we know without sending lots of
                        // extra stuff each time.
                        // we cap the number of extraneous `m`s we include to be at most 10% of the
                        // number of `m`s` we _have_ to include to avoid excessive overhead.
                        let mut rng = rand::thread_rng();
                        let additional_cap = (10 * notify_of.len() / 100) as u32;
                        notify_of.extend(already_known.iter().filter(|_| {
                            rng.gen_ratio(
                                additional_cap.min(already_known.len() as u32),
                                already_known.len() as u32,
                            )
                        }));
                        Message {
                            src: self.node.clone(),
                            dest: n.clone(),
                            body: MessageBody {
                                msg_id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        }
                        .send(&mut *stdout)
                        .with_context(|| format!("gossip to {:#?}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&reply.dest)
                            .expect("got gossip from unknown node")
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        reply.body.payload = Payload::BroadcastOk;
                        reply.send(&mut *stdout).context("reply to broadcast")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            messages: self.messages.clone(),
                        };
                        reply.send(&mut *stdout).context("reply to read")?;
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology.remove(&self.node).unwrap_or_else(|| {
                            panic!("no topology given for node {:#?}", self.node)
                        });
                        reply.body.payload = Payload::TopologyOk;
                        reply.send(&mut *stdout).context("reply to topology")?;
                    }
                    Payload::BroadcastOk | Payload::ReadOk { .. } | Payload::TopologyOk => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
