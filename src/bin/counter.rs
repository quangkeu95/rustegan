use anyhow::Context;
use chrono::{DateTime, Utc};
use rustegan::{message::*, node::*, *};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::StdoutLock;
use std::sync::mpsc::Sender;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Gossip { seen: Vec<Record> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    delta: usize,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
enum Command {
    Gossip,
}

struct CounterNode {
    node: NodeId,
    node_ids: Vec<NodeId>,
    id: usize,
    counter: usize,
    seen: Vec<Record>,
    known: HashMap<NodeId, Vec<Record>>,
}

impl Node<(), Payload, Command> for CounterNode {
    fn from_init(
        _state: (),
        init: Init,
        sender: Sender<Event<Payload, Command>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));
            if let Err(_) = sender.send(Event::Command(Command::Gossip)) {
                break;
            }
        });

        Ok(CounterNode {
            id: 0,
            node: init.node_id,
            node_ids: init.node_ids,
            counter: 0,
            seen: Vec::new(),
            known: HashMap::new(),
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
                    for n in self.node_ids.iter() {
                        if *n == self.node {
                            continue;
                        }
                        let msg = Message {
                            src: self.node.clone(),
                            dest: n.clone(),
                            body: MessageBody {
                                msg_id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip {
                                    seen: self.seen.clone(),
                                },
                            },
                        };

                        msg.send(&mut *stdout)?;
                    }
                }
            },
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Add { delta } => {
                        self.seen.push(Record {
                            delta,
                            timestamp: Utc::now(),
                        });
                        self.calc_counter();
                        reply.body.payload = Payload::AddOk;
                        reply.send(stdout).context("reply to add")?;
                    }
                    Payload::Read => {
                        reply.body.payload = Payload::ReadOk {
                            value: self.counter,
                        };
                        reply.send(stdout).context("reply to read")?;
                    }
                    Payload::Gossip { seen } => {
                        self.known.insert(message_src, seen);

                        self.calc_counter();
                    }
                    Payload::AddOk | Payload::ReadOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

impl CounterNode {
    fn calc_counter(&mut self) {
        let mut all_values = self
            .known
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<Record>>();
        all_values.extend_from_slice(&self.seen);

        self.counter = all_values.iter().map(|i| i.delta).sum::<usize>();
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, CounterNode, _, _>(())
}
