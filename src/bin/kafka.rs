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
    Send { key: String, msg: usize },
    SendOk { offset: usize },
    Poll { offsets: HashMap<String, usize> },
    PollOk { msgs: HashMap<String, Vec<Record>> },
    CommitOffsets { offsets: HashMap<String, usize> },
    CommitOffsetsOk,
    ListCommittedOffsets { keys: Vec<String> },
    ListCommittedOffsetsOk { offsets: HashMap<String, usize> },
}

// [offset, msg]
type Record = [usize; 2];

#[derive(Debug, Clone)]
enum Command {
    Gossip,
}

struct KafkaNode {
    node: NodeId,
    node_ids: Vec<NodeId>,
    id: usize,
    log_storage: HashMap<String, Vec<Log>>,
    commits: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
struct Log {
    offset: usize,
    msg: usize,
}

impl Log {
    fn into_record(self) -> Record {
        [self.offset, self.msg]
    }
}

impl Node<(), Payload, Command> for KafkaNode {
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

        Ok(KafkaNode {
            node: init.node_id,
            node_ids: init.node_ids,
            id: 0,
            log_storage: HashMap::new(),
            commits: HashMap::new(),
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
                    // for n in self.node_ids.iter() {
                    //     if *n == self.node {
                    //         continue;
                    //     }
                    //     let msg = Message {
                    //         src: self.node.clone(),
                    //         dest: n.clone(),
                    //         body: MessageBody {
                    //             msg_id: None,
                    //             in_reply_to: None,
                    //             payload: Payload::Gossip {
                    //                 seen: self.seen.clone(),
                    //             },
                    //         },
                    //     };

                    //     msg.send(&mut *stdout)?;
                    // }
                }
            },
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Send { key, msg } => {
                        let last_offset = self.last_offset(&key);
                        let offset = last_offset + 1;

                        if let Some(logs) = self.log_storage.get_mut(&key) {
                            logs.push(Log { offset, msg });
                        } else {
                            self.log_storage.insert(key, vec![Log { offset, msg }]);
                        }

                        reply.body.payload = Payload::SendOk { offset };
                        reply.send(&mut *stdout)?;
                    }
                    Payload::Poll { offsets } => {
                        let poll_res = offsets
                            .into_iter()
                            .map(|(key, offset)| {
                                let logs = self.get_logs(&key, offset, 3);
                                let logs = logs
                                    .into_iter()
                                    .map(|item| item.into_record())
                                    .collect::<Vec<Record>>();
                                (key, logs)
                            })
                            .collect::<HashMap<String, Vec<Record>>>();

                        reply.body.payload = Payload::PollOk { msgs: poll_res };
                        reply.send(&mut *stdout)?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        for (key, offset) in offsets.into_iter() {
                            self.commits.insert(key, offset);
                        }

                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply.send(&mut *stdout)?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let list_commits: HashMap<String, usize> = self
                            .commits
                            .iter()
                            .filter_map(|(key, &offset)| {
                                keys.contains(key).then(|| (key.clone(), offset))
                            })
                            .collect();

                        reply.body.payload = Payload::ListCommittedOffsetsOk {
                            offsets: list_commits,
                        };
                        reply.send(&mut *stdout)?;
                    }

                    Payload::SendOk { .. }
                    | Payload::PollOk { .. }
                    | Payload::CommitOffsetsOk
                    | Payload::ListCommittedOffsetsOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

impl KafkaNode {
    fn last_offset(&self, key: &str) -> usize {
        let last_offset = if let Some(log) = self.log_storage.get(key) {
            if let Some(last) = log.last() {
                last.offset
            } else {
                0
            }
        } else {
            0
        };
        last_offset
    }

    fn get_logs(&self, key: &str, offset: usize, max_items: usize) -> Vec<Log> {
        let logs = self
            .log_storage
            .get(key)
            .map(Clone::clone)
            .unwrap_or(Vec::new());

        logs.into_iter()
            .filter(|item| item.offset >= offset)
            .take(max_items)
            .collect::<Vec<Log>>()
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaNode, _, _>(())
}
