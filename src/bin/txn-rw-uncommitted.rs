use anyhow::Context;
use chrono::{DateTime, Utc};
use rustegan::{message::*, node::*, *};
use serde::de::value;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::io::StdoutLock;
use std::sync::mpsc::Sender;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Txn { txn: Vec<Operation> },
    TxnOk { txn: Vec<Operation> },
    SyncReq { logs: Vec<Log> },
    SyncOk { log_index: usize },
}

#[derive(Debug, Clone)]
enum Operation {
    Write { key: usize, value: usize },
    Read { key: usize, value: Option<usize> },
}

impl Operation {
    fn set_read_value(&mut self, value: usize) {
        match self {
            Operation::Read {
                key,
                value: old_value,
            } => {
                *old_value = Some(value);
            }
            _ => {}
        }
    }

    fn is_write(&self) -> bool {
        match self {
            Operation::Write { .. } => true,
            _ => false,
        }
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;

        match value {
            serde_json::Value::Array(array) => {
                if array.len() != 3 {
                    return Err(serde::de::Error::custom(
                        "invalid operation, expected an array with length = 3",
                    ));
                }
                let operation_type = array[0].as_str().unwrap();
                let key: usize = array[1].as_u64().unwrap().try_into().unwrap();

                match operation_type {
                    "r" => Ok(Operation::Read { key, value: None }),
                    "w" => {
                        let value: usize = array[2].as_u64().unwrap().try_into().unwrap();
                        Ok(Operation::Write { key, value })
                    }
                    _ => {
                        return Err(serde::de::Error::custom("unknown operation"));
                    }
                }
            }
            _ => return Err(serde::de::Error::custom("expected an array"))?,
        }
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match self {
            Operation::Write { key, value } => {
                seq.serialize_element("w")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Operation::Read { key, value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;

                if let Some(value) = value {
                    seq.serialize_element(value)?;
                } else {
                    seq.serialize_element("null")?;
                }
            }
        }
        seq.end()
    }
}

#[derive(Debug, Clone)]
enum Command {
    SyncRequest,
}

type Transaction = Vec<Operation>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log {
    operations: Vec<(usize, usize)>,
    timestamp: DateTime<Utc>,
}

struct TxnRwNode {
    node: NodeId,
    node_ids: Vec<NodeId>,
    id: usize,
    storage: HashMap<usize, ValueWithTimestamp>,
    logs: Vec<Log>,
    known_log_index: HashMap<NodeId, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValueWithTimestamp {
    value: usize,
    timestamp: DateTime<Utc>,
}

impl Node<(), Payload, Command> for TxnRwNode {
    fn from_init(
        _state: (),
        init: Init,
        sender: Sender<Event<Payload, Command>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));

            if let Err(_) = sender.send(Event::Command(Command::SyncRequest)) {
                break;
            }
        });

        Ok(TxnRwNode {
            node: init.node_id,
            node_ids: init.node_ids,
            id: 0,
            storage: HashMap::new(),
            logs: Vec::new(),
            known_log_index: HashMap::new(),
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
                Command::SyncRequest => {
                    let other_nodes = self.other_nodes();

                    if other_nodes.len() > 0 {
                        for n in other_nodes {
                            let log_index_start =
                                self.known_log_index.get(&n).map(|n| *n).unwrap_or_default();
                            let logs = self.logs[log_index_start..].iter().cloned().collect();

                            let msg = Message {
                                src: self.node.clone(),
                                dest: n,
                                body: MessageBody {
                                    msg_id: Some(self.id),
                                    in_reply_to: None,
                                    payload: Payload::SyncReq { logs },
                                },
                            };

                            msg.send(stdout)?;
                        }
                    } else {
                    }
                }
            },
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));

                match reply.body.payload {
                    Payload::Txn { txn } => {
                        let timestamp = Utc::now();
                        let mut responses: Vec<Operation> = Vec::new();

                        let mut write_tx: Vec<Operation> = Vec::new();

                        for tx in txn {
                            let mut res = tx.clone();
                            match tx {
                                Operation::Read { key, value: _value } => {
                                    if let Some(read_value) = self.get_uncommitted_value(&key) {
                                        res.set_read_value(read_value);
                                    }
                                }
                                wr_tx @ Operation::Write { .. } => {
                                    write_tx.push(wr_tx);
                                }
                            }
                            responses.push(res);
                        }

                        self.append_log(write_tx, timestamp);

                        reply.body.payload = Payload::TxnOk { txn: responses };
                        reply.send(stdout)?;
                    }
                    Payload::SyncReq { logs } => {
                        let log_len = logs.len();

                        for item in logs {
                            let timestamp = item.timestamp;
                            for op in item.operations {
                                let key = op.0;
                                let value = op.1;
                                if let Some(old_value) = self.storage.get_mut(&key) {
                                    if old_value.timestamp < timestamp {
                                        old_value.value = value;
                                    }
                                } else {
                                    self.storage
                                        .insert(key, ValueWithTimestamp { value, timestamp });
                                }
                            }
                        }

                        let log_index = self
                            .known_log_index
                            .get(&message_src)
                            .map(|n| *n)
                            .unwrap_or_default();
                        reply.body.payload = Payload::SyncOk {
                            log_index: log_index + log_len,
                        }
                    }
                    Payload::SyncOk { log_index } => {
                        self.known_log_index.insert(message_src, log_index);
                    }
                    Payload::TxnOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

impl TxnRwNode {
    fn other_nodes(&self) -> Vec<NodeId> {
        self.node_ids
            .iter()
            .filter_map(|n| {
                if *n != self.node {
                    Some(n.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_uncommitted_value(&self, key: &usize) -> Option<usize> {
        self.storage.get(key).map(|n| n.value)
    }

    fn append_log(&mut self, tx: Transaction, timestamp: DateTime<Utc>) {
        // let operations = tx
        //     .clone()
        //     .into_iter()
        //     .filter_map(|op| match op {
        //         Operation::Write { key, value } => Some((key, value)),
        //         _ => None,
        //     })
        //     .collect::<Vec<(usize, usize)>>();

        // let log = Log {
        //     operations,
        //     timestamp,
        // };
        // self.logs.push(log);

        for op in tx {
            match op {
                Operation::Write { key, value } => {
                    self.storage
                        .insert(key, ValueWithTimestamp { value, timestamp });
                }
                _ => {}
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, TxnRwNode, _, _>(())
}
