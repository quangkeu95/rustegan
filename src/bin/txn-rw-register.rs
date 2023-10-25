use anyhow::Context;
use chrono::{DateTime, Utc};
use rustegan::{message::*, node::*, *};
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
enum Command {}

struct TxnRwNode {
    id: usize,
    storage: HashMap<usize, usize>,
}

impl Node<(), Payload, Command> for TxnRwNode {
    fn from_init(
        _state: (),
        init: Init,
        sender: Sender<Event<Payload, Command>>,
    ) -> anyhow::Result<Self> {
        Ok(TxnRwNode {
            id: 0,
            storage: HashMap::new(),
        })
    }

    fn handle(
        &mut self,
        event: Event<Payload, Command>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match event {
            Event::EOF => {}
            Event::Command(command) => {}
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));

                match reply.body.payload {
                    Payload::Txn { txn } => {
                        let mut responses: Vec<Operation> = Vec::new();

                        let mut prepare_write_txs: Vec<(usize, usize)> = Vec::new();

                        for tx in txn {
                            let mut res = tx.clone();
                            match tx {
                                Operation::Read { key, value: _value } => {
                                    if let Some(read_value) = self.storage.get(&key) {
                                        res.set_read_value(*read_value);
                                    }
                                }
                                Operation::Write { key, value } => {
                                    prepare_write_txs.push((key, value));
                                }
                            }
                            responses.push(res);
                        }

                        for tx in prepare_write_txs {
                            self.storage.insert(tx.0, tx.1);
                        }

                        reply.body.payload = Payload::TxnOk { txn: responses };
                        reply.send(stdout)?;
                    }
                    Payload::TxnOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, TxnRwNode, _, _>(())
}
