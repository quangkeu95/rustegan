use anyhow::Context;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use rand::Rng;
use rustegan::{message::*, node::*, *};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Lines, StdinLock, StdoutLock};
use std::sync::mpsc::Sender;
use std::time::Duration;

lazy_static! {
    static ref ELECTION_TIMEOUT_DURATION: chrono::Duration = chrono::Duration::seconds(2);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Record>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    RequestVote {
        candidate: NodeId,
        timeout: i64,
        term: usize,
    },
    GrantVote {},
}

// [offset, msg]
type Record = [usize; 2];

#[derive(Debug, Clone)]
enum Command {
    Election, // ContestLeader { start_timestamp: DateTime<Utc> },
}

#[derive(Debug, Clone)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

struct KafkaNode {
    node: NodeId,
    node_ids: Vec<NodeId>,
    node_role: NodeRole,
    leader: Option<LeaderInfo>,
    election_deadline: DateTime<Utc>,
    term: usize,
    voted_for: HashMap<usize, NodeId>, // key is term, value is voted node
    start_timestamp: DateTime<Utc>,
    id: usize,
    /// Offset is shared between node
    offset: usize,
    log_storage: HashMap<String, Vec<Log>>,
    commits: HashMap<String, usize>,
}

struct LeaderInfo {
    node: NodeId,
    start_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct Log {
    offset: usize,
    msg: usize,
}

struct CachedTx {
    key: String,
    msg: usize,
    // timestamp: DateTime<Utc>,
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
        let now = Utc::now() + rand::thread_rng().gen_range(100..=900) * Duration::from_millis(1);

        // a thread to continuous checking leader status and start an election
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(300));

            if let Err(_) = sender.send(Event::Command(Command::Election)) {
                break;
            }
        });

        Ok(KafkaNode {
            node: init.node_id,
            node_ids: init.node_ids,
            node_role: NodeRole::Follower,
            leader: None,
            election_deadline: Utc::now(),
            term: 0,
            voted_for: HashMap::new(),
            start_timestamp: now,
            id: 0,
            offset: 0,
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
                Command::Election => {
                    // check if we wanna start an election
                    let now = Utc::now();

                    // if we got no leader and the election deadline is stale, we request votes from others to vote for us to become the leader
                    if self.election_deadline < now && self.leader.is_none() {
                        self.become_candidate();

                        let timeout = now - self.election_deadline;
                        self.request_vote(timeout, stdout)?;

                        // we reset the election deadline to add some gap before we retry to send request vote again if the previous request votes is failed
                        self.reset_election_deadline();
                    }
                }
            },
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Send { key, msg } => {
                        //
                        // let leader = self.leader.as_ref().unwrap();
                        let last_offset = self.last_offset(&key);
                        let offset = if last_offset == 0 { 0 } else { last_offset + 1 };

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
                    Payload::RequestVote {
                        candidate,
                        timeout,
                        term,
                    } => {
                        self.maybe_step_down(term);
                        if term < self.term {
                            eprintln!("Candidate {:#?} term {:#?} lower than our term {:#?}. Not granting vote.", candidate, term, self.term);
                        } else if let Some(voted) = self.voted_for.get(&term) {
                            eprintln!("Already voted for {:#?} with term {:#?}", voted, term);
                        } else {
                            // self.voted_for.insert()
                        }
                    }
                    Payload::GrantVote {} => {}

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
    // when become an candidate, we increase our term and voted for us
    fn become_candidate(&mut self) {
        self.node_role = NodeRole::Candidate;
        self.term += 1;
        self.voted_for.insert(self.term, self.node.clone());
        eprintln!(
            "{:#?} become candidate with term {:#?}",
            self.node, self.term
        );
    }

    fn become_follower(&mut self) {
        self.node_role = NodeRole::Follower;
        if let Some(voted) = self.voted_for.get(&self.term) {
            if *voted == self.node {
                // when become a follower and the vote is set for us with the term, we want to delete the vote
                self.voted_for.remove(&self.term);
            }
        }
        eprintln!("{:#?} become follower", self.node);
    }

    fn maybe_step_down(&mut self, remote_term: usize) {
        if self.term < remote_term {
            eprintln!(
                "Stepping down: remote term {:#?} is higher then our term {:#?}",
                remote_term, self.term
            );
            self.term = remote_term;
            self.become_follower();
        }
    }

    // Request other nodes vote for us as a leader
    fn request_vote(
        &mut self,
        timeout: chrono::Duration,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let nodes = self.node_ids.iter().filter(|&n| *n != self.node);
        let term = self.term;

        for n in nodes {
            let msg = Message {
                src: self.node.clone(),
                dest: n.clone(),
                body: MessageBody {
                    msg_id: None,
                    in_reply_to: None,
                    payload: Payload::RequestVote {
                        timeout: timeout.num_milliseconds(),
                        term,
                        candidate: self.node.clone(),
                    },
                },
            };

            msg.send(&mut *stdout)?;
        }

        Ok(())
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline = Utc::now()
            + *ELECTION_TIMEOUT_DURATION
            + rand::thread_rng().gen_range(100..=900) * Duration::from_millis(1);
    }

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
