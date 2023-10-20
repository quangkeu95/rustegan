use anyhow::Context;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use rand::Rng;
use rustegan::{message::*, node::*, *};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
    VoteRequest {
        candidate: NodeId,
        timeout: i64,
        term: usize,
        last_log_length: usize,
        last_log_term: usize,
    },
    VoteResponse {
        candidate: NodeId,
        term: usize,
        approve: bool,
    },
    LogRequest {
        leader: NodeId,
        term: usize,
        prefix_len: usize,
        prefix_term: usize,
        commit_len: usize,
        suffix: Vec<Log>,
    },
}

// [offset, msg]
type Record = [usize; 2];

#[derive(Debug, Clone)]
enum Command {
    Election, // ContestLeader { start_timestamp: DateTime<Utc> },
    ReplicateLog,
}

#[derive(Debug, Clone)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

struct KafkaNode {
    /// Current node id
    node: NodeId,
    /// All node ids
    node_ids: Vec<NodeId>,
    /// Current node role
    node_role: NodeRole,
    /// Curent leader
    leader: Option<NodeId>,
    /// Election deadline
    election_deadline: DateTime<Utc>,
    /// Term
    term: usize,
    /// For each term, we keep track of the leader we voted for
    voted_for: HashMap<usize, NodeId>,
    /// Keep track of votes for current term
    votes_received: HashSet<NodeId>,
    /// Log caching
    log_cache: Vec<Log>,
    /// The number of logs that has been sent to the followers
    sent_length: HashMap<NodeId, usize>,
    /// The number of acked that we have received from the followers
    acked_length: HashMap<NodeId, usize>,
    start_timestamp: DateTime<Utc>,
    id: usize,
    /// Offset is shared between node
    offset: HashMap<String, usize>,
    log_storage: HashMap<String, Vec<Log>>,
    commits: HashMap<String, usize>,
}

struct LeaderInfo {
    node: NodeId,
    start_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log {
    offset: usize,
    key: String,
    msg: usize,
    term: usize,
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
        {
            let sender = sender.clone();

            std::thread::spawn(move || loop {
                std::thread::sleep(Duration::from_millis(300));

                if let Err(_) = sender.send(Event::Command(Command::Election)) {
                    break;
                }
            });
        }

        // periodically check that if we are a leader, we want to replicate the log
        {
            let sender = sender.clone();

            std::thread::spawn(move || loop {
                std::thread::sleep(Duration::from_millis(300));
                if let Err(_) = sender.send(Event::Command(Command::ReplicateLog)) {
                    break;
                }
            });
        }

        Ok(KafkaNode {
            node: init.node_id,
            node_ids: init.node_ids,
            node_role: NodeRole::Follower,
            leader: None,
            election_deadline: Utc::now(),
            term: 0,
            voted_for: HashMap::new(),
            votes_received: HashSet::new(),
            log_cache: Vec::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            start_timestamp: now,
            id: 0,
            offset: HashMap::new(),
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
                Command::ReplicateLog => {
                    if self.is_leader() {
                        for n in self.other_nodes() {
                            self.replicate_log(&n, stdout)?;
                        }
                    }
                }
            },
            Event::Message(message) => {
                let message_src = message.src.clone();
                let mut reply = message.clone().into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::Send { key, msg } => {
                        // only leader can append the logs
                        if matches!(self.node_role, NodeRole::Leader) {
                            let offset = self.next_offset(&key);
                            self.log_cache.push(Log {
                                offset,
                                key,
                                msg,
                                term: self.term,
                            });

                            // require all nodes to acknowledge to the new log
                            self.acked_length
                                .insert(self.node.clone(), self.log_cache.len());

                            for n in self.other_nodes() {
                                self.replicate_log(&n, stdout)?;
                            }
                        } else {
                            // forward the request to the current leader
                            let _ = self.forward_msg_to_leader(message)?;
                        }

                        // if let Some(logs) = self.log_storage.get_mut(&key) {
                        //     logs.push(Log {
                        //         offset,
                        //         msg,
                        //         term: self.term,
                        //     });
                        // } else {
                        //     self.log_storage.insert(
                        //         key,
                        //         vec![Log {
                        //             offset,
                        //             msg,
                        //             term: self.term,
                        //         }],
                        //     );
                        // }

                        // reply.body.payload = Payload::SendOk { offset };
                        // reply.send(&mut *stdout)?;
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
                    Payload::VoteRequest {
                        candidate,
                        timeout,
                        term: c_term,
                        last_log_term: c_log_term,
                        last_log_length: c_log_length,
                    } => {
                        self.maybe_step_down(c_term);
                        if c_term < self.term {
                            eprintln!("Candidate {:#?} term {:#?} lower than our term {:#?}. Not granting vote.", candidate, c_term, self.term);
                        } else if let Some(voted) = self.voted_for.get(&c_term) {
                            eprintln!("Already voted for {:#?} with term {:#?}", voted, c_term);
                        } else {
                            let last_term = self.last_log_term();
                            let log_ok = c_term > last_term
                                || (c_term == last_term && c_log_length >= self.log_cache.len());

                            if c_term == self.term
                                && log_ok
                                && self.voted_for.get(&c_term).is_none()
                            {
                                eprintln!("Voted for {:#?} with term {:#?}", candidate, c_term);
                                self.voted_for.insert(c_term, candidate.clone());

                                reply.body.payload = Payload::VoteResponse {
                                    candidate,
                                    term: c_term,
                                    approve: true,
                                };
                                reply.send(&mut *stdout)?;
                            } else {
                                // if candidate's log is outdated from our log, the candidate cannot be a leader
                                eprintln!("Candidate {:#?} log is outdated", candidate);

                                reply.body.payload = Payload::VoteResponse {
                                    candidate,
                                    term: c_term,
                                    approve: false,
                                };
                                reply.send(&mut *stdout)?;
                            }
                        }
                    }
                    Payload::VoteResponse {
                        candidate,
                        term,
                        approve,
                    } => {
                        if matches!(self.node_role, NodeRole::Candidate)
                            && term == self.term
                            && approve
                        {
                            self.votes_received.insert(candidate);

                            // if we got majority of the votes
                            if self.votes_received.len() >= (self.node_ids.len() + 1) / 2 {
                                // become leader
                                self.become_leader();

                                for n in self.other_nodes() {
                                    self.sent_length.insert(n.clone(), self.log_cache.len());
                                    self.acked_length.insert(n.clone(), 0);

                                    self.replicate_log(&n, stdout)?;
                                }
                            }
                        } else if term > self.term {
                            self.maybe_step_down(term);
                        }
                    }
                    Payload::LogRequest {
                        leader,
                        term,
                        prefix_len,
                        prefix_term,
                        commit_len,
                        suffix,
                    } => {}

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

    // when become an candidate, we increase our term and voted for us
    fn become_candidate(&mut self) {
        self.node_role = NodeRole::Candidate;
        self.term += 1;
        self.voted_for.insert(self.term, self.node.clone());
        self.votes_received.insert(self.node.clone());
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

    fn become_leader(&mut self) {
        self.node_role = NodeRole::Leader;
        self.leader = Some(self.node.clone());
        eprintln!("{:#?} become leader", self.node);
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

    fn is_leader(&self) -> bool {
        matches!(self.node_role, NodeRole::Leader)
    }

    fn leader_is_failed(&mut self) {
        self.leader = None;
        eprintln!("Detect leader is down");
    }

    fn last_log_term(&self) -> usize {
        let last_log_term = if let Some(last) = self.log_cache.last() {
            last.term
        } else {
            0
        };
        last_log_term
    }

    // Request other nodes vote for us as a leader
    fn request_vote(
        &mut self,
        timeout: chrono::Duration,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let nodes = self.node_ids.iter().filter(|&n| *n != self.node);
        let term = self.term;
        let last_log_term = self.last_log_term();
        let last_log_length = self.log_cache.len();

        for n in nodes {
            let msg = Message {
                src: self.node.clone(),
                dest: n.clone(),
                body: MessageBody {
                    msg_id: Some(self.id),
                    in_reply_to: None,
                    payload: Payload::VoteRequest {
                        timeout: timeout.num_milliseconds(),
                        term,
                        candidate: self.node.clone(),
                        last_log_term,
                        last_log_length,
                    },
                },
            };

            msg.send(&mut *stdout)?;
        }

        Ok(())
    }

    fn replicate_log(&mut self, follower: &NodeId, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        let prefix_len = self.sent_length.get(follower).map(|n| *n).unwrap_or(0);
        let suffix = &self.log_cache[prefix_len..];

        let prefix_term = if prefix_len > 0 {
            self.log_cache[prefix_len - 1].term
        } else {
            0
        };

        let message = Message {
            src: self.node.clone(),
            dest: follower.clone(),
            body: MessageBody {
                msg_id: None,
                in_reply_to: None,
                payload: Payload::LogRequest {
                    leader: self.node.clone(),
                    term: self.term,
                    prefix_len,
                    prefix_term,
                    commit_len: 0,
                    suffix: suffix.into(),
                },
            },
        };

        message.send(stdout)?;

        Ok(())
    }

    fn forward_msg_to_leader(&self, msg: Message<Payload>) -> anyhow::Result<()> {
        Ok(())
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline = Utc::now()
            + *ELECTION_TIMEOUT_DURATION
            + rand::thread_rng().gen_range(100..=900) * Duration::from_millis(1);
    }

    fn last_offset(&self, key: &str) -> usize {
        self.offset.get(key).map(|n| *n).unwrap_or(0)
    }

    fn next_offset(&self, key: &str) -> usize {
        self.last_offset(key) + 1
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
