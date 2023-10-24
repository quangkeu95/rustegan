use chrono::{DateTime, Utc};
use log::*;
use rand::Rng;
use rustegan::{message::*, node::*, *};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::io::StdoutLock;
use std::sync::mpsc::Sender;
use std::time::Duration;

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
    LogResponse {
        node: NodeId,
        term: usize,
        ack: usize,
        success: bool,
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
    /// Commits
    commit_cache: Vec<Log>,
    /// The number of logs that has been sent to the followers
    sent_length: HashMap<NodeId, usize>,
    /// The number of acked that we have received from the followers
    acked_length: HashMap<NodeId, usize>,
    id: usize,
    /// Offset is shared between node
    offset: HashMap<String, usize>,
    committed_offset: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log {
    offset: usize,
    key: String,
    msg: usize,
    term: usize,
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
        let election_deadline = Self::get_rand_election_deadline();
        let (node_role, leader) = if init.node_id == "n0".into() {
            (NodeRole::Leader, Some("n0".into()))
        } else {
            (NodeRole::Follower, Some("n0".into()))
        };

        // a thread to continuous checking leader status and start an election
        {
            let sender = sender.clone();

            std::thread::spawn(move || loop {
                let rand_duration =
                    Duration::from_millis(1) * rand::thread_rng().gen_range(150..=300);
                std::thread::sleep(rand_duration);

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
            node_role,
            leader,
            election_deadline,
            term: 0,
            voted_for: HashMap::new(),
            votes_received: HashSet::new(),
            log_cache: Vec::new(),
            commit_cache: Vec::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            id: 0,
            offset: HashMap::new(),
            committed_offset: HashMap::new(),
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
                        error!("Receive Send request with key {} msg {}", key, msg);

                        if matches!(self.node_role, NodeRole::Leader) {
                            let offset = self.next_offset(&key);

                            // append to log cache
                            self.add_log(Log {
                                offset,
                                key: key.clone(),
                                msg,
                                term: self.term,
                            });

                            error!(
                                "Reply Send OK for key {} message {} offset {} from {}",
                                key, msg, offset, message_src
                            );
                            reply.body.payload = Payload::SendOk { offset };
                            reply.send(stdout)?;
                        } else {
                            // forward the request to the current leader
                            let _ = self.forward_msg_to_leader(message, stdout)?;
                        }
                    }
                    Payload::Poll { offsets } => {
                        let msgs = offsets
                            .into_iter()
                            .map(|(key, offset)| {
                                let last_committed_offset = self.get_last_committed_offset(&key);

                                if last_committed_offset < offset {
                                    (key, Vec::new())
                                } else {
                                    let logs = self
                                        .commit_cache
                                        .iter()
                                        .filter_map(|log| {
                                            if log.key == key && log.offset >= offset {
                                                Some(log.clone().into_record())
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<Vec<Record>>();
                                    (key, logs)
                                }
                            })
                            .collect::<HashMap<String, Vec<Record>>>();

                        reply.body.payload = Payload::PollOk { msgs };
                        reply.send(&mut *stdout)?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        let mut invalid = false;
                        for (key, offset) in offsets {
                            let last_offset = self.get_last_committed_offset(&key);
                            if last_offset < offset {
                                invalid = true;
                                break;
                            }
                        }

                        if !invalid {
                            reply.body.payload = Payload::CommitOffsetsOk;
                            reply.send(&mut *stdout)?;
                        }
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let offsets = keys
                            .into_iter()
                            .filter_map(|key| {
                                let offset = self.get_last_committed_offset(&key);
                                if offset > 0 {
                                    Some((key, offset))
                                } else {
                                    None
                                }
                            })
                            .collect::<HashMap<String, usize>>();

                        reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                        reply.send(&mut *stdout)?;
                    }
                    Payload::VoteRequest {
                        candidate,
                        timeout,
                        term: c_term,
                        last_log_term: c_log_term,
                        last_log_length: c_log_length,
                    } => {
                        error!(
                            "Receive VoteRequest from {:#?} with term {:#?}",
                            candidate, c_term
                        );
                        // if candidate term if greater than us, we step down
                        self.maybe_step_down(c_term);
                        let last_term = self.last_log_term();
                        let log_ok = c_term > last_term
                            || (c_log_term == last_term && c_log_length >= self.log_cache.len());

                        if c_term == self.term && log_ok && self.voted_for.get(&c_term).is_none() {
                            error!("Voted for {:#?} with term {:#?}", candidate, c_term);
                            self.voted_for.insert(c_term, candidate.clone());

                            reply.body.payload = Payload::VoteResponse {
                                candidate,
                                term: self.term,
                                approve: true,
                            };
                            reply.send(&mut *stdout)?;
                        } else {
                            // if candidate's log is outdated from our log, the candidate cannot be a leader
                            error!("Not vote for {:#?}", candidate);

                            // return our term to VoteRequest sender, so that the candidate can cancel the election if their term is less than our term
                            reply.body.payload = Payload::VoteResponse {
                                candidate,
                                term: self.term,
                                approve: false,
                            };
                            reply.send(&mut *stdout)?;
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
                        } else {
                            self.maybe_step_down(term);
                            self.reset_election_deadline();
                        }
                    }
                    Payload::LogRequest {
                        leader,
                        term,
                        prefix_len,
                        prefix_term,
                        commit_len,
                        suffix,
                    } => {
                        error!(
                            "Receive LogRequest from {} with term {} prefix_len {} prefix_term {} commit_len {} suffix {:#?}",
                            leader, term, prefix_len, prefix_term, commit_len, suffix
                        );
                        self.maybe_step_down(term);
                        self.reset_election_deadline();

                        if self.term == term {
                            self.leader = Some(leader);
                        }

                        // we want to make sure that our logs is up-to-date with leader logs (not missing any entries)
                        let log_ok = self.log_cache.len() >= prefix_len
                            && (prefix_len == 0
                                || self.log_cache[prefix_len - 1].term == prefix_term);

                        if term == self.term && log_ok {
                            self.append_entries(prefix_len, commit_len, &suffix)?;
                            let ack = prefix_len + suffix.len();

                            reply.body.payload = Payload::LogResponse {
                                node: self.node.clone(),
                                term: self.term,
                                ack,
                                success: true,
                            };
                            reply.send(&mut *stdout)?;
                        } else {
                            reply.body.payload = Payload::LogResponse {
                                node: self.node.clone(),
                                term: self.term,
                                ack: 0,
                                success: false,
                            };
                            reply.send(&mut *stdout)?;
                        }
                    }
                    Payload::LogResponse {
                        node,
                        term,
                        ack,
                        success,
                    } => {
                        error!(
                            "Receive LogResponse from {} term {} ack {} success {}",
                            node, term, ack, success
                        );
                        if self.term == term && matches!(self.node_role, NodeRole::Leader) {
                            let sent_length =
                                self.sent_length.get(&node).map(|n| *n).unwrap_or_default();

                            if success
                                && ack
                                    > self.acked_length.get(&node).map(|n| *n).unwrap_or_default()
                            {
                                self.sent_length.insert(node.clone(), ack);
                                self.acked_length.insert(node.clone(), ack);

                                error!("Leader commit with ack {}", ack);
                                self.leader_commit()?;
                            } else if sent_length > 0 {
                                self.sent_length.insert(node.clone(), sent_length - 1);
                                self.replicate_log(&node, stdout)?;
                            }
                        } else {
                            self.maybe_step_down(term);
                            self.reset_election_deadline();
                        }
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
        error!(
            "{:#?} become candidate with term {:#?}",
            self.node, self.term
        );
    }

    fn become_follower(&mut self) {
        self.node_role = NodeRole::Follower;
        self.voted_for.remove(&self.term);

        error!("{:#?} become follower", self.node);
    }

    fn become_leader(&mut self) {
        self.node_role = NodeRole::Leader;
        self.leader = Some(self.node.clone());
        error!("{:#?} become leader", self.node);
    }

    fn maybe_step_down(&mut self, remote_term: usize) {
        if self.term < remote_term {
            error!(
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

    fn last_log_term(&self) -> usize {
        self.log_cache.last().map(|n| n.term).unwrap_or_default()
    }

    // Request other nodes vote for us as a leader
    fn request_vote(
        &mut self,
        timeout: chrono::Duration,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let term = self.term;
        let last_log_term = self.last_log_term();
        let last_log_length = self.log_cache.len();

        for n in self.other_nodes() {
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
                    commit_len: self.commits_len(),
                    suffix: suffix.into(),
                },
            },
        };

        error!("Replicate log {:#?} to {}", message, follower);
        message.send(stdout)?;

        Ok(())
    }

    fn append_entries(
        &mut self,
        prefix_len: usize,
        leader_commit: usize,
        suffix: &[Log],
    ) -> anyhow::Result<()> {
        if suffix.len() > 0 && self.log_cache.len() > prefix_len {
            let index = min(self.log_cache.len(), prefix_len + suffix.len()) - 1;
            if self.log_cache[index].term != suffix[index - prefix_len].term {
                self.log_cache = self
                    .log_cache
                    .iter()
                    .enumerate()
                    .filter_map(|(index, item)| {
                        if index < prefix_len {
                            Some(item.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
            }
        }

        if prefix_len + suffix.len() > self.log_cache.len() {
            error!(
                "Append to logs with log_length = {} prefix_len = {} suffix_len = {}",
                self.log_cache.len(),
                prefix_len,
                suffix.len()
            );
            for i in self.log_cache.len() - prefix_len..suffix.len() {
                self.log_cache.push(suffix[i].clone());
            }
        }

        // the followers commmit only when the leader sends the leader_commit which is greater than follower's commit length
        if leader_commit > self.commits_len() {
            // some logs are ready to be committed
            for i in self.commits_len()..leader_commit {
                self.commit(self.log_cache[i].clone());
            }
        }

        Ok(())
    }

    fn forward_msg_to_leader(
        &self,
        mut msg: Message<Payload>,
        stdout: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        if let Some(leader) = &self.leader {
            msg.dest = leader.clone();

            // error!("Forward message {:#?} to leader {}", &msg, leader);
            msg.send(stdout)?;
        }
        Ok(())
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline = Self::get_rand_election_deadline();
    }

    fn get_rand_election_deadline() -> DateTime<Utc> {
        Utc::now() + rand::thread_rng().gen_range(150..=300) * Duration::from_millis(1)
    }

    fn last_offset(&self, key: &str) -> usize {
        self.offset.get(key).map(|n| *n).unwrap_or_default()
    }

    fn next_offset(&self, key: &str) -> usize {
        self.last_offset(key) + 1
    }

    fn commits_len(&self) -> usize {
        self.commit_cache.len()
    }

    fn get_last_committed_offset(&self, key: &str) -> usize {
        self.committed_offset
            .get(key)
            .map(|n| *n)
            .unwrap_or_default()
    }

    fn commit(&mut self, log: Log) {
        error!("Commit {:#?}", log);
        let offset = log.offset;
        let key = log.key.clone();

        self.commit_cache.push(log);

        self.committed_offset.insert(key, offset);
    }

    fn leader_commit(&mut self) -> anyhow::Result<()> {
        let min_acks = (self.node_ids.len() + 1) / 2;
        // acked return the number of nodes that have acknowledged log length greater than a specific length
        let acked = |length: usize| -> usize {
            self.acked_length
                .iter()
                .filter(|(_node, &acked)| acked >= length)
                .count()
        };

        // ready is a vector of log length that has at least a number of nodes acknowledged
        let ready = (1..=self.log_cache.len())
            .filter(|&length| acked(length) >= min_acks)
            .collect::<HashSet<usize>>();

        let max_commit = ready.iter().max().map(|n| *n).unwrap_or_default();

        if ready.len() > 0
            && max_commit > self.commits_len()
            && self
                .log_cache
                .get(max_commit)
                .map(|n| n.term)
                .unwrap_or_default()
                == self.term
        {
            for i in self.commits_len()..max_commit {
                self.commit(self.log_cache[i].clone());
            }
        }

        Ok(())
    }

    fn add_log(&mut self, log: Log) {
        error!("Append log {:#?}", log);
        self.offset.insert(log.key.clone(), log.offset);
        self.log_cache.push(log);
    }
}

fn main() -> anyhow::Result<()> {
    stderrlog::new()
        .module(module_path!())
        .timestamp(stderrlog::Timestamp::Millisecond)
        .init()
        .unwrap();

    main_loop::<_, KafkaNode, _, _>(())
}
