// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
extern crate bincode;
extern crate bytes;
#[macro_use]
extern crate futures;
extern crate kv;
extern crate raft;
extern crate tokio;
extern crate tokio_codec;

use bytes::{BufMut, BytesMut};

use futures::sink;
use futures::sync::mpsc::{self, Receiver, Sender};
use futures::sync::oneshot;
use futures::{Async, Future, Poll, Sink, Stream};

use kv::{ByteStr, Command, CommandResult, Error, Msg, Response, ResultCallback};

use std::collections::{HashMap, HashSet};
use std::io;
use std::time::{Duration, Instant};

use raft::prelude::*;
use raft::storage::MemStorage;

use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::timer::Interval;

use tokio_codec::{Decoder, Encoder, FramedRead, FramedWrite};

// A simple example about how to use the Raft library in Rust.
fn main() {
    let (sender, receiver) = mpsc::channel(5);
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(Node::new(receiver));

    let addr = "127.0.0.1:12345".parse().unwrap();
    let tcp = TcpListener::bind(&addr).unwrap();
    let server = tcp.incoming()
        .for_each(move |tcp| {
            let conn = ClientConnection::new(tcp, sender.clone());
            tokio::spawn(conn);
            Ok(())
        })
        .map_err(|e| {
            println!("server error {:?}", e);
        });

    rt.spawn(server);
    rt.shutdown_on_idle().wait().unwrap();
}

struct Node {
    r: RawNode<MemStorage>,
    future: Box<Stream<Item = Msg, Error = ()> + Send>,
    cbs: HashMap<u8, ResultCallback>,
}

impl Node {
    fn new(receiver: Receiver<Msg>) -> Self {
        // Create the configuration for the Raft node.
        let cfg = Config {
            // The unique ID for the Raft node.
            id: 1,
            // The Raft node list.
            // Mostly, the peers need to be saved in the storage
            // and we can get them from the Storage::initial_state function, so here
            // you need to set it empty.
            peers: vec![1],
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            // Just for log
            tag: format!("[{}]", 1),
            ..Default::default()
        };

        let interval = Interval::new(Instant::now(), Duration::from_millis(100))
            .map(move |t| Msg::Tick(t))
            .map_err(move |e| panic!("timer error: {}", e));
        Self {
            // Create the Raft node.
            r: RawNode::new(&cfg, MemStorage::new(), vec![]).unwrap(),
            future: Box::new(receiver.select(interval)),
            // Use a HashMap to hold the `propose` callbacks.
            cbs: HashMap::new(),
        }
    }

    fn on_ready(&mut self) {
        if !self.r.has_ready() {
            return;
        }

        // The Raft is ready, we can do something now.
        let mut ready = self.r.ready();

        let is_leader = self.r.raft.leader_id == self.r.raft.id;
        if is_leader {
            // If the peer is leader, the leader can send messages to other followers ASAP.
            let msgs = ready.messages.drain(..);
            for _msg in msgs {
                // Here we only have one peer, so can ignore this.
            }
        }

        if !raft::is_empty_snap(&ready.snapshot) {
            // This is a snapshot, we need to apply the snapshot at first.
            self.r
                .mut_store()
                .wl()
                .apply_snapshot(ready.snapshot.clone())
                .unwrap();
        }

        if !ready.read_states.is_empty() {
            for rs in &ready.read_states {
                let max = rs.index + 1;
                let entries = self.r
                    .get_store()
                    .entries(1, max, u64::max_value())
                    .unwrap();

                let exact = rs.request_ctx[0] == 0;
                let id = rs.request_ctx[1];
                let mut prefix = vec![];
                if rs.request_ctx.len() > 2 {
                    if exact {
                        prefix.push((rs.request_ctx.len() - 2) as u8);
                    }
                    prefix.extend(&rs.request_ctx[2..]);
                }
                let prefix_len = prefix.len();

                let mut found = Vec::new();
                let mut ignore = HashSet::new();
                for e in entries.iter().rev() {
                    if e.data.is_empty() || e.data.len() < prefix_len {
                        continue;
                    }
                    if exact && prefix == &e.data[..prefix_len] {
                        if e.data.len() > prefix_len {
                            found.push(ByteStr(e.data[prefix_len + 1..].to_vec()));
                        }
                        if exact {
                            break;
                        }
                    } else if prefix_len == 0 || prefix == &e.data[1..prefix_len + 1] {
                        let key_len = e.data[0] as usize;
                        let key = ByteStr(e.data[1..key_len + 1].to_vec());
                        if e.data.len() > (key_len + 1) && !ignore.contains(&key) {
                            found.push(key)
                        } else {
                            ignore.insert(key);
                        }
                    }
                }

                let res = if !exact {
                    Ok(Response::Keys(found))
                } else if found.len() == 0 {
                    Err(Error::NotFound)
                } else {
                    Ok(Response::Value(found.pop().unwrap()))
                };

                if let Some(mut cb) = self.cbs.remove(&id) {
                    cb(res);
                }
            }
        }

        if !ready.entries.is_empty() {
            // Append entries to the Raft log
            self.r.mut_store().wl().append(&ready.entries).unwrap();
        }

        if let Some(ref hs) = ready.hs {
            // Raft HardState changed, and we need to persist it.
            self.r.mut_store().wl().set_hardstate(hs.clone());
        }

        if !is_leader {
            // If not leader, the follower needs to reply the messages to
            // the leader after appending Raft entries.
            let msgs = ready.messages.drain(..);
            for _msg in msgs {
                // Send messages to other peers.
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            let mut _last_apply_index = 0;
            for entry in committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                _last_apply_index = entry.get_index();

                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                if entry.get_entry_type() == EntryType::EntryNormal {
                    if let Some(mut cb) = self.cbs.remove(entry.get_context().get(0).unwrap()) {
                        cb(Ok(Response::Applied));
                    }
                }

                // TODO: handle EntryConfChange
            }
        }

        // Advance the Raft
        self.r.advance(ready);
    }
}

impl Future for Node {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match try_ready!(self.future.poll()) {
                Some(Msg::Request { id, cmd, cb }) => {
                    self.cbs.insert(id, cb);
                    match cmd {
                        Command::Set { mut key, mut value } => {
                            let mut entry_data = vec![key.0.len() as u8];
                            entry_data.append(&mut key.0);
                            if !value.0.is_empty() {
                                entry_data.push(value.0.len() as u8);
                                entry_data.append(&mut value.0);
                            }
                            self.r.propose(vec![id], entry_data).unwrap();
                        }
                        Command::Get { mut key } => {
                            let mut entry_data = vec![0, id];
                            entry_data.append(&mut key.0);
                            self.r.read_index(entry_data);
                        }
                        Command::List { mut prefix } => {
                            let mut entry_data = vec![1, id];
                            entry_data.append(&mut prefix.0);
                            self.r.read_index(entry_data);
                        }
                    }
                }
                Some(Msg::Raft(m)) => self.r.step(m).unwrap(),
                Some(Msg::Tick(_)) => {
                    self.r.tick();
                }
                None => {
                    break Ok(Async::Ready(()));
                }
            }
            self.on_ready();
        }
    }
}

struct ClientConnection {
    cmd_stream: Box<Stream<Item = Command, Error = io::Error> + Send>,
    rsp_sink: Option<BoxedResultSink>,
    sender: Option<Sender<Msg>>,
    node_send: Option<sink::Send<Sender<Msg>>>,
    client_send: Option<sink::Send<BoxedResultSink>>,
    receiver: Option<oneshot::Receiver<CommandResult>>,
    ending: bool,
}

impl ClientConnection {
    fn new(tcp: TcpStream, sender: Sender<Msg>) -> Self {
        let (read, write) = tcp.split();
        Self {
            cmd_stream: Box::new(FramedRead::new(read, ClientStream {})),
            rsp_sink: Some(Box::new(FramedWrite::new(write, ClientStream {}))),
            sender: Some(sender),
            node_send: None,
            client_send: None,
            receiver: None,
            ending: false,
        }
    }
}

impl Future for ClientConnection {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut waiting = true;
        loop {
            if let Some(sender) = self.sender.take() {
                let cmd = match self.cmd_stream.poll() {
                    Ok(Async::Ready(Some(cmd))) => {
                        waiting = false;
                        Some(cmd)
                    }
                    Ok(Async::Ready(None)) => {
                        waiting = false;
                        self.ending = true;
                        None
                    }
                    Ok(Async::NotReady) => None,
                    Err(e) => panic!("cmd stream error: {:?}", e),
                };

                match cmd {
                    Some(cmd) => {
                        let (s1, r1) = oneshot::channel::<CommandResult>();
                        let mut s1 = Some(s1);
                        println!("send request: {:?}", cmd);
                        let msg = Msg::Request {
                            id: 1,
                            cmd,
                            cb: Box::new(move |rsp| {
                                s1.take().unwrap().send(rsp).unwrap();
                            }),
                        };

                        self.node_send = Some(sender.send(msg));
                        self.receiver = Some(r1);
                        waiting = false;
                    }
                    None => {
                        self.sender = Some(sender);
                    }
                }
            }

            if let Some(mut send) = self.node_send.take() {
                match send.poll() {
                    Ok(Async::Ready(_)) => {
                        waiting = false;
                    }
                    Ok(Async::NotReady) => {
                        self.node_send = Some(send);
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("sending failed: {:?}", e),
                }
            }

            if let Some(mut receiver) = self.receiver.take() {
                match receiver.poll() {
                    Ok(Async::Ready(rsp)) => {
                        println!("send response: {:?}", rsp);
                        let sink = self.rsp_sink.take().unwrap();
                        self.client_send = Some(sink.send(rsp));
                        waiting = false;
                    }
                    Ok(Async::NotReady) => {
                        self.receiver = Some(receiver);
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("receiver canceled: {:?}", e),
                }
            }

            if let Some(mut send) = self.client_send.take() {
                match send.poll() {
                    Ok(Async::Ready(sink)) => {
                        self.rsp_sink = Some(sink);
                        waiting = false;
                    }
                    Ok(Async::NotReady) => {
                        self.client_send = Some(send);
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("sending to client failed: {:?}", e),
                }
            }

            if self.ending {
                break Ok(Async::Ready(()));
            }
            if waiting {
                break Ok(Async::NotReady);
            }
        }
    }
}

struct ClientStream {}

impl Decoder for ClientStream {
    type Item = Command;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match bincode::deserialize(src) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("decode: {:?}", e),
            )),
        }
    }
}

impl Encoder for ClientStream {
    type Item = CommandResult;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        bincode::serialize_into(&mut dst.writer(), &item).unwrap();
        Ok(())
    }
}

type BoxedResultSink = Box<Sink<SinkItem = CommandResult, SinkError = io::Error> + Send>;
