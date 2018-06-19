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
#[macro_use]
extern crate futures;
extern crate kv;
extern crate raft;
extern crate tokio;

use futures::sync::oneshot;
use futures::sync::mpsc::{self, Receiver, Sender};
use futures::{Async, Future, Poll, Sink, Stream};
use futures::sink;

use kv::{Msg, ProposeCallback, Response};

use std::collections::HashMap;
use std::time::{Duration, Instant};

use raft::prelude::*;
use raft::storage::MemStorage;

use tokio::timer::{Delay, Interval};

// A simple example about how to use the Raft library in Rust.
fn main() {
    let (sender, receiver) = mpsc::channel(5);
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.spawn(ClientConnection::new(sender));
    rt.spawn(Node::new(receiver));
    rt.shutdown_on_idle().wait().unwrap();
}

struct Node {
    r: RawNode<MemStorage>,
    future: Box<Stream<Item = Msg, Error = ()> + Send> ,
    cbs: HashMap<u8, ProposeCallback>,
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
                        cb(Ok(()));
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
                Some(Msg::Propose { id, mut key, mut value, cb }) => {
                    self.cbs.insert(id, cb);
                    let mut entry_data = vec![key.len() as u8];
                    entry_data.append(&mut key);
                    entry_data.push(value.len() as u8);
                    entry_data.append(&mut value);
                    self.r.propose(vec![id], entry_data).unwrap();
                }
                Some(Msg::Raft(m)) => self.r.step(m).unwrap(),
                Some(Msg::Tick(_)) => {
                    self.r.tick();
                }
                None => {
                    break Ok(Async::Ready(()));
                },
            }
            self.on_ready();
        }
    }
}

struct ClientConnection {
    sender: Option<Sender<Msg>>,
    trigger: Option<Box<Future<Item = (), Error = ()> + Send>>,
    send: Option<sink::Send<Sender<Msg>>>,
    receiver: Option<oneshot::Receiver<Response>>,
}

impl ClientConnection {
    fn new(sender: Sender<Msg>) -> Self {
        let trigger = Delay::new(Instant::now() + Duration::from_secs(2))
            .map_err(|e| panic!("timer failed; err={:?}", e));
        Self {
            sender: Some(sender),
            trigger: Some(Box::new(trigger)),
            send: None,
            receiver: None,
        }
    }
}

impl Future for ClientConnection {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut waiting = true;
        loop {
            if let Some(ref mut trigger) = self.trigger {
                let _ = try_ready!(trigger.poll());
                waiting = false;
            }

            self.trigger.take();
            if let Some(sender) = self.sender.take() {
                let (s1, r1) = oneshot::channel::<Response>();
                // Send a command to the Raft, wait for the Raft to apply it
                // and get the result.
                let mut s1 = Some(s1);
                println!("propose a request");
                let msg = Msg::Propose {
                    id: 1,
                    key: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                    cb: Box::new(move |rsp| {
                        s1.take().unwrap().send(rsp).unwrap();
                    })
                };
                self.send = Some(sender.send(msg));
                self.receiver = Some(r1);
                waiting = false;
            }

            if let Some(ref mut send) = self.send {
                match send.poll() {
                    Ok(Async::Ready(_)) => {
                        waiting = false;
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("sending failed: {:?}", e),
                }
            }

            self.send.take();
            if let Some(ref mut receiver) = self.receiver {
                match receiver.poll() {
                    Ok(Async::Ready(n)) => {
                        assert_eq!(n, Ok(()));
                        println!("receive the propose callback");
                        return Ok(Async::Ready(()));
                    }
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("receiver canceled: {:?}", e),
                }
            }
            if waiting {
                break Ok(Async::NotReady);
            }
        }
    }
}
