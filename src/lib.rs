extern crate raft;

use raft::prelude::*;

use std::time::Instant;

pub enum Msg {
    Propose {
        id: u8,
        key: Vec<u8>,
        value: Vec<u8>,
        cb: ProposeCallback,
    },
    // Here we don't use Raft Message, so use dead_code to
    // avoid the compiler warning.
    #[allow(dead_code)]
    Raft(Message),
    Tick(Instant),
}

pub type Response = Result<(), ()>;

pub type ProposeCallback = Box<FnMut(Response) + Send>;
