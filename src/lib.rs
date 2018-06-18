extern crate raft;

use raft::prelude::*;

pub enum Msg {
    Propose {
        id: u8,
        cb: ProposeCallback,
    },
    // Here we don't use Raft Message, so use dead_code to
    // avoid the compiler warning.
    #[allow(dead_code)]
    Raft(Message),
}

pub type ProposeCallback = Box<Fn() + Send>;
