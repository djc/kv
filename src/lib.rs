extern crate raft;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate structopt;

use raft::prelude::*;

use std::io;
use std::str::FromStr;
use std::time::Instant;

pub enum Msg {
    Propose {
        id: u8,
        key: Vec<u8>,
        value: Vec<u8>,
        cb: ResultCallback,
    },
    // Here we don't use Raft Message, so use dead_code to
    // avoid the compiler warning.
    #[allow(dead_code)]
    Raft(Message),
    Tick(Instant),
}

#[derive(Clone, Debug, Deserialize, Serialize, StructOpt)]
pub enum Command {
    #[structopt(name = "set")]
    Set { key: ByteStr, value: ByteStr },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ByteStr(pub Vec<u8>);

impl FromStr for ByteStr {
    type Err = io::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ByteStr(s.as_bytes().to_vec()))
    }
}

pub type CommandResult = Result<Response, ()>;

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Applied,
}

pub type ResultCallback = Box<FnMut(CommandResult) + Send>;
