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
extern crate futures;
extern crate kv;
extern crate raft;
#[macro_use]
extern crate structopt;
extern crate tokio;
extern crate tokio_codec;

use bytes::{BufMut, BytesMut};

use futures::sink;
use futures::{Async, Future, Poll, Sink, Stream};

use kv::{Command, CommandResult};

use std::io;
use std::net::SocketAddr;

use structopt::StructOpt;

use tokio::io::AsyncRead;
use tokio::net::TcpStream;

use tokio_codec::{Decoder, Encoder, FramedRead, FramedWrite};

// A simple example about how to use the Raft library in Rust.
fn main() {
    let opts = Options::from_args();
    tokio::run(
        TcpStream::connect(&opts.host)
            .map_err(|e| panic!("error while connecting: {}", e))
            .and_then(move |tcp| ServerConnection::new(tcp, opts.cmd.clone())),
    );
}

#[derive(Debug, StructOpt)]
#[structopt(name = "client", about = "A key/value store client")]
struct Options {
    #[structopt(short = "h", long = "host")]
    host: SocketAddr,
    #[structopt(subcommand)]
    cmd: Command,
}

struct ServerConnection {
    cmd_sink: Option<BoxedCommandSink>,
    rsp_stream: Box<Stream<Item = CommandResult, Error = io::Error> + Send>,
    send: Option<sink::Send<BoxedCommandSink>>,
    cmd: Option<Command>,
}

impl ServerConnection {
    fn new(tcp: TcpStream, cmd: Command) -> Self {
        let (read, write) = tcp.split();
        Self {
            cmd_sink: Some(Box::new(FramedWrite::new(write, ServerStream {}))),
            rsp_stream: Box::new(FramedRead::new(read, ServerStream {})),
            send: None,
            cmd: Some(cmd),
        }
    }
}

impl Future for ServerConnection {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if let Some(cmd) = self.cmd.take() {
                self.send = Some(self.cmd_sink.take().unwrap().send(cmd));
            }

            if let Some(ref mut send) = self.send {
                match send.poll() {
                    Ok(Async::Ready(_)) => {}
                    Ok(Async::NotReady) => {
                        return Ok(Async::NotReady);
                    }
                    Err(e) => panic!("sending failed: {:?}", e),
                }
            }

            self.send.take();
            match self.rsp_stream.poll() {
                Ok(Async::Ready(Some(rsp))) => {
                    println!("received response: {:?}", rsp);
                    return Ok(Async::Ready(()));
                }
                Ok(Async::Ready(None)) => {
                    println!("connection closed");
                    return Ok(Async::Ready(()));
                }
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(e) => panic!("receiver canceled: {:?}", e),
            }
        }
    }
}

struct ServerStream {}

impl Decoder for ServerStream {
    type Item = CommandResult;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 1 {
            return Ok(None);
        }
        match bincode::deserialize(src) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(e) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("decode: {:?}", e),
            )),
        }
    }
}

impl Encoder for ServerStream {
    type Item = Command;
    type Error = io::Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        bincode::serialize_into(&mut dst.writer(), &item).unwrap();
        Ok(())
    }
}

type BoxedCommandSink = Box<Sink<SinkItem = Command, SinkError = io::Error> + Send>;
